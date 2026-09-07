use crate::actors::MultiProducerSingleConsumer;
use crate::actors::Receiver;
use crate::actors::Sender;
use crate::cache::CacheStruct;
use crate::cache::CachingStrategy;
use crate::cache::Response;
use crate::process_manager::Dialog;
use crate::process_manager::MessageFromProcess;
use crate::process_manager::MessageToProcess;
use crate::process_manager::MessageToProcessManager;
use crate::process_manager::ProcessId;
use crate::random_number::RandomNumber;
use crate::runtime::JoinHandle;
use crate::runtime::Runtime;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct Subscribe(pub &'static str);

pub async fn handle_fall_back<
    Rn: RandomNumber,
    Rt: Runtime,
    Mpsc: MultiProducerSingleConsumer,
    Di: Dialog,
    OperationsInput: Clone,
    OperationsResult,
>(
    mut cache: CacheStruct<Mpsc, Subscribe, OperationsInput, OperationsResult>,
    mut sender_to_process_manager: Mpsc::Sender<MessageToProcessManager<Mpsc, Di>>,
    dialog: Di,
    process_id: ProcessId,
    data: OperationsInput,
    f: impl Fn(OperationsResult) -> bool + Clone + 'static,
) {
    let txn_number = Rn::generate();

    let f1 = f.clone();
    let data1 = data.clone();
    let mut cache1 = cache.clone();
    let mut sender_to_process_manager1 = sender_to_process_manager.clone();

    let mut handle = <Rt>::abortable_spawn_local(async move {
        let mut receiver_to_response =
            cache1.send_to_cache_actor(CachingStrategy::WriteServerOnly, txn_number, data1).await;

        match receiver_to_response.recv().await.unwrap() {
            Response::CloseTheChannel => {}
            Response::ServerCannotBeReached => {}
            Response::Data {
                is_response_from_server,
                data,
            } => {
                sender_to_process_manager1
                    .send(MessageToProcessManager::FromProcess {
                        process_id,
                        message: MessageFromProcess::Response {
                            is_response_from_server,
                            is_response_ok: f1(data),
                        },
                    })
                    .await
                    .unwrap();
            }
        }
    });

    let (sender, mut receiver_to_process) = <Mpsc>::channel();
    sender_to_process_manager
        .send(MessageToProcessManager::FromProcess {
            process_id,
            message: MessageFromProcess::Subscribe {
                sender,
                dialog,
            },
        })
        .await
        .unwrap();

    match receiver_to_process.recv().await.unwrap() {
        MessageToProcess::CancelOperation => {}
        MessageToProcess::FallBackToCache => {
            let mut receiver_to_response =
                cache.send_to_cache_actor(CachingStrategy::WriteCacheOnly, txn_number, data).await;

            match receiver_to_response.recv().await.unwrap() {
                Response::CloseTheChannel => {}
                Response::ServerCannotBeReached => {}
                Response::Data {
                    is_response_from_server: _,
                    data,
                } => {
                    f(data);
                }
            }
        }
    }
    handle.abort().await;
}

pub fn spawn_listener<
    Rn: RandomNumber,
    Rt: Runtime,
    Mpsc: MultiProducerSingleConsumer,
    OperationsInput: Clone,
    OperationsResult,
>(
    mut cache: CacheStruct<Mpsc, Subscribe, OperationsInput, OperationsResult>,
    list_of_subscribtion: &'static [Subscribe],
    data: OperationsInput,
    is_error: impl Fn(OperationsResult) + 'static,
) -> impl FnOnce() {
    let component_id = Rn::generate() as u16;
    let mut cache1 = cache.clone();

    let mut handle = Rt::abortable_spawn_local(async move {
        let mut receiver_to_poke =
            cache.send_subs_to_cache_actor(component_id, list_of_subscribtion).await;

        cache
            .send_to_cache_actor(CachingStrategy::ReadServerOnly, Rn::generate(), data.clone())
            .await;

        loop {
            let value = cache
                .send_to_cache_actor(CachingStrategy::ReadCacheOnly, Rn::generate(), data.clone())
                .await
                .recv()
                .await
                .unwrap();

            if let Response::Data {
                data,
                ..
            } = value
            {
                is_error(data);
            };

            if receiver_to_poke.recv().await.is_err() {
                break;
            }
        }
        cache.send_unsubs_to_cache_actor(component_id).await;
    });

    move || {
        Rt::spawn_local(async move {
            handle.abort().await;
            cache1.send_unsubs_to_cache_actor(component_id).await;
        });
    }
}
