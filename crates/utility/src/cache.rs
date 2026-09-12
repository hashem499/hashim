use infrastructure::actors::Mpsc;
use infrastructure::actors::MpscReceiver;
use infrastructure::actors::MpscSender;
use infrastructure::actors::MultiProducerSingleConsumer;
use infrastructure::actors::Sender;
use infrastructure::encode_decode::Coding;
use infrastructure::encode_decode::Ed;
use infrastructure::runtime::Rt;
use infrastructure::runtime::Runtime;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct Subscribe(u32);

pub trait OpInputTrait: Debug {}
pub trait OpResultTrait: Debug {
    fn into_any(self: Arc<Self>) -> Box<dyn Any>;
}

#[derive(Debug, Clone)]
pub struct OpInput(Arc<dyn OpInputTrait>);
#[derive(Debug, Clone)]
pub struct OpResult(Arc<dyn OpResultTrait>);

impl<T: OpInputTrait + 'static> From<T> for OpInput {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl OpResult {
    pub fn downcast<T: 'static>(self) -> T {
        *self.0.into_any().downcast::<T>().unwrap()
    }
}

pub enum MessageFromServer<E, Resp, Reso> {
    Error(E),
    Response(Resp),
    Resources(Reso),
}

#[derive(Debug, Clone)]
pub enum Response {
    CloseTheChannel,
    ServerCannotBeReached,
    Data {
        is_response_from_server: bool,
        data:                    OpResult,
    },
}

pub enum MessageToCache {
    WeAreBackOnline,
    DataFromServer(Vec<u8>),
    Subscribe {
        component_id:         u16,
        list_of_subscribtion: &'static [Subscribe],
        sender:               MpscSender<()>,
    },
    UnSubscribe {
        component_id: u16,
    },
    Query {
        strategy:   CachingStrategy,
        sender:     MpscSender<Response>,
        txn_number: u64,
        data:       OpInput,
    },
}

pub trait CacheActorUtils {
    fn cache_receiver(
        receiver: &mut MpscReceiver<MessageToCache>,
    ) -> impl Future<Output = MessageToCache>;

    type NetworkSender;
    fn send_to_network(sender: &mut Self::NetworkSender, data: Vec<u8>)
    -> impl Future<Output = ()>;

    type ErrorSender;
    fn internal_server_error(sender: &mut Self::ErrorSender) -> impl Future<Output = ()>;
    fn invalid_format_error(sender: &mut Self::ErrorSender) -> impl Future<Output = ()>;

    type NetworkStatus;
    fn is_online(network_status: &Self::NetworkStatus) -> impl Future<Output = bool>;

    type Cache;
    fn new_cache() -> impl Future<Output = Self::Cache>;

    fn get_all_pending_txn(cache: &Self::Cache) -> impl Future<Output = Vec<(u64, OpInput)>>;
    fn clear_state_pending_txn(cache: &mut Self::Cache) -> impl Future<Output = ()>;
    fn start_state_pending_txn(cache: &mut Self::Cache) -> impl Future<Output = ()>;

    type SendingTxns: Serialize;
    fn prepare_txn_for_send(
        cache: &Self::Cache,
        txns: Vec<(u64, OpInput)>,
    ) -> impl Future<Output = Self::SendingTxns>;

    type ErrorFromServer;
    type Response;
    type ResourceToStore;
    type ResourceFromServer;
    type MessageFromServer<'de>: Deserialize<'de>;

    fn to(
        msg: Self::MessageFromServer<'_>,
    ) -> MessageFromServer<Self::ErrorFromServer, Self::Response, Self::ResourceFromServer>;

    fn convert_resource_from_server_to_resource_to_store(
        resource: &Self::ResourceFromServer,
    ) -> Vec<Self::ResourceToStore>;

    fn extract_resource_from_response(resp: &Self::Response) -> Vec<Self::ResourceToStore>;
    fn extract_resource_from_result(data: &OpResult) -> Option<Self::ResourceToStore>;

    fn write_resource_to_cache_from_server(
        cache: &mut Self::Cache,
        resource: &Self::ResourceToStore,
    ) -> impl Future<Output = ()>;
    fn write_resource_to_cache_from_client(
        cache: &mut Self::Cache,
        resource: &Self::ResourceToStore,
    ) -> impl Future<Output = ()>;

    fn delete_successful_txn_input(
        cache: &Self::Cache,
        resp: &Self::Response,
    ) -> impl Future<Output = ()>;
    fn mark_txn_input_as_faild(
        cache: &Self::Cache,
        resp: &Self::Response,
    ) -> impl Future<Output = ()>;
    fn write_faild_txn_result(
        cache: &Self::Cache,
        resp: &Self::Response,
    ) -> impl Future<Output = ()>;
    fn get_all_response_txn_numbers(
        resp: &Self::Response,
    ) -> impl Future<Output = Vec<(u64, OpResult)>>;
    fn check_input(cache: &mut Self::Cache, data: &OpInput) -> impl Future<Output = OpResult>;
    fn write_input(
        cache: &Self::Cache,
        txn_number: u64,
        data: &OpInput,
    ) -> impl Future<Output = ()>;

    fn create_pending_txn(txn_number: u64, data: OpInput) -> (u64, OpInput) {
        (txn_number, data)
    }
    fn collect_subs_to_poke(
        subs_to_poke: &mut HashSet<Subscribe>,
        resource: &Self::ResourceToStore,
    );
}

#[allow(dead_code)]
pub enum CachingStrategy {
    ReadCacheOnly,
    ReadCacheFirst,
    ReadCacheAndServer,
    ReadServerFirst,
    ReadServerOnly,
    WriteCacheOnly,
    WriteCacheFirst,
    WriteCacheAndServer,
    WriteServerFirst,
    WriteServerOnly,
}

pub struct CacheStruct {
    sender: MpscSender<MessageToCache>,
}

impl Clone for CacheStruct {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl CacheStruct {
    pub fn new<Cu: CacheActorUtils + 'static>(
        receiver_to_cache: MpscReceiver<MessageToCache>,
        sender_to_cache: MpscSender<MessageToCache>,
        sender_to_network: Cu::NetworkSender,
        sender_to_error: Cu::ErrorSender,
        is_online: Cu::NetworkStatus,
    ) -> Self {
        Self::cache_actor::<Cu>(receiver_to_cache, sender_to_network, sender_to_error, is_online);

        Self {
            sender: sender_to_cache,
        }
    }

    pub async fn send_to_cache_actor(
        &mut self,
        strategy: CachingStrategy,
        txn_number: u64,
        data: OpInput,
    ) -> MpscReceiver<Response> {
        let (sender, receiver) = Mpsc::channel();

        self.sender
            .send(MessageToCache::Query {
                strategy,
                sender,
                txn_number,
                data,
            })
            .await
            .unwrap();

        receiver
    }

    pub async fn send_subs_to_cache_actor(
        &mut self,
        component_id: u16,
        list_of_subscribtion: &'static [Subscribe],
    ) -> MpscReceiver<()> {
        let (sender, receiver) = Mpsc::channel();

        self.sender
            .send(MessageToCache::Subscribe {
                component_id,
                list_of_subscribtion,
                sender,
            })
            .await
            .unwrap();

        receiver
    }

    pub async fn send_unsubs_to_cache_actor(&mut self, component_id: u16) {
        self.sender
            .send(MessageToCache::UnSubscribe {
                component_id,
            })
            .await
            .unwrap();
    }

    fn cache_actor<Cu: CacheActorUtils + 'static>(
        mut receiver_to_cache: MpscReceiver<MessageToCache>,
        mut sender_to_network: Cu::NetworkSender,
        mut sender_to_error: Cu::ErrorSender,
        is_online: Cu::NetworkStatus,
    ) {
        Rt::spawn_local(async move {
            let mut pool_of_senders = HashMap::<u64, MpscSender<Response>>::with_capacity(100);
            let mut pool_of_pokers = HashMap::<u16, MpscSender<()>>::with_capacity(10);
            let mut pool_of_subscribes = HashMap::<Subscribe, HashSet<u16>>::with_capacity(100);

            let mut cache = Cu::new_cache().await;

            loop {
                match Cu::cache_receiver(&mut receiver_to_cache).await {
                    MessageToCache::WeAreBackOnline => {
                        let txns = Cu::get_all_pending_txn(&cache).await;
                        if txns.is_empty() {
                            continue;
                        }
                        let txns = Cu::prepare_txn_for_send(&cache, txns).await;
                        let txns = Ed::encode(&txns);
                        Cu::send_to_network(&mut sender_to_network, txns).await;
                    }
                    MessageToCache::DataFromServer(raw_data) => {
                        let message_type = match Ed::decode::<Cu::MessageFromServer<'_>>(&raw_data)
                        {
                            Ok(message_type) => Cu::to(message_type),
                            Err(_) => {
                                Cu::invalid_format_error(&mut sender_to_error).await;
                                continue;
                            }
                        };

                        match message_type {
                            MessageFromServer::Error(_) => {
                                Cu::internal_server_error(&mut sender_to_error).await;
                            }
                            MessageFromServer::Response(response) => {
                                let mut subs_to_poke = HashSet::new();

                                Cu::clear_state_pending_txn(&mut cache).await;
                                Cu::delete_successful_txn_input(&cache, &response).await;
                                Cu::mark_txn_input_as_faild(&cache, &response).await;
                                Cu::write_faild_txn_result(&cache, &response).await;

                                let resource = Cu::extract_resource_from_response(&response);
                                for resource in resource {
                                    Cu::write_resource_to_cache_from_server(&mut cache, &resource)
                                        .await;
                                    Cu::collect_subs_to_poke(&mut subs_to_poke, &resource);
                                }

                                let txn_numbers = Cu::get_all_response_txn_numbers(&response).await;
                                for (number, data) in txn_numbers {
                                    let sender = pool_of_senders.remove(&number);
                                    if let Some(mut sender) = sender {
                                        let _ = sender
                                            .send(Response::Data {
                                                is_response_from_server: true,
                                                data,
                                            })
                                            .await;
                                        let _ = sender.send(Response::CloseTheChannel).await;
                                    }
                                }

                                Cu::start_state_pending_txn(&mut cache).await;
                                let txns = Cu::get_all_pending_txn(&cache).await;

                                for (_, txn) in txns {
                                    let result = Cu::check_input(&mut cache, &txn).await;
                                    let resource = Cu::extract_resource_from_result(&result);

                                    if let Some(resource) = resource {
                                        Cu::write_resource_to_cache_from_client(
                                            &mut cache, &resource,
                                        )
                                        .await;
                                    }
                                }

                                poke_the_subs::<Subscribe>(
                                    &mut pool_of_pokers,
                                    &pool_of_subscribes,
                                    &subs_to_poke,
                                )
                                .await;
                            }
                            MessageFromServer::Resources(resources) => {
                                Cu::clear_state_pending_txn(&mut cache).await;
                                let mut subs_to_poke = HashSet::new();

                                let resources =
                                    Cu::convert_resource_from_server_to_resource_to_store(
                                        &resources,
                                    );

                                for resources in resources {
                                    Cu::write_resource_to_cache_from_server(&mut cache, &resources)
                                        .await;
                                    Cu::collect_subs_to_poke(&mut subs_to_poke, &resources);
                                }

                                poke_the_subs::<Subscribe>(
                                    &mut pool_of_pokers,
                                    &pool_of_subscribes,
                                    &subs_to_poke,
                                )
                                .await;

                                Cu::start_state_pending_txn(&mut cache).await;
                                let txns = Cu::get_all_pending_txn(&cache).await;

                                for (_, txn) in txns {
                                    let result = Cu::check_input(&mut cache, &txn).await;
                                    let resource = Cu::extract_resource_from_result(&result);

                                    if let Some(resource) = resource {
                                        Cu::write_resource_to_cache_from_client(
                                            &mut cache, &resource,
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                    }
                    MessageToCache::Subscribe {
                        component_id,
                        list_of_subscribtion,
                        sender,
                    } => {
                        pool_of_pokers.insert(component_id, sender);
                        for subscribe in list_of_subscribtion {
                            pool_of_subscribes
                                .entry(subscribe.clone())
                                .or_default()
                                .insert(component_id);
                        }
                    }
                    MessageToCache::UnSubscribe {
                        component_id,
                    } => {
                        pool_of_pokers.remove(&component_id);

                        for components in &mut pool_of_subscribes.values_mut() {
                            components.remove(&component_id);
                        }

                        pool_of_subscribes.retain(|_, components| !components.is_empty());
                    }
                    MessageToCache::Query {
                        strategy,
                        mut sender,
                        txn_number,
                        data,
                    } => {
                        match strategy {
                            CachingStrategy::ReadCacheOnly => {
                                let result = Cu::check_input(&mut cache, &data).await;
                                let _ = sender
                                    .send(Response::Data {
                                        is_response_from_server: false,
                                        data:                    result,
                                    })
                                    .await;
                                let _ = sender.send(Response::CloseTheChannel).await;
                            }
                            CachingStrategy::ReadCacheFirst => todo!(),
                            CachingStrategy::ReadCacheAndServer => {
                                let result = Cu::check_input(&mut cache, &data).await;

                                let _ = sender
                                    .send(Response::Data {
                                        is_response_from_server: false,
                                        data:                    result,
                                    })
                                    .await;

                                let operations = Cu::create_pending_txn(txn_number, data);

                                if Cu::is_online(&is_online).await {
                                    let txn_to_send =
                                        Cu::prepare_txn_for_send(&cache, vec![operations]).await;

                                    let data = Ed::encode(&txn_to_send);
                                    Cu::send_to_network(&mut sender_to_network, data).await;

                                    pool_of_senders.insert(txn_number, sender);
                                } else {
                                    let _ = sender.send(Response::ServerCannotBeReached).await;
                                    let _ = sender.send(Response::CloseTheChannel).await;
                                };
                            }
                            CachingStrategy::ReadServerFirst => todo!(),
                            CachingStrategy::ReadServerOnly => {
                                let operations = Cu::create_pending_txn(txn_number, data);

                                if Cu::is_online(&is_online).await {
                                    let txn_to_send =
                                        Cu::prepare_txn_for_send(&cache, vec![operations]).await;

                                    let data = Ed::encode(&txn_to_send);
                                    Cu::send_to_network(&mut sender_to_network, data).await;

                                    pool_of_senders.insert(txn_number, sender);
                                } else {
                                    let _ = sender.send(Response::ServerCannotBeReached).await;
                                    let _ = sender.send(Response::CloseTheChannel).await;
                                };
                            }
                            CachingStrategy::WriteCacheOnly => {
                                let result = Cu::check_input(&mut cache, &data).await;
                                let resource = Cu::extract_resource_from_result(&result);

                                let mut subs_to_poke = HashSet::new();
                                if let Some(resource) = resource {
                                    Cu::write_resource_to_cache_from_client(&mut cache, &resource)
                                        .await;
                                    Cu::collect_subs_to_poke(&mut subs_to_poke, &resource);
                                }
                                Cu::write_input(&cache, txn_number, &data).await;

                                poke_the_subs::<Subscribe>(
                                    &mut pool_of_pokers,
                                    &pool_of_subscribes,
                                    &subs_to_poke,
                                )
                                .await;

                                let _ = sender
                                    .send(Response::Data {
                                        is_response_from_server: false,
                                        data:                    result,
                                    })
                                    .await;

                                let _ = sender.send(Response::CloseTheChannel).await;
                            }
                            CachingStrategy::WriteCacheFirst => todo!(),
                            CachingStrategy::WriteCacheAndServer => {
                                let result = Cu::check_input(&mut cache, &data).await;
                                let resource = Cu::extract_resource_from_result(&result);

                                let mut subs_to_poke = HashSet::new();
                                if let Some(resource) = resource {
                                    Cu::write_resource_to_cache_from_client(&mut cache, &resource)
                                        .await;
                                    Cu::collect_subs_to_poke(&mut subs_to_poke, &resource);
                                }
                                Cu::write_input(&cache, txn_number, &data).await;

                                poke_the_subs::<Subscribe>(
                                    &mut pool_of_pokers,
                                    &pool_of_subscribes,
                                    &subs_to_poke,
                                )
                                .await;

                                let _ = sender
                                    .send(Response::Data {
                                        is_response_from_server: false,
                                        data:                    result,
                                    })
                                    .await;

                                let operations = Cu::create_pending_txn(txn_number, data);

                                if Cu::is_online(&is_online).await {
                                    let txn_to_send =
                                        Cu::prepare_txn_for_send(&cache, vec![operations]).await;

                                    let data = Ed::encode(&txn_to_send);
                                    Cu::send_to_network(&mut sender_to_network, data).await;

                                    pool_of_senders.insert(txn_number, sender);
                                } else {
                                    let _ = sender.send(Response::ServerCannotBeReached).await;
                                    let _ = sender.send(Response::CloseTheChannel).await;
                                };
                            }
                            CachingStrategy::WriteServerFirst => todo!(),
                            CachingStrategy::WriteServerOnly => {
                                let operations = Cu::create_pending_txn(txn_number, data);

                                if Cu::is_online(&is_online).await {
                                    let txn_to_send =
                                        Cu::prepare_txn_for_send(&cache, vec![operations]).await;

                                    let data = Ed::encode(&txn_to_send);
                                    Cu::send_to_network(&mut sender_to_network, data).await;

                                    pool_of_senders.insert(txn_number, sender);
                                } else {
                                    let _ = sender.send(Response::ServerCannotBeReached).await;
                                    let _ = sender.send(Response::CloseTheChannel).await;
                                };
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn poke_the_subs<Subscribe: 'static + Hash + Eq>(
    pool_of_pokers: &mut HashMap<u16, MpscSender<()>>,
    pool_of_subscribes: &HashMap<Subscribe, HashSet<u16>>,
    subs_to_poke: &HashSet<Subscribe>,
) {
    let mut components_to_poke = HashSet::new();

    for one_sub in subs_to_poke {
        let Some(a) = pool_of_subscribes.get(one_sub) else {
            continue;
        };

        for a in a {
            components_to_poke.insert(a);
        }
    }

    for i in components_to_poke {
        let sender = pool_of_pokers.get_mut(i).unwrap();
        let _ = sender.send(()).await;
    }
}
