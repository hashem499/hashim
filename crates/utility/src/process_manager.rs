use crate::actors::MultiProducerSingleConsumer;
use crate::actors::Receiver;
use crate::actors::Sender;
use crate::random_number::RandomNumber;
use crate::runtime::JoinHandle;
use crate::runtime::Runtime;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct ProcessId(u16);

impl ProcessId {
    pub fn new<Rn: RandomNumber>() -> Self {
        ProcessId(Rn::generate() as u16)
    }
}

pub trait Dialog: Clone + 'static {
    fn show(&self);
    fn hide(&self);
}

#[derive(Debug, Clone, Copy)]
pub enum UserConsent {
    WaitForServerResponse,
    DontWaitForServerResponse,
    CancelOperation,
}

pub enum MessageFromProcess<Mpsc: MultiProducerSingleConsumer, Di: Dialog> {
    Subscribe {
        sender: Mpsc::Sender<MessageToProcess>,
        dialog: Di,
    },
    Response {
        is_response_from_server: bool,
        is_response_ok:          bool,
    },
}

pub enum MessageToProcessManager<Mpsc: MultiProducerSingleConsumer, Di: Dialog> {
    FromUser {
        process_id: ProcessId,
        consent:    UserConsent,
    },
    FromProcess {
        process_id: ProcessId,
        message:    MessageFromProcess<Mpsc, Di>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum MessageToProcess {
    FallBackToCache,
    CancelOperation,
}

pub fn process_manager_actor<Mpsc: MultiProducerSingleConsumer, Di: Dialog, Rt: Runtime>()
-> Mpsc::Sender<MessageToProcessManager<Mpsc, Di>> {
    let (sender, mut receiver): (
        <Mpsc as MultiProducerSingleConsumer>::Sender<MessageToProcessManager<Mpsc, Di>>,
        <Mpsc as MultiProducerSingleConsumer>::Receiver<MessageToProcessManager<Mpsc, Di>>,
    ) = Mpsc::channel();

    Rt::spawn_local(async move {
        struct ProcessInfo<Rt: Runtime, Mpsc: MultiProducerSingleConsumer, Di: Dialog> {
            sender:                  Mpsc::Sender<MessageToProcess>,
            dialog:                  Di,
            timer_handle:            Rt::JoinHandle<()>,
            is_response_from_server: Option<bool>,
            is_ok:                   Option<bool>,
            is_user_want_to_proceed: UserConsent,
        }

        let mut process_states = HashMap::<ProcessId, ProcessInfo<Rt, Mpsc, Di>>::new();

        loop {
            let msg: MessageToProcessManager<_, Di> = receiver.recv().await.unwrap();

            match msg {
                MessageToProcessManager::FromUser {
                    process_id,
                    consent,
                } => {
                    let table = process_states.get_mut(&process_id).unwrap();

                    table.dialog.hide();
                    table.is_user_want_to_proceed = consent;
                    table.timer_handle.abort().await;

                    match consent {
                        UserConsent::WaitForServerResponse => {
                            table.timer_handle = timer_handle::<Rt, Di>(table.dialog.clone());
                        }
                        UserConsent::DontWaitForServerResponse => {
                            table.sender.send(MessageToProcess::FallBackToCache).await.unwrap();
                        }
                        UserConsent::CancelOperation => {
                            table.sender.send(MessageToProcess::CancelOperation).await.unwrap();
                        }
                    };
                }
                MessageToProcessManager::FromProcess {
                    process_id,
                    message,
                } => {
                    match message {
                        MessageFromProcess::Subscribe {
                            sender,
                            dialog,
                        } => {
                            let timer_handle = timer_handle::<Rt, Di>(dialog.clone());

                            process_states.insert(process_id, ProcessInfo {
                                sender,
                                dialog,
                                timer_handle,
                                is_response_from_server: None,
                                is_ok: None,
                                is_user_want_to_proceed: UserConsent::WaitForServerResponse,
                            });
                        }
                        MessageFromProcess::Response {
                            is_response_from_server,
                            is_response_ok,
                        } => {
                            let table = process_states.get_mut(&process_id).unwrap();

                            table.is_ok = Some(is_response_ok);
                            table.is_response_from_server = Some(is_response_from_server);

                            if is_response_from_server {
                                table.sender.send(MessageToProcess::CancelOperation).await.unwrap();

                                process_states.remove(&process_id);
                            }
                        }
                    };
                }
            };
        }
    });

    sender
}

fn timer_handle<Rt: Runtime, Di: Dialog>(dialog_clone: Di) -> Rt::JoinHandle<()> {
    Rt::abortable_spawn_local(async move {
        Rt::sleep(Duration::from_secs(5)).await;
        dialog_clone.show();
    })
}
