use infrastructure::actors::Mpsc;
use infrastructure::actors::MpscReceiver;
use infrastructure::actors::MpscSender;
use infrastructure::actors::MultiProducerSingleConsumer;
use infrastructure::actors::Receiver;
use infrastructure::actors::Sender;
use infrastructure::random_number::RandomNumber;
use infrastructure::random_number::Rn;
use infrastructure::runtime::Jh;
use infrastructure::runtime::JoinHandle;
use infrastructure::runtime::Rt;
use infrastructure::runtime::Runtime;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct ProcessId(u16);

impl ProcessId {
    pub fn new() -> Self {
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

pub enum MessageFromProcess<Di: Dialog> {
    Subscribe {
        sender: MpscSender<MessageToProcess>,
        dialog: Di,
    },
    Response {
        is_response_from_server: bool,
        is_response_ok:          bool,
    },
}

pub enum MessageToProcessManager<Di: Dialog> {
    FromUser {
        process_id: ProcessId,
        consent:    UserConsent,
    },
    FromProcess {
        process_id: ProcessId,
        message:    MessageFromProcess<Di>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum MessageToProcess {
    FallBackToCache,
    CancelOperation,
}

pub fn process_manager_actor<Di: Dialog>() -> MpscSender<MessageToProcessManager<Di>> {
    let (sender, mut receiver): (
        MpscSender<MessageToProcessManager<Di>>,
        MpscReceiver<MessageToProcessManager<Di>>,
    ) = Mpsc::channel();

    Rt::spawn_local(async move {
        struct ProcessInfo<Di: Dialog> {
            sender:                  MpscSender<MessageToProcess>,
            dialog:                  Di,
            timer_handle:            Jh<()>,
            is_response_from_server: Option<bool>,
            is_ok:                   Option<bool>,
            is_user_want_to_proceed: UserConsent,
        }

        let mut process_states = HashMap::<ProcessId, ProcessInfo<Di>>::new();

        loop {
            let msg: MessageToProcessManager<Di> = receiver.recv().await.unwrap();

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
                            table.timer_handle = timer_handle::<Di>(table.dialog.clone());
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
                            let timer_handle = timer_handle::<Di>(dialog.clone());

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

fn timer_handle<Di: Dialog>(dialog_clone: Di) -> Jh<()> {
    Rt::abortable_spawn_local(async move {
        Rt::sleep(Duration::from_secs(5)).await;
        dialog_clone.show();
    })
}
