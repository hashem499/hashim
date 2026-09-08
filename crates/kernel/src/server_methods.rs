use crate::new_types::UserUuid;
use crate::new_types::UuidType;
use crate::request_response::FromServer;
use crate::request_response::Input;
use crate::request_response::MyResult;
use crate::request_response::Txn;
use crate::request_response::TypeResourceDTO;
use crate::server::DBClient;
use crate::server::ListOfResources;
use crate::server::SideEffects;
use crate::types::HashimError;
use crate::types::JWTError;
use crate::types::NonceError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use utility::actors::MultiProducerSingleConsumer;
use utility::actors::Receiver;
use utility::actors::Sender;
use utility::authentication::HashedPassword;
use utility::encode_decode::Coding;
use utility::functions::Regex;
use utility::jwt::JWT;
use utility::random_number::RandomNumber;
use utility::row_id::RowId;
use utility::runtime::Either;
use utility::runtime::Runtime;
use utility::time::Time;
use utility::types::DynamicError;
use utility::types::HashMapWithHashMapValue;
use utility::types::LogError;

pub trait Database: 'static {
    type Client: DBClient;
    fn new() -> impl Future<Output = Self>;
    fn get_client(&self) -> impl Future<Output = Result<Self::Client, DynamicError>>;
}

pub enum WSMessage {
    Binary(Vec<u8>),
    Close,
}

pub trait WSServer: 'static {
    fn send_bin(&mut self, bin: Vec<u8>) -> impl Future<Output = Result<(), DynamicError>>;
    fn receive(&mut self) -> impl Future<Output = Result<WSMessage, DynamicError>>;
    fn close(self) -> impl Future<Output = Result<(), DynamicError>>;
}

pub struct ServerMethods<Mpsc: MultiProducerSingleConsumer, Jwt: JWT, Db: Database> {
    database:                    Db,
    jwt:                         Jwt,
    pub(crate) sender_to_broker: Mpsc::Sender<MessageToBroker<Mpsc>>,
}

impl<Mpsc: MultiProducerSingleConsumer, Jwt: JWT, Db: Database<Client = Cli>, Cli: DBClient>
    ServerMethods<Mpsc, Jwt, Db>
{
    pub async fn new<Rt: Runtime>() -> Self {
        let (sender_to_broker, receiver_to_broker) = Mpsc::channel();
        Self::broker_actor::<Rt>(receiver_to_broker);

        Self {
            database: Db::new().await,
            jwt: Jwt::new(),
            sender_to_broker,
        }
    }

    pub fn server_actor<
        Rt: Runtime,
        Ws: WSServer,
        Rn: RandomNumber,
        Ed: Coding,
        Id: RowId,
        Ti: Time,
        Rg: Regex,
        Auth: HashedPassword,
    >(
        self: Arc<Self>,
        mut session: Ws,
    ) {
        Rt::spawn_local(async move {
            let mut sender_to_broker = self.sender_to_broker.clone();
            let (sender_to_server, mut receiver_to_server) =
                Mpsc::channel::<Vec<TypeResourceDTO>>();
            let connection_id = Rn::generate();

            loop {
                let result = Rt::select(session.receive(), receiver_to_server.recv()).await;
                match result {
                    Either::One(msg) => {
                        let Ok(msg) = msg else {
                            break;
                        };

                        match msg {
                            WSMessage::Close => break,
                            WSMessage::Binary(received_data) => {
                                let Ok(input) = Ed::decode::<Input>(&received_data) else {
                                    if session
                                        .send_bin(Ed::encode(&FromServer::Error(
                                            HashimError::InvalidDataFormat,
                                        )))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    continue;
                                };

                                let Ok(mut client) = self.database.get_client().await else {
                                    if session
                                        .send_bin(Ed::encode(&FromServer::Error(
                                            HashimError::InternalServerError,
                                        )))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    continue;
                                };

                                dbg!(&input);
                                let mut side_effects = SideEffects::default();
                                let output = push_data::<Id, Ti, Auth, Jwt, Cli>(
                                    input,
                                    &mut side_effects,
                                    &mut client,
                                    &self.jwt,
                                )
                                .await;

                                dbg!(&output);
                                match output {
                                    Ok(ok) => {
                                        if session
                                            .send_bin(Ed::encode(&FromServer::PushData(ok)))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(_) => {
                                        if session
                                            .send_bin(Ed::encode(&FromServer::Error(
                                                HashimError::InternalServerError,
                                            )))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                }

                                if !side_effects.users_to_resubscribe.is_empty() {
                                    let Ok(subs) = get_table_of_subscribed_data::<Cli>(
                                        &mut client,
                                        &side_effects.users_to_resubscribe,
                                    )
                                    .await
                                    else {
                                        if session
                                            .send_bin(Ed::encode(&FromServer::Error(
                                                HashimError::InternalServerError,
                                            )))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                        continue;
                                    };

                                    if sender_to_broker
                                        .send(MessageToBroker::Subscribe {
                                            connection_id,
                                            list_of_subscribtion: subs,
                                            users_uuids: side_effects.users_to_resubscribe,
                                            sender_to_server: sender_to_server.clone(),
                                        })
                                        .await
                                        .log()
                                        .is_err()
                                    {
                                        break;
                                    }
                                }

                                if !side_effects.resource_to_broadcast_for_branch.is_empty()
                                    && sender_to_broker
                                        .send(MessageToBroker::Publish {
                                            connection_id,
                                            list_of_resources_for_branch: side_effects
                                                .resource_to_broadcast_for_branch,
                                        })
                                        .await
                                        .log()
                                        .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Either::Two(wraped_resource) => {
                        let resource = wraped_resource.unwrap();
                        if session
                            .send_bin(Ed::encode(&FromServer::Resources(resource)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }

            session.close().await.unwrap();

            sender_to_broker
                .send(MessageToBroker::Unsubscribe {
                    connection_id,
                })
                .await
                .unwrap();
        });
    }

    pub(crate) fn broker_actor<Rt: Runtime>(
        mut receiver_to_broker: Mpsc::Receiver<MessageToBroker<Mpsc>>,
    ) {
        Rt::spawn_local(async move {
            let mut pool_of_pubsub_for_branch: broker_functions::UserSubscribes =
                HashMap::with_capacity(10000);
            let mut pool_of_server_facad_channels: UserSenders<Mpsc> =
                HashMap::with_capacity(10000);

            loop {
                let message = receiver_to_broker.recv().await.unwrap();
                match message {
                    MessageToBroker::Subscribe {
                        connection_id,
                        list_of_subscribtion,
                        users_uuids,
                        sender_to_server,
                    } => {
                        for user_uuid in users_uuids {
                            pool_of_server_facad_channels.nested_insert(
                                user_uuid,
                                connection_id,
                                sender_to_server.clone(),
                            );
                        }

                        broker_functions::merge_subscribes(
                            &mut pool_of_pubsub_for_branch,
                            list_of_subscribtion.branches,
                        );
                    }
                    MessageToBroker::Unsubscribe {
                        connection_id,
                    } => {
                        let mut user_to_remove = Vec::new();

                        for (user_uuid, inner) in &mut pool_of_server_facad_channels {
                            if inner.remove(&connection_id).is_some() && inner.is_empty() {
                                user_to_remove.push(user_uuid.clone());
                            }
                        }

                        for user_uuid in user_to_remove {
                            pool_of_server_facad_channels.remove(&user_uuid);
                            broker_functions::unsubscribe(
                                &mut pool_of_pubsub_for_branch,
                                &user_uuid,
                            );
                        }
                    }
                    MessageToBroker::Publish {
                        connection_id,
                        list_of_resources_for_branch,
                    } => {
                        let mut resource_to_send: HashMap<UserUuid, Vec<TypeResourceDTO>> =
                            HashMap::new();

                        broker_functions::map_resource_to_subscribes(
                            &pool_of_pubsub_for_branch,
                            &list_of_resources_for_branch,
                            &mut resource_to_send,
                        );

                        for (user_uuid, resource) in resource_to_send {
                            let channels = pool_of_server_facad_channels.get_mut(&user_uuid);

                            if let Some(channels) = channels {
                                for (connection_id1, mut sender) in channels.clone() {
                                    if connection_id == connection_id1 {
                                        continue;
                                    }
                                    if sender.send(resource.clone()).await.is_err() {
                                        channels.remove(&connection_id1);
                                    }
                                }
                                if channels.is_empty() {
                                    pool_of_server_facad_channels.remove(&user_uuid);
                                }
                            } else {
                                dbg!("there is some problem here this should not happen");
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn push_data<Id: RowId, Ti: Time, Auth: HashedPassword, Jwt: JWT, Cli: DBClient>(
    input: Input,
    side_effects: &mut SideEffects,
    client: &mut Cli,
    jwt: &Jwt,
) -> Result<MyResult, DynamicError> {
    let mut the_return_result = MyResult {
        jwts:       Vec::with_capacity(input.jwts.len()),
        nonce:      Ok(()),
        operations: Vec::with_capacity(input.operations.len()),
    };

    let mut is_there_error = false;

    for jwt_value in &input.jwts {
        if let Some(user_uuid) = jwt.validate(jwt_value.clone()) {
            side_effects.authenticated_users.insert(user_uuid);
        } else {
            the_return_result.jwts.push(Err(JWTError::Invalid));
            is_there_error = true;
        }
    }

    if !Id::validate(&input.nonce) {
        the_return_result.nonce = Err(NonceError::Invalid);
        return Ok(the_return_result);
    }

    let is_nonce_used =
        client.write_nonce_if_not_used_and_return_is_nonce_used(&input.nonce).await?;

    if !check_nonce_if_valid::<Id>(&input.nonce, is_nonce_used) {
        the_return_result.nonce = Err(NonceError::Invalid);
    }

    if is_there_error {
        return Ok(the_return_result);
    }

    for transaction in input.operations {
        let result = transaction.operation.handle_operation(side_effects, client).await?;

        the_return_result.operations.push(Txn {
            txn_number: transaction.txn_number,
            operation:  result,
        });
    }

    Ok(the_return_result)
}

fn check_nonce_if_valid<Id: RowId>(nonce: &UuidType, is_used: bool) -> bool {
    if is_used {
        return false;
    }

    let Some(nonce) = Id::get_time_as_seconds(nonce) else {
        return false;
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let max_future = 5;

    if nonce > now + max_future {
        return false;
    }

    let max_age = 300;

    if now.saturating_sub(nonce) > max_age {
        return false;
    }

    true
}

async fn get_table_of_subscribed_data<Cli: DBClient>(
    client: &mut Cli,
    users_uuids: &HashSet<UserUuid>,
) -> Result<AllSubscribes, DynamicError> {
    let the_companies_and_branches_he_in = client.read_roles_for_user(users_uuids).await?;

    let mut subs = AllSubscribes {
        branches: HashMap::new(),
    };

    for (user, companies) in the_companies_and_branches_he_in.companies {
        for company in companies {
            let Some(branches) =
                the_companies_and_branches_he_in.branches_of_each_company.get(&company)
            else {
                continue;
            };

            for branch in branches {
                subs.branches.entry(branch.clone()).or_default().insert(user.clone());
            }
        }
    }

    for (user, branches) in the_companies_and_branches_he_in.branches {
        for branch in branches {
            subs.branches.entry(branch.clone()).or_default().insert(user.clone());
        }
    }

    Ok(subs)
}

mod broker_functions {
    use crate::new_types::BranchUuid;
    use crate::new_types::UserUuid;
    use crate::request_response::TypeResourceDTO;
    use crate::server::ListOfResources;
    use std::collections::HashMap;
    use std::collections::HashSet;

    pub(crate) type UserSubscribes = HashMap<BranchUuid, HashSet<UserUuid>>;

    pub(crate) fn map_resource_to_subscribes(
        pool_of_pubsub: &UserSubscribes,
        list_of_resources: &ListOfResources,
        resource_to_send: &mut HashMap<UserUuid, Vec<TypeResourceDTO>>,
    ) {
        for (branch, resources_for_branch) in list_of_resources {
            let Some(users) = pool_of_pubsub.get(&branch) else {
                dbg!("there is some problem here this should not happen");
                continue;
            };

            for user_uuid in users {
                let user_resource = resource_to_send.entry(user_uuid.clone()).or_default();

                for resource in resources_for_branch {
                    user_resource.push(resource.clone());
                }
            }
        }
    }

    pub(crate) fn unsubscribe(pool_of_pubsub: &mut UserSubscribes, user_uuid: &UserUuid) {
        pool_of_pubsub.retain(|_, users_and_subs| {
            users_and_subs.remove(user_uuid);
            !users_and_subs.is_empty()
        });
    }

    pub(crate) fn merge_subscribes(
        pool_of_pubsub: &mut UserSubscribes,
        list_of_subscribtion: UserSubscribes,
    ) {
        for (branch, users_subscribes) in list_of_subscribtion {
            for user_uuid in users_subscribes {
                pool_of_pubsub.entry(branch.clone()).or_default().insert(user_uuid);
            }
        }
    }
}

pub(crate) struct AllSubscribes {
    pub(crate) branches: broker_functions::UserSubscribes,
}

type UserSenders<Mpsc> = HashMap<
    UserUuid,
    HashMap<u64, <Mpsc as MultiProducerSingleConsumer>::Sender<Vec<TypeResourceDTO>>>, // because user may have multiple web socket connection
>;

pub(crate) enum MessageToBroker<Mpsc: MultiProducerSingleConsumer> {
    Subscribe {
        connection_id:        u64,
        list_of_subscribtion: AllSubscribes,
        users_uuids:          HashSet<UserUuid>,
        sender_to_server:     Mpsc::Sender<Vec<TypeResourceDTO>>,
    },
    Unsubscribe {
        connection_id: u64,
    },
    Publish {
        connection_id:                u64,
        list_of_resources_for_branch: ListOfResources,
    },
}
