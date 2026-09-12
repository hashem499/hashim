use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::ReadInput;
use crate::domain::ReadOutput;
use infrastructure::actors::MpscSender;
use infrastructure::actors::Receiver;
use infrastructure::actors::Sender;
use infrastructure::random_number::RandomNumber;
use infrastructure::random_number::Rn;
use infrastructure::row_id::Id;
use infrastructure::row_id::RowId;
use kernel::client::Cache;
use kernel::client::DialogSignalAdapter;
use kernel::new_types::AccountUuid;
use kernel::new_types::CompanyUuid;
use kernel::new_types::UserUuid;
use kernel::new_types::UuidType;
use kernel::types::DatabaseRead;
use kernel::types::MyErrorTrait;
use use_case_get_all_accounts::client::fetch;
use utility::cache::CacheStruct;
use utility::cache::CachingStrategy;
use utility::cache::OpInputTrait;
use utility::cache::Response;
use utility::process_manager::MessageToProcessManager;
use utility::process_manager::ProcessId;
use utility::process_manager::UserConsent;
use utility::types::MakeOptionIfEmpty;
use utility::types::ReadAndSet;
use utility::ui_orchestration::handle_fall_back;
use utility_ui::domain::Dialog;
use utility_ui::domain::HashimSignal;

impl OpInputTrait for Input {}

type Type1 = Input;
type Type2 = Input;
type Type3 = MyResult;
type Type4 = MyResult;

pub trait GlobalModel {
    fn user_uuid(&self) -> impl ReadAndSet<UserUuid>;
    fn selected_company(&self) -> impl ReadAndSet<CompanyUuid>;
}

pub trait LocalModel {
    type Sig<T: Clone + Default>: HashimSignal<T>;

    fn process_id(&self) -> impl ReadAndSet<Option<ProcessId>>;
    fn show_dialog(&self) -> Self::Sig<Dialog>;
    fn is_loading(&self) -> Self::Sig<bool>;
    fn is_debit(&self) -> Self::Sig<bool>;
    fn is_permanent_account(&self) -> Self::Sig<bool>;
    fn account_name(&self) -> Self::Sig<String>;
    fn notes(&self) -> Self::Sig<String>;
    fn unit_of_measurement_of_quantity(&self) -> Self::Sig<String>;
    fn account_name_error(&self) -> Self::Sig<Option<String>>;
}

#[derive(Debug)]
pub enum CreateAccount {
    Subscribe,
    Submit,
    Consent(UserConsent),
    Clean,
    IsDebit(bool),
    IsPermanentAccount(bool),
    AccountName(String),
    Notes(String),
    UnitOfMeasurementOfQuantity(String),
}

pub(crate) async fn state_full_operation<
    Ch: Cache,
    LongCache: for<'a> DatabaseRead<Db<'a> = Ch, Input = ReadInput, Output = ReadOutput>,
>(
    data: &Type2,
    state: &mut Ch,
) -> Type3 {
    let errr = data.state_full_check::<LongCache>(state).await.unwrap();

    if errr.is_there_error() {
        return Err(errr).into();
    }

    Ok(data.state_less_operation()).into()
}

fn apply_on_the_model(output: &Type4, local_model: &impl LocalModel) {
    match output {
        Ok(_) => {
            local_model.account_name_error().reset();
        }
        Err(business_error) => {
            local_model
                .account_name_error()
                .set(business_error.account_name.as_ref().map(|_| String::from("duplicated")));
        }
    }
}

impl CreateAccount {
    pub(crate) async fn update<Di, Ch, LongCache, LM>(
        self,
        global_model: &impl GlobalModel,
        local_model: &'static LM,
        cache: CacheStruct,
        mut sender_to_process_manager: MpscSender<MessageToProcessManager<DialogSignalAdapter<Di>>>,
    ) where
        LM: LocalModel<Sig<Dialog> = Di>,
        Di: HashimSignal<Dialog> + Clone + 'static,
        Ch: Cache,
        LongCache: for<'a> DatabaseRead<Db<'a> = Ch>,
    {
        match self {
            CreateAccount::Submit => {
                handle_submit::<Di, Ch, LongCache, LM>(
                    global_model,
                    local_model,
                    cache,
                    sender_to_process_manager,
                )
                .await
            }
            CreateAccount::Consent(i) => {
                sender_to_process_manager
                    .send(MessageToProcessManager::FromUser {
                        process_id: local_model.process_id().read().unwrap(),
                        consent:    i,
                    })
                    .await
                    .unwrap();
            }
            CreateAccount::Clean => handle_clean(local_model),
            CreateAccount::IsDebit(v) => local_model.is_debit().set(v),
            CreateAccount::IsPermanentAccount(v) => local_model.is_permanent_account().set(v),
            CreateAccount::AccountName(v) => {
                local_model.account_name().set(v);
                handle_check::<Ch, LongCache>(global_model, local_model, cache).await;
            }
            CreateAccount::Notes(v) => local_model.notes().set(v),
            CreateAccount::UnitOfMeasurementOfQuantity(v) => {
                local_model.unit_of_measurement_of_quantity().set(v)
            }
            CreateAccount::Subscribe => {
                fetch(
                    global_model.selected_company().read(),
                    global_model.user_uuid().read(),
                    cache,
                )
                .await
            }
        }
    }
}

fn build_input(global_model: &impl GlobalModel, local_model: &impl LocalModel) -> Type1 {
    Input {
        user_uuid:                       global_model.user_uuid().read(),
        new_uuid:                        AccountUuid::from(UuidType::from(Id::generate())),
        is_debit:                        local_model.is_debit().read(),
        is_permanent_account:            local_model.is_permanent_account().read(),
        account_name:                    local_model.account_name().read(),
        notes:                           local_model.notes().read().none_if_empty(),
        unit_of_measurement_of_quantity: local_model.unit_of_measurement_of_quantity().read(),
        belong_to_company:               global_model.selected_company().read(),
    }
}

fn handle_clean<As: LocalModel>(local_model: &As) {
    local_model.process_id().put(None);
    local_model.account_name().reset();
    local_model.is_debit().reset();
    local_model.is_permanent_account().reset();
    local_model.notes().reset();
    local_model.unit_of_measurement_of_quantity().reset();
    local_model.is_loading().reset();
    local_model.account_name_error().reset();
}

async fn handle_submit<Di, Ch, LongCache, LM>(
    global_model: &impl GlobalModel,
    local_model: &'static LM,
    cache: CacheStruct,
    sender_to_process_manager: MpscSender<MessageToProcessManager<DialogSignalAdapter<Di>>>,
) where
    LM: LocalModel<Sig<Dialog> = Di>,
    Di: HashimSignal<Dialog> + Clone + 'static,
    Ch: Cache,
    LongCache: for<'a> DatabaseRead<Db<'a> = Ch>,
{
    let process_id = ProcessId::new();
    local_model.process_id().put(Some(process_id));

    let dialog_signal_adapter = DialogSignalAdapter(local_model.show_dialog());

    handle_fall_back::<DialogSignalAdapter<Di>>(
        cache,
        sender_to_process_manager,
        dialog_signal_adapter,
        process_id,
        build_input(global_model, local_model).into(),
        move |data| {
            let result = data.downcast();
            apply_on_the_model(&result, local_model);

            let is_ok = result.is_ok();
            if is_ok {
                handle_clean(local_model);
            }

            is_ok
        },
    )
    .await;

    local_model.is_loading().reset();
}

async fn handle_check<Ch: Cache, LongCache: for<'a> DatabaseRead<Db<'a> = Ch>>(
    global_model: &impl GlobalModel,
    local_model: &impl LocalModel,
    mut cache: CacheStruct,
) {
    let mut receiver_to_response = cache
        .send_to_cache_actor(
            CachingStrategy::ReadCacheOnly,
            Rn::generate(),
            build_input(global_model, local_model).into(),
        )
        .await;

    match receiver_to_response.recv().await.unwrap() {
        Response::CloseTheChannel => {}
        Response::ServerCannotBeReached => {}
        Response::Data {
            is_response_from_server: _,
            data,
        } => {
            let result = data.downcast();
            apply_on_the_model(&result, local_model);
        }
    }
}
