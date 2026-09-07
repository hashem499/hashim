use crate::new_types::JsonWebTokenType;
use crate::new_types::UserUuid;
use crate::request_response::Txn;
use crate::request_response::TypeOperationsInput;
use crate::request_response::TypeOperationsResult;
use utility::process_manager::Dialog as ProcessDialog;
use utility_ui::domain::Dialog as UiDialog;
use utility_ui::domain::HashimSignal;

pub trait Cache {
    fn new() -> impl Future<Output = Self>;

    fn get_all_txn_input(&self) -> impl Future<Output = Vec<Txn<TypeOperationsInput>>>;
    fn write_txn_input(&self, txn: &Txn<TypeOperationsInput>) -> impl Future<Output = ()>;
    fn write_txn_result(&self, txn: &Txn<TypeOperationsResult>) -> impl Future<Output = ()>;
    fn mark_txn_input_as_faild(&self, txn_number: &u64) -> impl Future<Output = ()>;
    fn delete_txn_input(&self, txn_number: &u64) -> impl Future<Output = ()>;
    fn clear_pending_txn_state(&self) -> impl Future<Output = ()>;
    fn start_pending_txn_state(&self) -> impl Future<Output = ()>;

    fn get_jwt(&self, user_uuid: &UserUuid) -> impl Future<Output = Option<JsonWebTokenType>>;
}

#[derive(Clone)]
pub struct DialogSignalAdapter<S: HashimSignal<UiDialog> + Clone + 'static>(pub S);

impl<S: HashimSignal<UiDialog> + Clone + 'static> ProcessDialog for DialogSignalAdapter<S> {
    fn show(&self) {
        self.0.set(UiDialog::Show);
    }

    fn hide(&self) {
        self.0.set(UiDialog::Hide);
    }
}
