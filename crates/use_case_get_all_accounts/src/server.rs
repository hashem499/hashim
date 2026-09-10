use crate::domain::DatabaseRead;
use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::Ok;
use kernel::make_auth_check;
use kernel::request_response::OperationsResult;
use kernel::request_response::TypeOperationsResult;
use kernel::server::DBClient;
use kernel::server::ServerOperationsInput;
use kernel::server::SideEffects;
use kernel::types::DatabaseWrite;
use kernel::types::MyErrorTrait;
use kernel::types::UserUuidError;
use std::pin::Pin;
use std::sync::OnceLock;
use utility::row_id::RowId;
use utility::types::DynamicError;

pub static REGISTRY: OnceLock<&dyn DatabaseRead<Db = &dyn DBClient>> = OnceLock::new();

impl ServerOperationsInput for Input {
    fn handle_operation(
        self: Box<Self>,
        side_effects: &mut SideEffects,
        client: &mut dyn DBClient,
    ) -> Pin<Box<dyn Future<Output = Result<TypeOperationsResult, DynamicError>>>> {
        Box::pin(async {
            let mut errr = self.state_less_check();
            make_auth_check!(side_effects, self, errr);

            if errr.is_there_error() {
                return Ok(TypeOperationsResult::from(MyResult::from(Err(errr))));
            }

            let reader = *REGISTRY.get().unwrap();

            let ok = self.state_full_operation(client, reader).await?;

            Ok(TypeOperationsResult::from(MyResult::from(Ok(ok))))
        })
    }
}
