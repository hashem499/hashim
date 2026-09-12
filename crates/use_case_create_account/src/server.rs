use crate::domain::DatabaseRead;
use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::Ok;
use anyhow::Result;
use kernel::make_auth_check;
use kernel::request_response::OperationsResult;
use kernel::request_response::TypeOperationsResult;
use kernel::server::DBClient;
use kernel::server::DBTransaction;
use kernel::server::ServerOperationsInput;
use kernel::server::SideEffects;
use kernel::types::DatabaseWrite;
use kernel::types::MyErrorTrait;
use kernel::types::UserUuidError;
use std::pin::Pin;
use std::sync::OnceLock;
use utility::types::DynamicError;

pub static READER: OnceLock<&dyn DatabaseRead> = OnceLock::new();
pub static WRITER: OnceLock<&dyn DatabaseWrite<Input = Input>> = OnceLock::new();

impl ServerOperationsInput for Input {
    fn handle_operation(
        self: Box<Self>,
        side_effects: &mut SideEffects,
        // jwt: &Jwt,
    ) -> Pin<Box<dyn Future<Output = Result<TypeOperationsResult>> + '_>> {
        Box::pin(async move {
            let mut errr = self.state_less_check();
            make_auth_check!(side_effects, self, errr);

            if errr.is_there_error() {
                return Ok(Err(errr).into());
            }

            let reader = *READER.get().unwrap();
            let mut txn = client.begin_transaction().await?;

            let result: Result<MyResult, DynamicError> = async {
                let errr = self.state_full_check::<Db>(&mut txn).await?;

                if errr.is_there_error() {
                    return Ok(Err(errr).into());
                }

                let result = self.state_less_operation();
                DbWrite::write(&mut txn, &result).await?;
                Ok(Ok(result).into())
            }
            .await;

            if let Ok(a) = &result {
                if a.is_ok() {
                    let _ = txn.commit_transaction().await?;
                }
            } else {
                txn.rollback_transaction().await?;
            }

            result
        })
    }
    // pub(crate) async fn handle_operation<
    //     Cli: DBClient,
    //     Db: for<'a> DatabaseRead<Db<'a> = Cli::Txn<'a>>,
    //     DbWrite: for<'a> DatabaseWrite<Db<'a> = Cli::Txn<'a>, Input = Ok>,
    // >(
    //     &self,
    //     side_effects: &mut SideEffects,
    //     client: &mut Cli,
    // ) -> Result<MyResult, DynamicError> {
    // }
}
