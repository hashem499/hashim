use crate::domain::DatabaseRead;
use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::Ok;
use kernel::server::DBClient;
use kernel::server::DBTransaction;
use kernel::server::SideEffects;
use kernel::types::DatabaseWrite;
use kernel::types::MyErrorTrait;
use utility::row_id::RowId;
use utility::types::DynamicError;

impl Input {
    pub(crate) async fn handle_operation<
        Id: RowId,
        Cli: DBClient,
        Db: for<'a> DatabaseRead<Db<'a> = Cli::Txn<'a>>,
        DbWrite: for<'a> DatabaseWrite<Db<'a> = Cli::Txn<'a>, Input = Ok>,
        SEff: SideEffects,
    >(
        &self,
        side_effects: &mut SEff,
        client: &mut Cli,
    ) -> Result<MyResult, DynamicError> {
        let mut errr = self.state_less_check::<Id>();
        errr.user_uuid = side_effects.check_is_user_authenticated(&self.user_uuid);

        if errr.is_there_error() {
            return Ok(Err(errr).into());
        }

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
    }
}
