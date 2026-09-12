use anyhow::Result;
use deadpool_postgres::Transaction;
use kernel::server::AtCommit;
use kernel::server::DBTransaction;
use std::pin::Pin;
use tokio_postgres::error::SqlState;
use utility::types::LogError;

pub struct S<'a> {
    pub(crate) txn: Transaction<'a>,
}

impl DBTransaction for S<'_> {
    fn commit_transaction<'a>(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<Result<(), AtCommit>>> + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            match self.txn.commit().await {
                Ok(_) => Ok(Ok(())),
                Err(e) => {
                    if get_sql_state(&e) == SqlState::T_R_SERIALIZATION_FAILURE {
                        return Ok(Err(AtCommit::DataIsChanged));
                    }
                    Err(e.into())
                }
            }
        })
    }

    fn rollback_transaction<'a>(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async {
            self.txn.rollback().await.log()?;
            Ok(())
        })
    }
}

fn get_sql_state(error: &tokio_postgres::Error) -> SqlState {
    error.as_db_error().unwrap().code().clone()
}
