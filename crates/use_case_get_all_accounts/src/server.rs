use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::ReadInput;
use crate::domain::ReadOutput;
use anyhow::Result;
use kernel::make_auth_check;
use kernel::server::DBClient;
use kernel::server::SideEffects;
use kernel::types::DatabaseRead;
use kernel::types::MyErrorTrait;
use kernel::types::UserUuidError;

impl Input {
    pub async fn handle_operation_generic<
        Cli: DBClient,
        DBReader: for<'a> DatabaseRead<Db<'a> = Cli, Input = ReadInput, Output = ReadOutput>,
    >(
        &self,
        side_effects: &mut SideEffects,
        client: &mut Cli,
    ) -> Result<MyResult> {
        let mut errr = self.state_less_check();
        make_auth_check!(side_effects, self, errr);

        if errr.is_there_error() {
            return Ok(Err(errr));
        }

        let ok = self.state_full_operation::<DBReader>(client).await?;

        Ok(Ok(ok))
    }
}
