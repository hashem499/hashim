use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::ReadInput;
use crate::domain::ReadOutput;
use anyhow::Result;
use kernel::make_auth_check;
use kernel::server::GenericServerOperationsInput;
use kernel::server::SideEffects;
use kernel::types::DatabaseRead;
use kernel::types::MyErrorTrait;
use kernel::types::UserUuidError;

impl GenericServerOperationsInput for Input {
    type ReadInput = ReadInput;
    type ReadOutput = ReadOutput;
    type Result = MyResult;

    async fn handle_operation_generic<
        DD,
        DBReader: for<'a> DatabaseRead<Db<'a> = DD, Input = Self::ReadInput, Output = Self::ReadOutput>,
    >(
        &self,
        side_effects: &mut SideEffects,
        client: &mut DBReader::Db<'_>,
    ) -> Result<Self::Result> {
        let mut errr = self.state_less_check();
        make_auth_check!(side_effects, self, errr);

        if errr.is_there_error() {
            return Ok(Err(errr));
        }

        let ok = self.state_full_operation::<DBReader>(client).await?;

        Ok(Ok(ok))
    }
}
