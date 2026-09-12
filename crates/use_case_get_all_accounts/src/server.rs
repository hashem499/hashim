use crate::domain::Input;
use crate::domain::MyResult;
use crate::domain::ReadInput;
use crate::domain::ReadOutput;
use anyhow::Result;
use kernel::make_auth_check;
use kernel::request_response::TypeOperationsResult;
use kernel::server::DBClient;
use kernel::server::GenericServerOperationsInput;
use kernel::server::ServerOperationsInput;
use kernel::server::SideEffects;
use kernel::types::DatabaseRead;
use kernel::types::MyErrorTrait;
use kernel::types::UserUuidError;
use std::pin::Pin;

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
            return Ok(MyResult::from(Err(errr)));
        }

        let ok = self.state_full_operation::<DBReader>(client).await?;

        Ok(MyResult::from(Ok(ok)))
    }
}

// impl ServerOperationsInput for Input {
//     fn handle_operation<'a>(
//         self: Box<Self>,
//         side_effects: &'a mut SideEffects,
//         client: &'a mut dyn DBClient,
//     ) -> Pin<Box<dyn Future<Output = Result<TypeOperationsResult>> + 'a>> {
//         Box::pin(async move {
//             let client = client.as_any().downcast_mut::<MyDB>().unwrap();

//             Ok(self.handle_operation_generic::<_, A>(side_effects, client).await?.into())
//         })
//     }
// }

// struct A;

// struct MyDB;

// impl DBClient for MyDB {
//     fn as_any(&mut self) -> &mut dyn std::any::Any {
//         self
//     }

//     fn begin_transaction(
//         &mut self,
//     ) -> Pin<Box<dyn Future<Output = Result<Box<dyn kernel::server::DBTransaction>>>>> {
//         todo!()
//     }

//     fn write_nonce_if_not_used_and_return_is_nonce_used(
//         &mut self,
//         nonce: &kernel::new_types::NonceUuid,
//     ) -> Pin<Box<dyn Future<Output = Result<bool>>>> {
//         todo!()
//     }

//     fn read_roles_for_user(
//         &mut self,
//         users_uuids: &std::collections::HashSet<kernel::new_types::UserUuid>,
//     ) -> Pin<Box<dyn Future<Output = Result<kernel::server::TheCompaniesAndBranchesHeIn>>>> {
//         todo!()
//     }
// }
// impl DatabaseRead for A {
//     type Db<'a> = MyDB;
//     type Input = ReadInput;
//     type Output = ReadOutput;

//     async fn read(
//         db: &mut Self::Db<'_>,
//         input: &Self::Input,
//     ) -> Result<Self::Output, utility::types::DynamicError> {
//         todo!()
//     }
// }
