use crate::new_types::BranchUuid;
use crate::new_types::CompanyUuid;
use crate::new_types::NonceUuid;
use crate::new_types::UserUuid;
use crate::request_response::OperationsInput;
use crate::request_response::OperationsResult;
use crate::request_response::TypeResourceDTO;
use crate::types::DatabaseRead;
use anyhow::Result;
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::Pin;

pub struct TheCompaniesAndBranchesHeIn {
    pub branches_of_each_company: HashMap<CompanyUuid, HashSet<BranchUuid>>,
    pub companies:                HashMap<UserUuid, HashSet<CompanyUuid>>,
    pub branches:                 HashMap<UserUuid, HashSet<BranchUuid>>,
}

pub mod domain_errors {
    #[derive(Debug)]
    pub enum AtCommit {
        DataIsChanged,
    }
}

pub trait DBTransaction {
    fn commit_transaction(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<Result<(), domain_errors::AtCommit>>>>>;
    fn rollback_transaction(self) -> Pin<Box<dyn Future<Output = Result<()>>>>;
}

pub trait DBClient {
    fn as_any(&mut self) -> &mut dyn Any;

    fn begin_transaction(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn DBTransaction>>>>>;

    fn write_nonce_if_not_used_and_return_is_nonce_used(
        &mut self,
        nonce: &NonceUuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool>>>>;

    // here we just do read we dont do here any set or check

    fn read_roles_for_user(
        &mut self,
        users_uuids: &HashSet<UserUuid>,
    ) -> Pin<Box<dyn Future<Output = Result<TheCompaniesAndBranchesHeIn>>>>;
}

pub(crate) type ListOfResources = HashMap<BranchUuid, Vec<TypeResourceDTO>>;

#[derive(Debug, Default)]
pub struct SideEffects {
    pub authenticated_users:              HashSet<UserUuid>,
    pub users_to_resubscribe:             HashSet<UserUuid>,
    pub resource_to_broadcast_for_branch: ListOfResources,
}

#[macro_export]
macro_rules! make_auth_check {
    ($side_effects:expr, $self:expr, $errr:expr) => {
        if !$side_effects.authenticated_users.contains(&$self.user_uuid) {
            $errr.user_uuid = Some(UserUuidError::NotAuthenticated);
        }
    };
}

pub trait ServerOperationsInput {
    fn handle_operation<'a>(
        self: Box<Self>,
        side_effects: &'a mut SideEffects,
        client: &'a mut dyn DBClient,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn OperationsResult>>> + 'a>>;
}

pub trait GenericServerOperationsInput {
    type Result;
    type ReadInput;
    type ReadOutput;

    fn handle_operation_generic<
        DD,
        DBReader: for<'a> DatabaseRead<Db<'a> = DD, Input = Self::ReadInput, Output = Self::ReadOutput>,
    >(
        &self,
        side_effects: &mut SideEffects,
        client: &mut DBReader::Db<'_>,
    ) -> impl Future<Output = Result<Self::Result>>;
}
