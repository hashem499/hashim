use crate::new_types::BranchUuid;
use crate::new_types::CompanyUuid;
use crate::new_types::NonceUuid;
use crate::new_types::UserUuid;
use crate::request_response::OperationsResult;
use crate::request_response::TypeResourceDTO;
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

pub enum AtCommit {
    DataIsChanged,
}

pub trait DBTransaction {
    fn commit_transaction<'a>(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<Result<(), AtCommit>>> + 'a>>
    where
        Self: 'a;
    fn rollback_transaction<'a>(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>
    where
        Self: 'a;
}

pub trait DBClient {
    fn as_any(&mut self) -> &mut dyn Any;

    fn begin_transaction<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn DBTransaction + 'a>>> + 'a>>;

    fn write_nonce_if_not_used_and_return_is_nonce_used<'a>(
        &'a mut self,
        nonce: &'a NonceUuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>;

    // here we just do read we dont do here any set or check

    fn read_roles_for_user<'a>(
        &'a mut self,
        users_uuids: &'a HashSet<UserUuid>,
    ) -> Pin<Box<dyn Future<Output = Result<TheCompaniesAndBranchesHeIn>> + 'a>>;
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
