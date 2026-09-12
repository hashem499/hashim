use crate::db_transaction;
use crate::utils::MyUuidConverter;
use anyhow::Result;
use deadpool_postgres::Object;
use kernel::new_types::BranchUuid;
use kernel::new_types::CompanyUuid;
use kernel::new_types::NonceUuid;
use kernel::new_types::UserUuid;
use kernel::new_types::UuidType;
use kernel::server::DBClient;
use kernel::server::DBTransaction;
use kernel::server::TheCompaniesAndBranchesHeIn;
use kernel::types::Role;
use std::any::Any;
use std::collections::HashSet;
use std::pin::Pin;
use std::str::FromStr;
use utility::types::LogError;
use uuid::Uuid;

const READ_ROLES_FOR_USER_QUERY: &str = r#"
    SELECT
        'company' as type,
        data_group,
        role,
        user_
    FROM accounting_app.access_control_for_company
    WHERE user_ = $1

    UNION ALL

    SELECT
        'branch' as type,
        data_group,
        role,
        user_
    FROM accounting_app.access_control_for_company_branch
    WHERE user_ = $1
"#;

const WRITE_NONCE_IF_NOT_USED_QUERY: &str = "
    INSERT INTO accounting_app.transaction_number (rowid) VALUES ($1)
    ON CONFLICT (rowid) DO NOTHING
    RETURNING true";

pub struct S {
    pub(crate) client: Object,
}

impl DBClient for S {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn begin_transaction<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn DBTransaction + 'a>>> + 'a>> {
        Box::pin(async move {
            let txn = db_transaction::S {
                txn: self.client.transaction().await.log()?,
            };
            let boxed: Box<dyn DBTransaction + 'a> = Box::new(txn);
            Ok(boxed)
        })
    }

    fn write_nonce_if_not_used_and_return_is_nonce_used<'a>(
        &'a mut self,
        nonce: &'a NonceUuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>> {
        Box::pin(async {
            let row = self
                .client
                .query_one(WRITE_NONCE_IF_NOT_USED_QUERY, &[&nonce.to_externel_uuid()])
                .await
                .log()?;

            let inserted: Option<Uuid> = row.try_get(0).ok();
            Ok(inserted.is_some())
        })
    }

    fn read_roles_for_user<'a>(
        &'a mut self,
        users_uuid: &'a HashSet<UserUuid>,
    ) -> Pin<Box<dyn Future<Output = Result<TheCompaniesAndBranchesHeIn>> + 'a>> {
        Box::pin(async move {
            let stmt = self.client.prepare_cached(READ_ROLES_FOR_USER_QUERY).await.log()?;

            let mut result = TheCompaniesAndBranchesHeIn {
                companies:                Default::default(),
                branches:                 Default::default(),
                branches_of_each_company: Default::default(),
            };

            for user_uuid in users_uuid {
                let user_id_param = user_uuid.clone().to_externel_uuid();

                let rows = self.client.query(&stmt, &[&user_id_param]).await.log()?;

                for row in rows {
                    let entity_type: String = row.try_get("type").log()?;
                    let data_group: Uuid = row.try_get("data_group").log()?;
                    let role_str: String = row.try_get("role").log()?;
                    let user_id: Uuid = row.try_get("user_").log()?;

                    let role = Role::from_str(&role_str).log()?;

                    let data_group_id: UuidType = UuidType::from(data_group.into_bytes()).into();
                    let user_id_typed = UuidType::from(user_id.into_bytes()).into();

                    match entity_type.as_str() {
                        "company" => {
                            result
                                .companies
                                .entry(user_id_typed)
                                .or_default()
                                .insert(CompanyUuid::from(data_group_id));
                        }
                        "branch" => {
                            result
                                .branches
                                .entry(user_id_typed)
                                .or_default()
                                .insert(BranchUuid::from(data_group_id));
                        }
                        _ => {}
                    }
                }
            }

            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::test_query_helper;

    #[tokio::test]
    async fn test_query_string_directly() {
        test_query_helper(READ_ROLES_FOR_USER_QUERY).await.unwrap();
        test_query_helper(WRITE_NONCE_IF_NOT_USED_QUERY).await.unwrap();
    }
}
