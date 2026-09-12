use anyhow::Result;
use infrastructure::row_id::Id;
use infrastructure::row_id::RowId;
use kernel::new_types::AccountUuid;
use kernel::new_types::CompanyUuid;
use kernel::new_types::UserUuid;
use kernel::types::DatabaseRead;
use kernel::types::MarkerMyErrorTrait;
use kernel::types::Role;
use kernel::types::RowIdError;
use kernel::types::UserUuidError;
use serde::Deserialize;
use serde::Serialize;

pub type MyResult = Result<Ok, Error>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Input {
    pub user_uuid:                       UserUuid,
    pub new_uuid:                        AccountUuid,
    pub is_debit:                        bool,
    pub is_permanent_account:            bool,
    pub account_name:                    String,
    pub notes:                           Option<String>,
    pub unit_of_measurement_of_quantity: String,
    pub belong_to_company:               CompanyUuid,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Ok {
    pub new_uuid:                        AccountUuid,
    pub is_debit:                        bool,
    pub is_permanent_account:            bool,
    pub account_name:                    String,
    pub notes:                           Option<String>,
    pub unit_of_measurement_of_quantity: String,
    pub belong_to_company:               CompanyUuid,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct Error {
    pub(crate) user_uuid:         Option<UserUuidError>,
    pub(crate) new_uuid:          Option<RowIdError>,
    pub(crate) belong_to_company: Option<RowIdError>,
    pub(crate) account_name:      Option<AccountNameError>,
}

impl MarkerMyErrorTrait for Error {}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) enum AccountNameError {
    Duplicated,
}

pub struct ReadInput {
    pub user_uuid:         UserUuid,
    pub new_uuid:          AccountUuid,
    pub belong_to_company: CompanyUuid,
    pub account_name:      String,
}

pub struct ReadOutput {
    pub is_company_uuid_exist: bool,
    pub is_new_uuid_used:      bool,
    pub user_roles:            Vec<Role>,
    pub is_account_name_used:  bool,
}

impl Input {
    pub(crate) fn state_less_check(&self) -> Error {
        let mut errr = Error::default();

        if !Id::validate(&self.new_uuid) {
            errr.new_uuid = Some(RowIdError::Invalid);
        }

        if !Id::validate(&self.user_uuid) {
            errr.user_uuid = Some(UserUuidError::Invalid);
        }

        if !Id::validate(&self.belong_to_company) {
            errr.belong_to_company = Some(RowIdError::Invalid);
        }
        errr
    }

    pub(crate) async fn state_full_check<
        Db: DatabaseRead<Input = ReadInput, Output = ReadOutput>,
    >(
        &self,
        db: &mut Db::Db<'_>,
    ) -> Result<Error> {
        let read_output = Db::read(db, &ReadInput {
            user_uuid:         self.user_uuid.clone(),
            new_uuid:          self.new_uuid.clone(),
            belong_to_company: self.belong_to_company.clone(),
            account_name:      self.account_name.clone(),
        })
        .await?;

        let mut errr = Error::default();

        if read_output.is_new_uuid_used {
            errr.new_uuid = Some(RowIdError::Duplicated);
        }

        if !read_output.is_company_uuid_exist {
            errr.belong_to_company = Some(RowIdError::NotExist);
        }

        if read_output.is_account_name_used {
            errr.account_name = Some(AccountNameError::Duplicated);
        }

        if !Role::has_any(&read_output.user_roles, &[Role::Manager, Role::CoManager]) {
            errr.user_uuid = Some(UserUuidError::YouDontHavePermissionToDoThat);
        }

        Ok(errr)
    }

    pub(crate) fn state_less_operation(&self) -> Ok {
        Ok {
            new_uuid:                        self.new_uuid.clone(),
            is_debit:                        self.is_debit,
            is_permanent_account:            self.is_permanent_account,
            account_name:                    self.account_name.clone(),
            notes:                           self.notes.clone(),
            unit_of_measurement_of_quantity: self.unit_of_measurement_of_quantity.clone(),
            belong_to_company:               self.belong_to_company.clone(),
        }
    }
}
