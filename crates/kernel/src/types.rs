use crate::new_types::BranchUuid;
use crate::new_types::CompanyUuid;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::pin::Pin;
use std::str::FromStr;
use utility::types::DynamicError;

pub trait DatabaseRead: Sync + Send {
    type Db;
    type Input;
    type Output;

    fn read(
        &self,
        db: &mut Self::Db,
        input: &Self::Input,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output, DynamicError>>>>;
}

pub trait DatabaseWrite {
    type Db<'a>;
    type Input;

    fn write(
        txn: &mut Self::Db<'_>,
        input: &Self::Input,
    ) -> impl Future<Output = Result<(), DynamicError>>;
}

pub trait MyErrorTrait {
    fn is_there_error(&self) -> bool;
}

pub trait MarkerMyErrorTrait {}

impl<T: MarkerMyErrorTrait + Default + PartialEq> MyErrorTrait for T {
    fn is_there_error(&self) -> bool {
        *self != Self::default()
    }
}

pub type ListOfCompanies = Vec<Company>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Company {
    pub uuid:     CompanyUuid,
    pub name:     String,
    pub role:     Role,
    pub branches: Vec<Branch>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Branch {
    pub uuid: BranchUuid,
    pub name: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Location {
    pub latitude:  f64,
    pub longitude: f64,
}

impl Location {
    pub fn is_valid(&self) -> bool {
        self.latitude >= -90.0
            && self.latitude <= 90.0
            && self.longitude >= -180.0
            && self.longitude <= 180.0
            && self.latitude.is_finite()
            && self.longitude.is_finite()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub enum Currency {
    #[default]
    USD,
    IQD,
}

impl FromStr for Currency {
    type Err = DynamicError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "USD" => Ok(Currency::USD),
            "IQD" => Ok(Currency::IQD),
            _ => Err("not exist".into()),
        }
    }
}

impl Currency {
    pub fn as_str(&self) -> &str {
        match self {
            Currency::USD => "USD",
            Currency::IQD => "IQD",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub enum Role {
    #[default]
    Manager,
    CoManager,
}

impl FromStr for Role {
    type Err = DynamicError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Manager" => Ok(Role::Manager),
            "CoManager" => Ok(Role::CoManager),
            _ => Err("not exist".into()),
        }
    }
}

impl Role {
    pub fn as_str(&self) -> &str {
        match self {
            Role::Manager => "Manager",
            Role::CoManager => "CoManager",
        }
    }

    pub fn has_any(user_roles: &[Self], roles: &[Role]) -> bool {
        for role in roles {
            if user_roles.contains(role) {
                return true;
            }
        }
        false
    }
}

pub const HOST: &str = "127.0.0.1";
pub const PORT: u16 = 8081;
pub const ADDRESS: &str = "127.0.0.1:8081";

// there should be no generic in all the below types

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum UserUuidError {
    Invalid,
    NotAuthenticated,
    YouDontHavePermissionToDoThat,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum RowIdError {
    Invalid,
    Duplicated,
    NotExist,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum NonceError {
    Invalid,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum JWTError {
    Invalid,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum HashimError {
    InternalServerError,
    InvalidDataFormat,
    ConnectionClosed,
}

impl Error for HashimError {}

impl Display for HashimError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HashimError::InternalServerError => write!(f, "Internal Server Error"),
            HashimError::InvalidDataFormat => write!(f, "Invalid Data Format"),
            HashimError::ConnectionClosed => write!(f, "Connection Closed"),
        }
    }
}
