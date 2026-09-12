use serde::Deserialize;
use serde::Serialize;
use std::ops::Deref;

macro_rules! make_type {
    ($inner_type:ty, $new_type:ident) => {
        #[derive(
            Debug, Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize,
        )]
        pub struct $new_type($inner_type);

        impl Deref for $new_type {
            type Target = $inner_type;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl From<$inner_type> for $new_type {
            fn from(value: $inner_type) -> Self {
                Self(value)
            }
        }
        impl $new_type {
            pub fn into_inner(self) -> $inner_type {
                self.0
            }
        }
    };
}

make_type!([u8; 16], UuidType);
make_type!(UuidType, NonceUuid);
make_type!(UuidType, UserUuid);
make_type!(UuidType, CompanyUuid);
make_type!(UuidType, BranchUuid);
make_type!(UuidType, AccountUuid);
make_type!(UuidType, AccountForBranchUuid);
make_type!(UuidType, SharedEntryUuid);
make_type!(String, JsonWebTokenType);
