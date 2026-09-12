use kernel::new_types::UuidType;
use uuid::Uuid;

pub trait MyUuidConverter {
    fn to_externel_uuid(&self) -> Uuid;
}

impl MyUuidConverter for UuidType {
    fn to_externel_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.clone().into_inner())
    }
}

pub trait MyUuidConverter1 {
    fn to_uuid(self) -> UuidType;
}

impl MyUuidConverter1 for Uuid {
    fn to_uuid(self) -> UuidType {
        UuidType::from(*self.as_bytes())
    }
}
