type UuidType = [u8; 16];

pub trait RowId: 'static {
    fn parse(s: impl AsRef<str>) -> Option<UuidType>;
    fn generate() -> UuidType;
    fn get_time_as_seconds(uuid: &UuidType) -> Option<u64>;
    fn validate(uuid: &UuidType) -> bool;
}

pub type Id = target::S;

mod target {
    use super::RowId;
    use super::UuidType;
    use uuid::Uuid;

    pub struct S;

    impl RowId for S {
        fn parse(s: impl AsRef<str>) -> Option<UuidType> {
            let uuid = Uuid::parse_str(s.as_ref()).ok()?;
            Some(*uuid.as_bytes())
        }

        fn generate() -> UuidType {
            *Uuid::now_v7().as_bytes()
        }

        fn get_time_as_seconds(uuid: &UuidType) -> Option<u64> {
            let u = Uuid::from_bytes(*uuid);
            u.get_timestamp().map(|ts| ts.to_unix().0)
        }

        fn validate(uuid: &UuidType) -> bool {
            let u = Uuid::from_bytes(*uuid);
            matches!(u.get_version_num(), 7)
        }
    }
}
