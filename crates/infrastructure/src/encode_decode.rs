use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

pub trait Coding {
    fn encode<T: Serialize>(data: &T) -> Vec<u8>;
    fn decode<'de, T: Deserialize<'de>>(data: &'de [u8]) -> Result<T>;
}

pub type Ed = target::S;

mod target {
    use super::Coding;
    use anyhow::Result;
    use postcard::from_bytes;
    use postcard::to_allocvec;
    use serde::Deserialize;
    use serde::Serialize;

    pub struct S;

    impl Coding for S {
        fn encode<T: Serialize>(data: &T) -> Vec<u8> {
            to_allocvec(&data).unwrap().to_vec()
        }

        fn decode<'de, T: Deserialize<'de>>(data: &'de [u8]) -> Result<T> {
            match from_bytes::<T>(data) {
                Ok(text) => Ok(text),
                Err(err) => Err(err.into()),
            }
        }
    }
}
