pub trait RandomNumber: 'static {
    fn generate() -> u64;
}

pub type Rn = target::S;

pub mod target {
    use super::RandomNumber;
    use getrandom::fill;

    pub struct S;

    impl RandomNumber for S {
        fn generate() -> u64 {
            let mut buf = [0u8; 8];
            fill(&mut buf).unwrap();
            u64::from_ne_bytes(buf)
        }
    }
}
