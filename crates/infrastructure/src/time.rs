pub trait Time: 'static {
    fn now_as_unix_milliseconds() -> u64;
}

pub type Ti = target::S;

mod target {
    use super::Time;
    use chrono::Utc;

    pub struct S;

    impl Time for S {
        fn now_as_unix_milliseconds() -> u64 {
            Utc::now().timestamp_millis() as u64
        }
    }
}
