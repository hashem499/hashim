pub trait Regex: 'static {
    fn is_regex(s: &str) -> Result<(), String>;
}

pub type Rg = target::S;

mod target {
    use super::Regex as MyRegex;
    use regex::Regex;
    use std::sync::LazyLock;

    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[\p{L}\p{M}]+(?:[-'][\p{L}\p{M}]+)*$").unwrap());

    #[derive(Debug, Clone)]
    pub struct S;

    impl MyRegex for S {
        fn is_regex(s: &str) -> Result<(), String> {
            match RE.is_match(s) {
                true => Ok(()),
                false => Err("not match".to_string()),
            }
        }
    }
}
