use serde::Deserialize;
use serde::Serialize;
use std::ops::Deref;

#[derive(Debug, Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct JsonWebTokenType(pub String);

impl Deref for JsonWebTokenType {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<String> for JsonWebTokenType {
    fn from(value: String) -> Self {
        Self(value)
    }
}

pub trait JWT: 'static {
    fn new() -> Self;
    fn sign<Subject: Serialize + for<'de> Deserialize<'de> + Clone>(
        &self,
        subject: &Subject,
    ) -> JsonWebTokenType;
    fn validate<Subject: Serialize + for<'de> Deserialize<'de> + Clone>(
        &self,
        token: JsonWebTokenType,
    ) -> Option<Subject>;
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "infrastructure")]
pub mod target {
    use super::JWT;
    use super::JsonWebTokenType;
    use chrono::Duration;
    use chrono::Utc;
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::DecodingKey;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use jsonwebtoken::Validation;
    use jsonwebtoken::decode;
    use jsonwebtoken::encode;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    pub struct S {
        key: Arc<Vec<u8>>,
    }

    fn exp_time() -> u64 {
        (Utc::now() + Duration::minutes(30)).timestamp() as u64
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Claims<Subject> {
        sub: Subject,
        exp: u64,
    }

    impl JWT for S {
        fn new() -> Self {
            Self {
                key: Arc::new("key".into()),
            }
        }

        fn sign<Subject: Serialize + for<'de> Deserialize<'de> + Clone>(
            &self,
            subject: &Subject,
        ) -> JsonWebTokenType {
            let claims = Claims {
                sub: subject.clone(),
                exp: exp_time(),
            };

            JsonWebTokenType(
                encode(&Header::default(), &claims, &EncodingKey::from_secret(&self.key)).unwrap(),
            )
        }

        fn validate<Subject: Serialize + for<'de> Deserialize<'de> + Clone>(
            &self,
            token: JsonWebTokenType,
        ) -> Option<Subject> {
            let result = decode::<Claims<Subject>>(
                &token.0,
                &DecodingKey::from_secret(&self.key),
                &Validation::new(Algorithm::HS256),
            );
            result.ok().map(|data| data.claims.sub)
        }
    }
}
