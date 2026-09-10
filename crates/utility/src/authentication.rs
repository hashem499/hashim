pub trait HashedPassword {
    fn sign_up(password: &str) -> String;
    fn sign_in(password: &str, password_hash: &str) -> bool;
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "infrastructure")]
pub mod target {
    use super::HashedPassword;
    use argon2::Argon2;
    use argon2::PasswordHash;
    use argon2::PasswordHasher;
    use argon2::PasswordVerifier;
    use argon2::password_hash::SaltString;
    use argon2::password_hash::rand_core::OsRng;
    use derive_more::From;

    #[derive(Debug, Clone, From)]
    pub struct S;

    impl HashedPassword for S {
        fn sign_up(password: &str) -> String {
            let salt = SaltString::generate(&mut OsRng);
            let argon2 = Argon2::default();
            argon2.hash_password(password.as_bytes(), &salt).unwrap().to_string()
        }

        fn sign_in(password: &str, password_hash: &str) -> bool {
            let parsed_hash = PasswordHash::new(password_hash).unwrap();
            Argon2::default().verify_password(password.as_bytes(), &parsed_hash).is_ok()
        }
    }
}
