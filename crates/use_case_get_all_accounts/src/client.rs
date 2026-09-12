use crate::domain::Input;
use infrastructure::random_number::RandomNumber;
use infrastructure::random_number::Rn;
use kernel::new_types::CompanyUuid;
use kernel::new_types::UserUuid;
use kernel::request_response::TypeOperationsInput;
use kernel::request_response::TypeOperationsResult;
use utility::cache::CacheStruct;
use utility::cache::CachingStrategy;
use utility::ui_orchestration::Subscribe;

pub async fn fetch(
    selected_company: CompanyUuid,
    user_uuid: UserUuid,
    mut cache: CacheStruct<Subscribe, TypeOperationsInput, TypeOperationsResult>,
) {
    let input = Input {
        user_uuid,
        company_uuid: selected_company,
    }
    .into();

    let txn_number = Rn::generate();

    cache.send_to_cache_actor(CachingStrategy::ReadServerOnly, txn_number, input).await;
}
