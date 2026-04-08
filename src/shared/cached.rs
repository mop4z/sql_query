use std::future::Future;

use redis::AsyncCommands;
use serde::{Serialize, de::DeserializeOwned};
use xxhash_rust::xxh3::xxh3_64;

use crate::shared::value::SqlParam;

/// Computes a cache key from the SQL string and bind parameters.
pub(crate) fn cache_key(sql: &str, binds: &[SqlParam]) -> String {
    let mut input = sql.to_lowercase();
    for b in binds {
        input.push_str(&format!("{:?}", b));
    }
    let hash = xxh3_64(input.as_bytes());
    format!("sq:{hash:016x}")
}

/// Generic cache-or-fetch helper. Checks Redis for a cached result using the
/// precomputed key, falling back to `fetch` on miss. Stores the result on miss.
pub(crate) async fn with_cache<T, F, Fut>(
    key: &str,
    ttl: u64,
    redis: &mut redis::aio::MultiplexedConnection,
    fetch: F,
) -> Result<T, sqlx::Error>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    if let Ok(Some(hit)) = redis.get::<&str, Option<String>>(key).await {
        if let Ok(val) = serde_json::from_str(&hit) {
            return Ok(val);
        }
    }

    let result = fetch().await?;

    if let Ok(json) = serde_json::to_string(&result) {
        let _: Result<(), _> = redis.set_ex(key, &json, ttl).await;
    }

    Ok(result)
}
