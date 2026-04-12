use std::fmt::Write;
use std::future::Future;

use redis::{AsyncCommands, Script};
use serde::{Serialize, de::DeserializeOwned};
use xxhash_rust::xxh3::Xxh3;

use crate::shared::value::SqlParam;

/// Computes a Redis entry key from the SQL string and bind parameters.
/// Format: `sq:e:{xxh3_hex}`. Stable across runs for the same SQL+binds.
///
/// Single streaming `Xxh3` pass — feeds the SQL bytes directly, then each bind
/// as a one-byte discriminant followed by raw payload bytes (no Debug
/// formatting, no intermediate `String`). `Json` / `Custom` variants share a
/// reusable scratch buffer for their fallback debug-string path.
#[must_use]
pub fn cache_key(sql: &str, binds: &[SqlParam]) -> String {
    let mut h = Xxh3::new();
    h.update(sql.as_bytes());
    h.update(&(binds.len() as u64).to_le_bytes());
    let mut scratch = String::new();
    for b in binds {
        b.hash_into(&mut h, &mut scratch);
    }
    let hash = h.digest();
    let mut key = String::with_capacity(5 + 16);
    let _ = write!(key, "sq:e:{hash:016x}");
    key
}

/// Builds the per-table reverse-index key for invalidation lookups.
fn table_key(table: &str) -> String {
    let mut s = String::with_capacity(5 + table.len());
    s.push_str("sq:t:");
    s.push_str(table);
    s
}

/// Generic cache-or-fetch helper. Checks Redis for a cached result using the
/// precomputed key, falling back to `fetch` on miss. On miss, stores the
/// serialized result and registers the entry under each table's reverse index
/// so a later `invalidate_tables` can sweep it.
pub async fn with_cache<T, F, Fut>(
    key: &str,
    ttl: u64,
    tables: &[&'static str],
    redis: &mut redis::aio::MultiplexedConnection,
    fetch: F,
) -> Result<T, sqlx::Error>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    if let Ok(Some(hit)) = redis.get::<&str, Option<String>>(key).await
        && let Ok(val) = serde_json::from_str(&hit)
    {
        tracing::debug!("HIT {key}");
        return Ok(val);
    }

    tracing::debug!("MISS {key}");
    let result = fetch().await?;

    if let Ok(json) = serde_json::to_string(&result) {
        let _: Result<(), _> = redis.set_ex::<_, _, ()>(key, &json, ttl).await;
        for t in tables {
            let _: Result<(), _> = redis.sadd::<_, _, ()>(table_key(t), key).await;
        }
    }

    Ok(result)
}

/// Invalidates every cached entry tagged with any of the given tables.
/// One Lua round-trip: walks each `sq:t:{table}` set, DELs each member entry,
/// then DELs the set itself. Errors bubble — write succeeded but cache may be
/// stale, caller needs to know.
pub async fn invalidate_tables(
    tables: &[&'static str],
    redis: &mut redis::aio::MultiplexedConnection,
) -> Result<(), sqlx::Error> {
    if tables.is_empty() {
        return Ok(());
    }
    let script = Script::new(
        r"
        for i = 1, #KEYS do
            local entries = redis.call('SMEMBERS', KEYS[i])
            for _, e in ipairs(entries) do
                redis.call('DEL', e)
            end
            redis.call('DEL', KEYS[i])
        end
        return 0
        ",
    );
    let mut invocation = script.prepare_invoke();
    for t in tables {
        invocation.key(table_key(t));
    }
    invocation
        .invoke_async::<()>(redis)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("cache invalidate failed: {e}")))?;
    Ok(())
}
