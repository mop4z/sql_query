use std::{
    fmt::{self, Write},
    future::Future,
    sync::LazyLock,
};

use redis::{Script, Value};
use serde::{Serialize, de::DeserializeOwned};
use xxhash_rust::xxh3::Xxh3;

#[cfg(feature = "test-support")]
use crate::shared::test_support::{self, PausePoint};
use crate::shared::value::SqlParam;

/// Trait for grouping related tables under a single tag.
/// Implement on an enum to define table groups for cache invalidation.
///
/// ```ignore
/// enum MyTags { UserBalance, Pricing }
/// impl CacheTag for MyTags {
///     fn tables(&self) -> &[&'static str] {
///         match self {
///             Self::UserBalance => &["users", "balances", "transactions"],
///             Self::Pricing => &["products", "discounts"],
///         }
///     }
/// }
/// ```
pub trait CacheTag
{
    fn tables(&self) -> &[&'static str];
}

/// Computes a Redis entry key from the SQL string and bind parameters.
/// Format: `sq:e:{xxh3_hex}`. Stable across runs for the same SQL+binds.
///
/// Single streaming `Xxh3` pass — feeds the SQL bytes directly, then each bind
/// as a one-byte discriminant followed by raw payload bytes (no Debug
/// formatting, no intermediate `String`). `Json` / `Custom` variants share a
/// reusable scratch buffer for their fallback debug-string path.
#[must_use]
pub fn cache_key(sql: &str, binds: &[SqlParam]) -> String
{
    let mut h = Xxh3::new();
    h.update(sql.as_bytes());
    h.update(&(binds.len() as u64).to_le_bytes());
    let mut scratch = String::new();
    for b in binds
    {
        b.hash_into(&mut h, &mut scratch);
    }
    let hash = h.digest();
    let mut key = String::with_capacity(5 + 16);
    let _ = write!(key, "sq:e:{hash:016x}");
    key
}

/// Builds the per-table reverse-index key for invalidation lookups.
fn table_key(table: &str) -> String
{
    let mut s = String::with_capacity(5 + table.len());
    s.push_str("sq:t:");
    s.push_str(table);
    s
}

/// Redis hash holding one opaque generation token per table. It never
/// expires; a missing field (flush, eviction) is re-created with a fresh
/// token, so state that disappears can never be mistaken for state that
/// stayed.
const GENERATIONS: &str = "sq:g:v1";

/// A token no earlier generation of any table can hold. A v7 UUID: strictly
/// ordered within a process, and across processes and restarts distinct by
/// timestamp and random bits — a practical guarantee, not a mathematical one.
fn fresh_token() -> String
{
    uuid::Uuid::now_v7().simple().to_string()
}

/// The dependency set an entry is stored and validated under: every table the
/// query reads plus every tagged table, sorted and deduplicated.
fn dependencies(tables: &[&'static str]) -> Vec<&'static str>
{
    let mut deps = tables.to_vec();
    deps.sort_unstable();
    deps.dedup();
    deps
}

/// Shared by the lookup and fill scripts. An entry's value is this header
/// followed by the serialized result, byte for byte. The header names every
/// dependency with its generation token, each length-prefixed, so no header is
/// a prefix of another and a prefix comparison is an exact one. The payload is
/// never parsed in Lua: JSON numbers stay the bytes serde wrote.
const HEADER_LUA: &str = r"
local function header(names, tokens)
    local parts = {'sqc1:', #names, ';'}
    for i = 1, #names do
        local n, t = names[i], tokens[i]
        parts[#parts + 1] = #n .. ':' .. n .. #t .. ':' .. t
    end
    return table.concat(parts)
end
";

/// KEYS: generation hash, entry. ARGV: fresh token, sorted table names.
/// Reads before it writes, so a wrong-typed key fails before any mutation.
/// Missing generations get the fresh token. Replies `{1, header_len, entry}`
/// when the entry was stored under exactly these tables at exactly their
/// current generations, else `{0, header}`: the snapshot a fill must still
/// match.
const LOOKUP_LUA: &str = r"
local names = {}
for i = 2, #ARGV do names[i - 1] = ARGV[i] end
if #names == 0 then return redis.error_reply('ERR sq lookup: no tables') end
local entry = redis.call('GET', KEYS[2])
local tokens = redis.call('HMGET', KEYS[1], unpack(names))
local missing = {}
for i = 1, #names do
    if not tokens[i] then
        tokens[i] = ARGV[1]
        missing[#missing + 1] = names[i]
        missing[#missing + 1] = ARGV[1]
    end
end
if #missing > 0 then redis.call('HSET', KEYS[1], unpack(missing)) end
local h = header(names, tokens)
if entry and string.sub(entry, 1, #h) == h then return {1, #h, entry} end
return {0, h}
";

/// KEYS: generation hash, entry, one reverse index per table. ARGV: TTL,
/// envelope (lookup header followed by payload), sorted table names. Writes
/// only if every generation still matches the header; a missing generation
/// never matches. Replies 1 when written, 0 when fenced.
const FILL_LUA: &str = r"
local ttl = tonumber(ARGV[1])
if not ttl or ttl < 1 or #KEYS < 3 or #ARGV ~= #KEYS then
    return redis.error_reply('ERR sq fill: bad arguments')
end
for i = 3, #KEYS do
    local kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'set' and kind ~= 'none' then
        return redis.error_reply('WRONGTYPE sq fill: ' .. KEYS[i] .. ' is not a set')
    end
end
local names = {}
for i = 3, #ARGV do names[i - 2] = ARGV[i] end
local tokens = redis.call('HMGET', KEYS[1], unpack(names))
for i = 1, #names do
    if not tokens[i] then return 0 end
end
local h = header(names, tokens)
if string.sub(ARGV[2], 1, #h) ~= h then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[1])
for i = 3, #KEYS do redis.call('SADD', KEYS[i], KEYS[2]) end
return 1
";

/// KEYS: generation hash, one reverse index per table. ARGV: fresh token,
/// table names. Checks every key's type, advances every table's generation —
/// whether or not anything is cached under it — then sweeps the entries.
const INVALIDATE_LUA: &str = r"
if #ARGV ~= #KEYS or #KEYS < 2 then
    return redis.error_reply('ERR sq invalidate: bad arguments')
end
local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'hash' and kind ~= 'none' then
    return redis.error_reply('WRONGTYPE sq invalidate: ' .. KEYS[1] .. ' is not a hash')
end
for i = 2, #KEYS do
    kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'set' and kind ~= 'none' then
        return redis.error_reply('WRONGTYPE sq invalidate: ' .. KEYS[i] .. ' is not a set')
    end
end
local fields = {}
for i = 2, #ARGV do
    fields[#fields + 1] = ARGV[i]
    fields[#fields + 1] = ARGV[1]
end
redis.call('HSET', KEYS[1], unpack(fields))
for i = 2, #KEYS do
    local entries = redis.call('SMEMBERS', KEYS[i])
    for _, e in ipairs(entries) do
        redis.call('DEL', e)
    end
    redis.call('DEL', KEYS[i])
end
return 0
";

static LOOKUP: LazyLock<Script> = LazyLock::new(|| Script::new(&[HEADER_LUA, LOOKUP_LUA].concat()));
static FILL: LazyLock<Script> = LazyLock::new(|| Script::new(&[HEADER_LUA, FILL_LUA].concat()));
static INVALIDATE: LazyLock<Script> = LazyLock::new(|| Script::new(INVALIDATE_LUA));

/// Why a cached read returned no rows.
#[derive(Debug)]
pub enum CacheError
{
    /// Postgres failed; the original error, unwrapped.
    Database(sqlx::Error),
    /// Redis failed during lookup or fill. Whether a fill's script ran is
    /// unknown, but a fill that ran was fenced.
    Redis(redis::RedisError),
    /// Redis answered a cache script with a reply outside its protocol.
    Protocol(&'static str),
    /// A table the read depends on was invalidated after the read's
    /// generation snapshot was taken and before its fill, so the result may
    /// predate that write. Nothing was written to Redis. Executing the same
    /// query uncached is safe; serving the discarded result is not.
    GenerationChanged,
}

impl fmt::Display for CacheError
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        match self
        {
            Self::Database(e) => write!(f, "database: {e}"),
            Self::Redis(e) => write!(f, "cache: {e}"),
            Self::Protocol(what) => write!(f, "cache protocol: {what}"),
            Self::GenerationChanged =>
            {
                write!(f, "cache fill fenced: a dependency was invalidated while the read ran")
            }
        }
    }
}

impl std::error::Error for CacheError
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)>
    {
        match self
        {
            Self::Database(e) => Some(e),
            Self::Redis(e) => Some(e),
            Self::Protocol(_) | Self::GenerationChanged => None,
        }
    }
}

enum Lookup
{
    /// `entry[..header_len]` is the current header; the rest is the payload.
    Hit
    {
        entry: Vec<u8>, header_len: usize
    },
    Miss
    {
        header: Vec<u8>
    },
}

async fn lookup(
    key: &str,
    deps: &[&'static str],
    redis: &mut redis::aio::MultiplexedConnection,
) -> Result<Lookup, CacheError>
{
    let mut call = LOOKUP.prepare_invoke();
    call.key(GENERATIONS).key(key).arg(fresh_token());
    for t in deps
    {
        call.arg(*t);
    }
    let reply: Value = call.invoke_async(redis).await.map_err(CacheError::Redis)?;
    let Value::Array(parts) = reply
    else
    {
        return Err(CacheError::Protocol("lookup reply is not an array"));
    };
    match <[Value; 3]>::try_from(parts)
    {
        Ok([Value::Int(1), Value::Int(len), Value::BulkString(entry)]) =>
        {
            let header_len = usize::try_from(len)
                .ok()
                .filter(|&n| n <= entry.len())
                .ok_or(CacheError::Protocol("lookup header length out of range"))?;
            Ok(Lookup::Hit { entry, header_len })
        }
        Ok(_) => Err(CacheError::Protocol("malformed lookup hit")),
        Err(parts) => match <[Value; 2]>::try_from(parts)
        {
            Ok([Value::Int(0), Value::BulkString(header)]) => Ok(Lookup::Miss { header }),
            _ => Err(CacheError::Protocol("malformed lookup miss")),
        },
    }
}

/// Writes `envelope` under `key` and registers it with every dependency, if
/// no dependency's generation moved since the lookup that produced its header.
async fn fill(
    key: &str,
    deps: &[&'static str],
    ttl: u64,
    envelope: &[u8],
    redis: &mut redis::aio::MultiplexedConnection,
) -> Result<bool, CacheError>
{
    let mut call = FILL.prepare_invoke();
    call.key(GENERATIONS).key(key);
    for t in deps
    {
        call.key(table_key(t));
    }
    call.arg(ttl).arg(envelope);
    for t in deps
    {
        call.arg(*t);
    }
    match call.invoke_async(redis).await.map_err(CacheError::Redis)?
    {
        Value::Int(1) => Ok(true),
        Value::Int(0) => Ok(false),
        _ => Err(CacheError::Protocol("malformed fill reply")),
    }
}

/// Generic cache-or-fetch helper. One script validates the entry against the
/// current generation of every dependency and, on a miss, returns the
/// snapshot taken before SQL runs; a second script stores the result only if
/// that snapshot still holds, and registers it under each table's reverse
/// index. A read with no dependency or a zero TTL goes to Postgres and never
/// touches Redis: there is nothing that could invalidate what it stored.
///
/// A hit whose payload does not decode as `T` is treated as a miss and
/// refilled under the same snapshot.
pub(crate) async fn with_cache<T, F, Fut>(
    sql: String,
    binds: Vec<SqlParam>,
    ttl: u64,
    tables: &[&'static str],
    redis: &mut redis::aio::MultiplexedConnection,
    fetch: F,
) -> Result<T, CacheError>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce(String, Vec<SqlParam>) -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let deps = dependencies(tables);
    if deps.is_empty() || ttl == 0
    {
        return fetch(sql, binds).await.map_err(CacheError::Database);
    }
    let key = cache_key(&sql, &binds);
    #[cfg(feature = "test-support")]
    let (site_sql, site_binds) = (sql.clone(), binds.clone());
    #[cfg(feature = "test-support")]
    let site = |hit| test_support::PauseSite {
        key: &key,
        sql: &site_sql,
        binds: &site_binds,
        tables: &deps,
        hit,
    };

    let header = match lookup(&key, &deps, redis).await?
    {
        Lookup::Hit { mut entry, header_len } =>
        {
            if let Ok(val) = serde_json::from_slice(&entry[header_len..])
            {
                tracing::debug!("HIT {key} {sql}");
                #[cfg(feature = "test-support")]
                test_support::hook(PausePoint::AfterLookup, &site(true)).await;
                return Ok(val);
            }
            entry.truncate(header_len);
            entry
        }
        Lookup::Miss { header } => header,
    };

    tracing::debug!("MISS {key} {sql}");
    #[cfg(feature = "test-support")]
    test_support::hook(PausePoint::AfterLookup, &site(false)).await;
    let result = fetch(sql, binds).await.map_err(CacheError::Database)?;
    #[cfg(feature = "test-support")]
    test_support::hook(PausePoint::BeforeFill, &site(false)).await;

    let mut envelope = header;
    if serde_json::to_writer(&mut envelope, &result).is_err()
    {
        return Ok(result);
    }
    if fill(&key, &deps, ttl, &envelope, redis).await?
    {
        Ok(result)
    }
    else
    {
        tracing::debug!("FENCED {key}");
        Err(CacheError::GenerationChanged)
    }
}

/// Invalidates every cached entry tagged with any of the given tables. One
/// Lua round-trip: gives each table a fresh generation — so a fill whose
/// snapshot predates this call is refused, and so is any entry the sweep
/// cannot reach — then walks each `sq:t:{table}` set, DELs each member entry
/// and DELs the set itself. Errors bubble — write succeeded but cache may be
/// stale, caller needs to know.
///
/// # Errors
/// Returns `sqlx::Error::Protocol` if the Redis invalidation script fails.
pub async fn invalidate_tables(
    tables: &[&'static str],
    redis: &mut redis::aio::MultiplexedConnection,
) -> Result<(), sqlx::Error>
{
    if tables.is_empty()
    {
        return Ok(());
    }
    let mut call = INVALIDATE.prepare_invoke();
    call.key(GENERATIONS);
    for t in tables
    {
        call.key(table_key(t));
    }
    call.arg(fresh_token());
    for t in tables
    {
        call.arg(*t);
    }
    call.invoke_async::<()>(redis)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("cache invalidate failed: {e}")))?;
    Ok(())
}
