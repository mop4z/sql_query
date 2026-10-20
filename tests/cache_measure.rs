//! Cache cost measurements: hits, misses, wide dependency sets, sweeps and
//! contention. Written against API both cache protocols share — the result of
//! a cached read is only ever compared or turned into a string — so the same
//! file measures the unfenced helper and the fenced one on the same instance.
//!
//! Needs `SQL_QUERY_TEST_DATABASE_URL` and `SQL_QUERY_TEST_REDIS_URL`; run with
//! `bash scripts/cache-tests.sh --test cache_measure -- --nocapture`. Each
//! scenario prints one `MEASURE {json}` line; nothing here asserts a budget.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use redis::aio::MultiplexedConnection;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sql_query::{CacheTag, Id, SqlBase, SqlCols, SqlQ, Table, define_id, invalidate_tables};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

define_id!(RowId);

#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct Row
{
    id: RowId,
    name: String,
    amount: Decimal,
    big: i64,
}

impl Table for Row
{
    type Col = RowCol;
    type Id = RowId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "measure_rows";
}

/// Nine extra tables: with the FROM table, a ten-table dependency set.
struct Wide;

impl CacheTag for Wide
{
    fn tables(&self) -> &[&'static str]
    {
        &["m_t1", "m_t2", "m_t3", "m_t4", "m_t5", "m_t6", "m_t7", "m_t8", "m_t9"]
    }
}

struct Env
{
    pool: PgPool,
    redis: MultiplexedConnection,
}

fn required(var: &str) -> String
{
    std::env::var(var).unwrap_or_else(|_| {
        panic!("{var} is unset; run scripts/cache-tests.sh, which starts an isolated instance")
    })
}

async fn env(rows: usize) -> (Env, Vec<RowId>)
{
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&required("SQL_QUERY_TEST_DATABASE_URL"))
        .await
        .expect("connect Postgres");
    let client = redis::Client::open(required("SQL_QUERY_TEST_REDIS_URL")).expect("redis url");
    let mut redis = client.get_multiplexed_async_connection().await.expect("connect Redis");
    redis::cmd("FLUSHDB").query_async::<()>(&mut redis).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS measure_rows;
         CREATE TABLE measure_rows (
             id uuid PRIMARY KEY, name text NOT NULL, amount numeric NOT NULL, big bigint NOT NULL);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let ids: Vec<RowId> = (0..rows).map(|_| RowId::new()).collect();
    for chunk in ids.chunks(500)
    {
        SqlQ::insert::<Row>()
            .values_nested(chunk.iter().enumerate().map(|(i, id)| {
                [
                    RowCol::Id.eq(*id),
                    RowCol::Name.eq(format!("row-{i}-{}", "x".repeat(40))),
                    RowCol::Amount
                        .eq(Decimal::from_str_exact("12345678901234567890.123456789").unwrap()),
                    RowCol::Big.eq(i64::MAX - i64::try_from(i).unwrap()),
                ]
            }))
            .unwrap()
            .build()
            .unwrap()
            .bind()
            .skip_inval()
            .execute(&pool)
            .await
            .unwrap();
    }
    (Env { pool, redis }, ids)
}

fn by_id(id: RowId) -> sql_query::BoundQueryAs<Row>
{
    SqlQ::select::<Row>().filter([RowCol::Id.eq(id)]).build().unwrap().bind_as::<Row>()
}

/// One hundred rows: a large payload.
fn page() -> sql_query::BoundQueryAs<Row>
{
    SqlQ::select::<Row>()
        .order_by(RowCol::Id, sql_query::SqlOrder::Asc)
        .limit(100)
        .build()
        .unwrap()
        .bind_as::<Row>()
}

/// CPU time of the calling thread. With a current-thread runtime, all of a
/// scenario's client work — hashing, scripts' argument encoding, serde — runs
/// on it.
fn thread_cpu() -> Duration
{
    let s = std::fs::read_to_string("/proc/thread-self/schedstat").expect("schedstat");
    Duration::from_nanos(s.split_whitespace().next().unwrap().parse().unwrap())
}

async fn reset_stats(r: &mut MultiplexedConnection)
{
    redis::cmd("CONFIG").arg("RESETSTAT").query_async::<()>(r).await.unwrap();
}

/// `INFO commandstats` as name -> (calls, usec), without the measurement's own
/// INFO/CONFIG calls.
async fn command_stats(r: &mut MultiplexedConnection) -> BTreeMap<String, (u64, u64)>
{
    let info: String = redis::cmd("INFO").arg("commandstats").query_async(r).await.unwrap();
    let mut out = BTreeMap::new();
    for line in info.lines()
    {
        let Some(rest) = line.strip_prefix("cmdstat_")
        else
        {
            continue;
        };
        let (name, fields) = rest.split_once(':').unwrap();
        if name.starts_with("info") || name.starts_with("config")
        {
            continue;
        }
        let field = |k: &str| -> u64 {
            fields
                .split(',')
                .find_map(|f| f.strip_prefix(k).and_then(|v| v.strip_prefix('=')))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
        out.insert(name.to_string(), (field("calls"), field("usec")));
    }
    out
}

async fn used_memory(r: &mut MultiplexedConnection) -> i64
{
    let info: String = redis::cmd("INFO").arg("memory").query_async(r).await.unwrap();
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap()
}

fn percentile(sorted: &[Duration], p: f64) -> f64
{
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i].as_secs_f64() * 1e6
}

#[allow(clippy::cast_precision_loss)]
fn report(
    scenario: &str,
    ops: usize,
    mut lat: Vec<Duration>,
    cpu: Duration,
    stats: &BTreeMap<String, (u64, u64)>,
    extra: &serde_json::Value,
)
{
    lat.sort();
    let per_op: BTreeMap<_, _> =
        stats.iter().map(|(k, (calls, _))| (k.clone(), *calls as f64 / ops as f64)).collect();
    let redis_usec: u64 = stats.iter().filter(|(k, _)| *k != "flushdb").map(|(_, (_, u))| u).sum();
    let line = serde_json::json!({
        "scenario": scenario,
        "ops": ops,
        "p50_us": percentile(&lat, 0.50),
        "p95_us": percentile(&lat, 0.95),
        "p99_us": percentile(&lat, 0.99),
        "client_cpu_us_per_op": cpu.as_secs_f64() * 1e6 / ops as f64,
        "redis_calls_per_op": per_op,
        "redis_usec_per_op": redis_usec as f64 / ops as f64,
        "extra": extra,
    });
    println!("MEASURE {line}");
}

async fn timed<F: Future<Output = ()>>(lat: &mut Vec<Duration>, f: F)
{
    let t = Instant::now();
    f.await;
    lat.push(t.elapsed());
}

async fn hits_and_misses(e: &mut Env, ids: &[RowId], wide: bool, label: &str)
{
    let read = |id: RowId| {
        let q = by_id(id).cached(3600);
        if wide { q.tag(&Wide) } else { q }
    };

    // Misses: every id once, on a flushed cache.
    redis::cmd("FLUSHDB").query_async::<()>(&mut e.redis).await.unwrap();
    let before = used_memory(&mut e.redis).await;
    reset_stats(&mut e.redis).await;
    let (mut lat, cpu0) = (Vec::with_capacity(ids.len()), thread_cpu());
    for id in ids
    {
        timed(&mut lat, async {
            read(*id).fetch_one(&e.pool, &mut e.redis).await.unwrap();
        })
        .await;
    }
    let cpu = thread_cpu().saturating_sub(cpu0);
    let stats = command_stats(&mut e.redis).await;
    let after = used_memory(&mut e.redis).await;
    let key: Vec<String> =
        redis::cmd("KEYS").arg("sq:e:*").query_async(&mut e.redis).await.unwrap();
    let entry_bytes: i64 =
        redis::cmd("STRLEN").arg(&key[0]).query_async(&mut e.redis).await.unwrap();
    let entry_mem: i64 =
        redis::cmd("MEMORY").arg("USAGE").arg(&key[0]).query_async(&mut e.redis).await.unwrap();
    let payload_bytes =
        serde_json::to_vec(&by_id(ids[0]).fetch_one(&e.pool).await.unwrap()).unwrap().len();
    let payload_bytes = payload_bytes.min(usize::try_from(entry_bytes).unwrap());
    report(
        &format!("miss_{label}"),
        ids.len(),
        lat,
        cpu,
        &stats,
        &serde_json::json!({
            "used_memory_delta_per_entry": (after - before) / i64::try_from(ids.len()).unwrap(),
            "entry_strlen": entry_bytes,
            "entry_memory_usage": entry_mem,
            "payload_bytes": payload_bytes,
        }),
    );

    // Hits: every id again, now cached.
    reset_stats(&mut e.redis).await;
    let (mut lat, cpu0) = (Vec::with_capacity(ids.len()), thread_cpu());
    for id in ids
    {
        timed(&mut lat, async {
            read(*id).fetch_one(&e.pool, &mut e.redis).await.unwrap();
        })
        .await;
    }
    let cpu = thread_cpu().saturating_sub(cpu0);
    let stats = command_stats(&mut e.redis).await;
    report(&format!("hit_{label}"), ids.len(), lat, cpu, &stats, &serde_json::json!({}));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn measure_hits_misses_and_payloads()
{
    let (mut e, ids) = env(1000).await;
    // Warm Postgres and load the scripts before anything is counted.
    for id in &ids[..50]
    {
        by_id(*id).cached(3600).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    }
    hits_and_misses(&mut e, &ids, false, "1dep").await;
    hits_and_misses(&mut e, &ids, true, "10dep").await;

    // A 100-row payload, repeatedly missed (invalidated) and hit.
    for (label, wide) in [("page_1dep", false), ("page_10dep", true)]
    {
        let read = || {
            let q = page().cached(3600);
            if wide { q.tag(&Wide) } else { q }
        };
        let rounds = 200;
        let (mut miss, mut hit) = (Vec::new(), Vec::new());
        reset_stats(&mut e.redis).await;
        let cpu0 = thread_cpu();
        for _ in 0..rounds
        {
            invalidate_tables(&["measure_rows"], &mut e.redis).await.unwrap();
            timed(&mut miss, async {
                read().fetch_all(&e.pool, &mut e.redis).await.unwrap();
            })
            .await;
            timed(&mut hit, async {
                read().fetch_all(&e.pool, &mut e.redis).await.unwrap();
            })
            .await;
        }
        let cpu = thread_cpu().saturating_sub(cpu0);
        let stats = command_stats(&mut e.redis).await;
        let key: Vec<String> =
            redis::cmd("KEYS").arg("sq:e:*").query_async(&mut e.redis).await.unwrap();
        let strlen: i64 =
            redis::cmd("STRLEN").arg(&key[0]).query_async(&mut e.redis).await.unwrap();
        let payload = serde_json::to_vec(&page().fetch_all(&e.pool).await.unwrap()).unwrap().len();
        miss.sort();
        hit.sort();
        let (miss_p50, hit_p50) = (percentile(&miss, 0.5), percentile(&hit, 0.5));
        report(
            &format!("{label}_miss_then_hit"),
            rounds,
            miss.iter().zip(&hit).map(|(m, h)| *m + *h).collect(),
            cpu,
            &stats,
            &serde_json::json!({
                "miss_p50_us": miss_p50,
                "hit_p50_us": hit_p50,
                "entry_strlen": strlen,
                "payload_bytes": payload,
                "note": "per op = one invalidate + one miss + one hit",
            }),
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn measure_sweep_cardinality()
{
    let (mut e, ids) = env(10_000).await;
    for n in [100usize, 1000, 10_000]
    {
        redis::cmd("FLUSHDB").query_async::<()>(&mut e.redis).await.unwrap();
        for id in &ids[..n]
        {
            by_id(*id).cached(3600).fetch_one(&e.pool, &mut e.redis).await.unwrap();
        }
        let members: i64 =
            redis::cmd("SCARD").arg("sq:t:measure_rows").query_async(&mut e.redis).await.unwrap();
        reset_stats(&mut e.redis).await;
        let cpu0 = thread_cpu();
        let mut lat = Vec::new();
        timed(&mut lat, async {
            invalidate_tables(&["measure_rows"], &mut e.redis).await.unwrap();
        })
        .await;
        let cpu = thread_cpu().saturating_sub(cpu0);
        let stats = command_stats(&mut e.redis).await;
        report(
            &format!("sweep_{n}"),
            1,
            lat,
            cpu,
            &stats,
            &serde_json::json!({ "reverse_index_members": members }),
        );
    }
}

/// `c` readers hammer one hot key while a writer renames its row every 2 ms
/// through the invalidating path. Counts fenced fills by their message so the
/// file compiles against either protocol; checks the last read against
/// Postgres after the writer stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn measure_contention()
{
    let (mut e, ids) = env(1).await;
    let id = ids[0];
    for c in [1usize, 2, 4]
    {
        redis::cmd("FLUSHDB").query_async::<()>(&mut e.redis).await.unwrap();
        reset_stats(&mut e.redis).await;
        let reads_per_reader = 2000;
        let started = Instant::now();
        let readers: Vec<_> = (0..c)
            .map(|_| {
                let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
                tokio::spawn(async move {
                    let (mut ok, mut fenced, mut lat) = (0usize, 0usize, Vec::new());
                    for _ in 0..reads_per_reader
                    {
                        let t = Instant::now();
                        match by_id(id).cached(3600).fetch_one(&pool, &mut redis).await
                        {
                            Ok(_) => ok += 1,
                            Err(err) if err.to_string().contains("fenced") => fenced += 1,
                            Err(err) => panic!("{err}"),
                        }
                        lat.push(t.elapsed());
                    }
                    (ok, fenced, lat)
                })
            })
            .collect();
        let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
        let writer = tokio::spawn(async move {
            let mut writes = 0usize;
            loop
            {
                SqlQ::update::<Row>()
                    .set([RowCol::Name.eq(format!("w{writes}"))])
                    .filter([RowCol::Id.eq(id)])
                    .build()
                    .unwrap()
                    .bind()
                    .execute(&pool, &mut redis)
                    .await
                    .unwrap();
                writes += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
                if writes >= 400
                {
                    break writes;
                }
            }
        });
        let (mut ok, mut fenced, mut lat) = (0, 0, Vec::new());
        for r in readers
        {
            let (o, f, l) = r.await.unwrap();
            ok += o;
            fenced += f;
            lat.extend(l);
        }
        let write_count = writer.await.unwrap();
        let elapsed = started.elapsed();
        let stats = command_stats(&mut e.redis).await;
        let cached = by_id(id).cached(3600).fetch_one(&e.pool, &mut e.redis).await.unwrap();
        let fresh = by_id(id).fetch_one(&e.pool).await.unwrap();
        let ops = ok + fenced;
        #[allow(clippy::cast_precision_loss)]
        let extra = serde_json::json!({
            "readers": c,
            "writes": write_count,
            "fenced": fenced,
            "reads_per_sec": ops as f64 / elapsed.as_secs_f64(),
            "final_read_matches_postgres": cached == fresh,
            "note": "client CPU not attributed on a multi-thread runtime",
        });
        report(&format!("contention_c{c}"), ops, lat, Duration::ZERO, &stats, &extra);
    }
}
