//! Cached reads racing invalidating writes, through the real cache helper.
//!
//! Needs an isolated Postgres and Redis named by `SQL_QUERY_TEST_DATABASE_URL`
//! and `SQL_QUERY_TEST_REDIS_URL`; there is no default. `scripts/cache-tests.sh`
//! starts throwaway containers, runs these serially and removes them. Every
//! test flushes Redis and recreates its tables, so they cannot run in parallel.

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use sql_query::{
    Id, SqlBase, SqlCols, SqlParam, SqlQ, Table, define_id,
    test_support::{PausePoint, pause_at},
};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

define_id!(ItemId);

#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct Item
{
    id: ItemId,
    name: String,
}

impl Table for Item
{
    type Col = ItemCol;
    type Id = ItemId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "fence_items";
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

/// Fresh tables and an empty Redis database.
async fn env() -> Env
{
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&required("SQL_QUERY_TEST_DATABASE_URL"))
        .await
        .expect("connect Postgres");
    let client = redis::Client::open(required("SQL_QUERY_TEST_REDIS_URL")).expect("redis url");
    let mut redis = client.get_multiplexed_async_connection().await.expect("connect Redis");
    redis::cmd("FLUSHDB").query_async::<()>(&mut redis).await.expect("FLUSHDB");
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS fence_items CASCADE;
         CREATE TABLE fence_items (id uuid PRIMARY KEY, name text NOT NULL);",
    )
    .execute(&pool)
    .await
    .expect("create tables");
    Env { pool, redis }
}

async fn insert_item(e: &Env, name: &str) -> ItemId
{
    let id = ItemId::new();
    SqlQ::insert::<Item>()
        .values([ItemCol::Id.eq(id), ItemCol::Name.eq(name)])
        .unwrap()
        .build()
        .unwrap()
        .bind()
        .skip_inval()
        .execute(&e.pool)
        .await
        .unwrap();
    id
}

/// Renames through the library's invalidating write path.
async fn rename(e: &mut Env, id: ItemId, name: &str)
{
    SqlQ::update::<Item>()
        .set([ItemCol::Name.eq(name)])
        .filter([ItemCol::Id.eq(id)])
        .build()
        .unwrap()
        .bind()
        .execute(&e.pool, &mut e.redis)
        .await
        .unwrap();
}

fn by_id(id: ItemId) -> sql_query::BoundQueryAs<Item>
{
    SqlQ::select::<Item>().filter([ItemCol::Id.eq(id)]).build().unwrap().bind_as::<Item>()
}

fn binds_id(binds: &[SqlParam], id: ItemId) -> bool
{
    binds.contains(&SqlParam::from(id))
}

/// A first-miss read whose SQL ran before a rename, and whose fill lands after
/// that rename's invalidation completed, must not leave the old row cached.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_fill_paused_across_a_completed_invalidation_is_not_served()
{
    let mut e = env().await;
    let id = insert_item(&e, "old").await;

    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    let read =
        tokio::spawn(async move { by_id(id).cached(3600).fetch_one(&pool, &mut redis).await });
    pause.reached().await;
    rename(&mut e, id, "new").await;
    pause.release();
    let _ = read.await.expect("read task");

    let cached = by_id(id).cached(3600).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    let fresh = by_id(id).fetch_one(&e.pool).await.unwrap();
    assert_eq!(
        cached.name, fresh.name,
        "the cached read serves {:?} while Postgres holds {:?}",
        cached.name, fresh.name
    );
}
