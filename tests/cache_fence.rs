//! Cached reads racing invalidating writes, through the real cache helper.
//!
//! Needs an isolated Postgres and Redis named by `SQL_QUERY_TEST_DATABASE_URL`
//! and `SQL_QUERY_TEST_REDIS_URL`; there is no default. `scripts/cache-tests.sh`
//! starts throwaway containers, runs these serially and removes them. Every
//! test flushes Redis and recreates its tables, so they cannot run in parallel.
//!
//! A hit is told from a miss by changing Postgres behind the cache's back
//! (`set_name_uncached`): a hit still serves the old value, a miss the new one.

use std::{future::Future, pin::Pin};

use redis::aio::MultiplexedConnection;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sql_query::{
    CacheError, CacheTag, Expr, Id, SqlBase, SqlCols, SqlJoin, SqlParam, SqlQ, Table, define_id,
    invalidate_tables,
    test_support::{PausePoint, pause_at},
};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

define_id!(ItemId);
define_id!(OwnerId);

#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct Item
{
    id: ItemId,
    name: String,
    amount: Decimal,
    big: i64,
}

impl Table for Item
{
    type Col = ItemCol;
    type Id = ItemId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "fence_items";
}

#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct Owner
{
    id: OwnerId,
    item_id: ItemId,
    label: String,
}

impl Table for Owner
{
    type Col = OwnerCol;
    type Id = OwnerId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "fence_owners";
}

/// A view over `fence_items`: the builder infers only the view's name.
#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct ItemName
{
    id: ItemId,
    name: String,
}

impl Table for ItemName
{
    type Col = ItemNameCol;
    type Id = ItemId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "fence_item_names";
}

/// A table that does not exist, for a real Postgres error.
#[derive(Debug, Clone, PartialEq, FromRow, SqlCols, Serialize, Deserialize)]
struct Missing
{
    id: ItemId,
}

impl Table for Missing
{
    type Col = MissingCol;
    type Id = ItemId;

    const PRIMARY_KEY: &'static str = "id";
    const TABLE_NAME: &'static str = "fence_missing";
}

struct ViewBase;

impl CacheTag for ViewBase
{
    fn tables(&self) -> &[&'static str]
    {
        &["fence_items"]
    }
}

const GENERATIONS: &str = "sq:g:v1";

struct Env
{
    pool: PgPool,
    redis: MultiplexedConnection,
    client: redis::Client,
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
        "DROP VIEW IF EXISTS fence_item_names;
         DROP TABLE IF EXISTS fence_owners, fence_items CASCADE;
         CREATE TABLE fence_items (
             id uuid PRIMARY KEY, name text NOT NULL,
             amount numeric NOT NULL DEFAULT 0, big bigint NOT NULL DEFAULT 0);
         CREATE TABLE fence_owners (
             id uuid PRIMARY KEY, item_id uuid NOT NULL REFERENCES fence_items,
             label text NOT NULL);
         CREATE VIEW fence_item_names AS SELECT id, name FROM fence_items;",
    )
    .execute(&pool)
    .await
    .expect("create tables");
    Env { pool, redis, client }
}

/// Inserts without invalidating: fixture setup, not a write under test.
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

async fn insert_owner(e: &Env, item: ItemId, label: &str) -> OwnerId
{
    let id = OwnerId::new();
    SqlQ::insert::<Owner>()
        .values([OwnerCol::Id.eq(id), OwnerCol::ItemId.eq(item), OwnerCol::Label.eq(label)])
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

/// Inserts through the library's invalidating write path.
async fn insert_item_invalidating(e: &mut Env, name: &str)
{
    SqlQ::insert::<Item>()
        .values([ItemCol::Id.eq(ItemId::new()), ItemCol::Name.eq(name)])
        .unwrap()
        .build()
        .unwrap()
        .bind()
        .execute(&e.pool, &mut e.redis)
        .await
        .unwrap();
}

/// Changes Postgres without telling the cache, to tell a hit from a miss.
async fn set_name_uncached(e: &Env, id: ItemId, name: &str)
{
    SqlQ::update::<Item>()
        .set([ItemCol::Name.eq(name)])
        .filter([ItemCol::Id.eq(id)])
        .build()
        .unwrap()
        .bind()
        .skip_inval()
        .execute(&e.pool)
        .await
        .unwrap();
}

fn by_id(id: ItemId) -> sql_query::BoundQueryAs<Item>
{
    SqlQ::select::<Item>().filter([ItemCol::Id.eq(id)]).build().unwrap().bind_as::<Item>()
}

fn key_of(id: ItemId) -> String
{
    by_id(id).cached(1).key()
}

async fn cached_name(e: &mut Env, id: ItemId) -> Result<String, CacheError>
{
    Ok(by_id(id).cached(3600).fetch_one(&e.pool, &mut e.redis).await?.name)
}

async fn fresh_name(e: &Env, id: ItemId) -> String
{
    by_id(id).fetch_one(&e.pool).await.unwrap().name
}

/// Spawns a cached read of `id` so the test can park and release it.
fn spawn_read(e: &Env, id: ItemId) -> tokio::task::JoinHandle<Result<Item, CacheError>>
{
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    tokio::spawn(async move { by_id(id).cached(3600).fetch_one(&pool, &mut redis).await })
}

fn binds_id(binds: &[SqlParam], id: impl Into<SqlParam>) -> bool
{
    binds.contains(&id.into())
}

async fn exists(e: &mut Env, key: &str) -> bool
{
    redis::cmd("EXISTS").arg(key).query_async::<i64>(&mut e.redis).await.unwrap() == 1
}

async fn generation(e: &mut Env, table: &str) -> Option<String>
{
    redis::cmd("HGET").arg(GENERATIONS).arg(table).query_async(&mut e.redis).await.unwrap()
}

async fn raw(e: &mut Env, args: &[&str])
{
    let mut cmd = redis::cmd(args[0]);
    for a in &args[1..]
    {
        cmd.arg(*a);
    }
    cmd.query_async::<redis::Value>(&mut e.redis).await.unwrap();
}

fn assert_fenced<T: std::fmt::Debug>(r: &Result<T, CacheError>)
{
    assert!(matches!(r, Err(CacheError::GenerationChanged)), "expected a fenced fill, got {r:?}");
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
    let read = spawn_read(&e, id);
    pause.reached().await;
    rename(&mut e, id, "new").await;
    pause.release();
    let first = read.await.expect("read task");

    assert_fenced(&first);
    assert!(!exists(&mut e, &key_of(id)).await, "a fenced fill wrote its entry");
    let cached = cached_name(&mut e, id).await.unwrap();
    let fresh = fresh_name(&e, id).await;
    assert_eq!(cached, fresh, "the cached read serves {cached:?} while Postgres holds {fresh:?}");
    assert_eq!(fresh, "new");
}

/// A hit whose lookup finished before a sweep may still deliver the old row;
/// a lookup that starts after the sweep completed never does.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_hit_after_a_completed_sweep_is_never_old()
{
    let mut e = env().await;
    let id = insert_item(&e, "a").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "a");
    set_name_uncached(&e, id, "b").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "a", "second read was not a hit");

    let mut pause = pause_at(PausePoint::AfterLookup, move |s| s.hit && binds_id(s.binds, id));
    let read = spawn_read(&e, id);
    pause.reached().await;
    rename(&mut e, id, "c").await;
    pause.release();
    assert_eq!(read.await.unwrap().unwrap().name, "a", "in-flight hit predates the sweep");

    assert_eq!(cached_name(&mut e, id).await.unwrap(), "c");
    assert!(exists(&mut e, &key_of(id)).await, "the post-sweep miss refilled");
}

/// Two misses snapshot two generations; two invalidations follow. Neither
/// fill may land, newer first or older last: tokens never repeat.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn older_fills_after_repeated_invalidations_are_refused()
{
    let mut e = env().await;
    let id = insert_item(&e, "v0").await;

    let mut first = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let read_a = spawn_read(&e, id);
    first.reached().await;
    let g0 = generation(&mut e, "fence_items").await.unwrap();
    rename(&mut e, id, "v1").await;

    let mut second = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let read_b = spawn_read(&e, id);
    second.reached().await;
    let g1 = generation(&mut e, "fence_items").await.unwrap();
    rename(&mut e, id, "v2").await;
    let g2 = generation(&mut e, "fence_items").await.unwrap();
    assert!(g0 != g1 && g1 != g2 && g0 != g2, "generations repeated: {g0} {g1} {g2}");

    second.release();
    assert_fenced(&read_b.await.unwrap());
    first.release();
    assert_fenced(&read_a.await.unwrap());
    assert!(!exists(&mut e, &key_of(id)).await);
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "v2");
}

/// A join depends on both tables; invalidating either one fences a fill and
/// turns a later lookup into a miss.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn every_joined_table_fences_and_invalidates()
{
    let mut e = env().await;
    let item = insert_item(&e, "item").await;
    let owner = insert_owner(&e, item, "owner").await;
    let joined = move || {
        SqlQ::select::<Owner>()
            .from([
                Expr::<Owner>::from(OwnerCol::Label),
                Expr::new().column_of::<Item>(ItemCol::Name),
            ])
            .join::<Item, Owner>(SqlJoin::Inner, ItemCol::Id, OwnerCol::ItemId)
            .filter([OwnerCol::Id.eq(owner)])
            .build()
            .unwrap()
            .bind_as::<(String, String)>()
            .cached(3600)
    };

    // Fenced by the joined table, not the FROM table.
    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, owner));
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    let read = tokio::spawn(async move { joined().fetch_one(&pool, &mut redis).await });
    pause.reached().await;
    rename(&mut e, item, "item2").await;
    pause.release();
    assert_fenced(&read.await.unwrap());

    let row = joined().fetch_one(&e.pool, &mut e.redis).await.unwrap();
    assert_eq!(row, ("owner".into(), "item2".into()));

    // Owner-side invalidation alone makes the next lookup a miss.
    sqlx::query("UPDATE fence_owners SET label = 'owner2'").execute(&e.pool).await.unwrap();
    assert_eq!(joined().fetch_one(&e.pool, &mut e.redis).await.unwrap().0, "owner", "not a hit");
    invalidate_tables(&["fence_owners"], &mut e.redis).await.unwrap();
    assert_eq!(joined().fetch_one(&e.pool, &mut e.redis).await.unwrap().0, "owner2");

    // Item-side invalidation alone does too.
    set_name_uncached(&e, item, "item3").await;
    assert_eq!(joined().fetch_one(&e.pool, &mut e.redis).await.unwrap().1, "item2", "not a hit");
    invalidate_tables(&["fence_items"], &mut e.redis).await.unwrap();
    assert_eq!(joined().fetch_one(&e.pool, &mut e.redis).await.unwrap().1, "item3");
}

/// A view read infers only the view; its tag adds the base table. The union is
/// the dependency set, and the same SQL under a different set never hits.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn tags_join_the_dependency_set_of_a_view()
{
    let mut e = env().await;
    let id = insert_item(&e, "base").await;
    let view = move || {
        SqlQ::select::<ItemName>()
            .filter([ItemNameCol::Id.eq(id)])
            .build()
            .unwrap()
            .bind_as::<ItemName>()
            .cached(3600)
    };

    // Untagged, the entry depends on the view alone.
    assert_eq!(view().fetch_one(&e.pool, &mut e.redis).await.unwrap().name, "base");
    set_name_uncached(&e, id, "renamed").await;
    let untagged = view().fetch_one(&e.pool, &mut e.redis).await.unwrap();
    assert_eq!(untagged.name, "base", "untagged read was not a hit");
    // Same key, wider dependency set: a miss, not the narrower entry.
    let tagged = view().tag(&ViewBase).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    assert_eq!(tagged.name, "renamed");

    // Tagged, a fill is fenced by the base table. The entry is dropped first
    // only so the read misses: a hit has no fill to fence.
    raw(&mut e, &["DEL", &view().key()]).await;
    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    let read =
        tokio::spawn(async move { view().tag(&ViewBase).fetch_one(&pool, &mut redis).await });
    pause.reached().await;
    rename(&mut e, id, "final").await;
    pause.release();
    assert_fenced(&read.await.unwrap());
    let after = view().tag(&ViewBase).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    assert_eq!(after.name, "final");
}

/// A CTE's tables are dependencies of the statement that uses it.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_cte_table_is_a_dependency()
{
    let mut e = env().await;
    let item = insert_item(&e, "cte").await;
    insert_owner(&e, item, "picked").await;
    let q = move || {
        SqlQ::with([(
            "picked",
            SqlQ::select::<Owner>().from([OwnerCol::ItemId]).filter([OwnerCol::Label.eq("picked")]),
        )])
        .select::<Item>()
        .from([ItemCol::Name])
        .filter([Expr::<Item>::new().raw("\"fence_items\".id IN (SELECT item_id FROM picked)")])
        .build()
        .unwrap()
        .bind_scalar::<String>()
        .cached(3600)
    };

    assert_eq!(q().fetch_all(&e.pool, &mut e.redis).await.unwrap(), ["cte"]);
    invalidate_tables(&["fence_items"], &mut e.redis).await.unwrap();
    let mut pause = pause_at(PausePoint::BeforeFill, |s| s.sql.contains("picked"));
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    let read = tokio::spawn(async move { q().fetch_all(&pool, &mut redis).await });
    pause.reached().await;
    sqlx::query("UPDATE fence_owners SET label = 'dropped'").execute(&e.pool).await.unwrap();
    invalidate_tables(&["fence_owners"], &mut e.redis).await.unwrap();
    pause.release();
    assert_fenced(&read.await.unwrap());
    assert!(q().fetch_all(&e.pool, &mut e.redis).await.unwrap().is_empty());
}

type Boxed<T> = Pin<Box<dyn Future<Output = Result<T, CacheError>> + Send>>;

/// Runs one cached terminal across an invalidating write: the fill is fenced,
/// the next read equals Postgres, and the read after that is a hit.
async fn fence_terminal<T>(
    e: &mut Env,
    marker: SqlParam,
    read: impl Fn(PgPool, MultiplexedConnection) -> Boxed<T>,
    write: impl AsyncFnOnce(&mut Env),
    expect: T,
) where
    T: std::fmt::Debug + PartialEq + Send + 'static,
{
    let m = marker.clone();
    let mut pause = pause_at(PausePoint::BeforeFill, move |s| s.binds.contains(&m));
    let pending = tokio::spawn(read(e.pool.clone(), e.redis.clone()));
    pause.reached().await;
    write(e).await;
    pause.release();
    assert_fenced(&pending.await.unwrap());
    assert_eq!(read(e.pool.clone(), e.redis.clone()).await.unwrap(), expect, "{marker:?}");
    assert_eq!(read(e.pool.clone(), e.redis.clone()).await.unwrap(), expect, "{marker:?} hit");
}

fn by_name(name: &'static str) -> sql_query::BoundQueryAs<Item>
{
    SqlQ::select::<Item>().filter([ItemCol::Name.eq(name)]).build().unwrap().bind_as::<Item>()
}

/// All six cached terminals share the fence, including an empty list and an
/// absent row gaining a row, and a present row going absent.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
#[allow(clippy::too_many_lines)]
async fn all_six_cached_terminals_are_fenced()
{
    let mut e = env().await;

    // CachedBoundQueryAs::fetch_all: empty list -> one row.
    fence_terminal(
        &mut e,
        "all".into(),
        |p, mut r| {
            Box::pin(
                async move { Ok(by_name("all").cached(60).fetch_all(&p, &mut r).await?.len()) },
            )
        },
        async |e: &mut Env| insert_item_invalidating(e, "all").await,
        1usize,
    )
    .await;

    // CachedBoundQueryAs::fetch_one: a renamed row.
    let id = insert_item(&e, "one").await;
    fence_terminal(
        &mut e,
        id.into(),
        move |p, mut r| {
            Box::pin(async move { Ok(by_id(id).cached(60).fetch_one(&p, &mut r).await?.name) })
        },
        async move |e: &mut Env| rename(e, id, "one2").await,
        "one2".to_string(),
    )
    .await;

    // CachedBoundQueryAs::fetch_optional: None -> a row.
    fence_terminal(
        &mut e,
        "opt".into(),
        |p, mut r| {
            Box::pin(async move {
                Ok(by_name("opt").cached(60).fetch_optional(&p, &mut r).await?.map(|i| i.name))
            })
        },
        async |e: &mut Env| insert_item_invalidating(e, "opt").await,
        Some("opt".to_string()),
    )
    .await;

    // CachedBoundQueryScalar::fetch_one: a count.
    let count = |name: &'static str| {
        SqlQ::select::<Item>()
            .from([ItemCol::Id.count()])
            .filter([ItemCol::Name.eq(name)])
            .build()
            .unwrap()
            .bind_scalar::<i64>()
    };
    fence_terminal(
        &mut e,
        "count".into(),
        move |p, mut r| {
            Box::pin(async move { count("count").cached(60).fetch_one(&p, &mut r).await })
        },
        async |e: &mut Env| insert_item_invalidating(e, "count").await,
        1i64,
    )
    .await;

    // CachedBoundQueryScalar::fetch_optional: a row -> None, by a delete.
    let gone = insert_item(&e, "gone").await;
    let name_of = move || {
        SqlQ::select::<Item>()
            .from([ItemCol::Name])
            .filter([ItemCol::Id.eq(gone)])
            .build()
            .unwrap()
            .bind_scalar::<String>()
    };
    fence_terminal(
        &mut e,
        gone.into(),
        move |p, mut r| {
            Box::pin(async move { name_of().cached(60).fetch_optional(&p, &mut r).await })
        },
        async move |e: &mut Env| {
            SqlQ::delete::<Item>()
                .filter([ItemCol::Id.eq(gone)])
                .build()
                .unwrap()
                .bind()
                .execute(&e.pool, &mut e.redis)
                .await
                .unwrap();
        },
        None,
    )
    .await;

    // CachedBoundQueryScalar::fetch_all: matching names.
    let names = || {
        SqlQ::select::<Item>()
            .from([ItemCol::Name])
            .filter([ItemCol::Name.like("scalar-%")])
            .build()
            .unwrap()
            .bind_scalar::<String>()
    };
    fence_terminal(
        &mut e,
        "scalar-%".into(),
        move |p, mut r| Box::pin(async move { names().cached(60).fetch_all(&p, &mut r).await }),
        async |e: &mut Env| insert_item_invalidating(e, "scalar-1").await,
        vec!["scalar-1".to_string()],
    )
    .await;
}

/// Lost generations, lost reverse indexes and legacy values all degrade to
/// misses; none validates an old entry. Invalidation advances a generation
/// whether or not anything is cached under it.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn lost_metadata_never_validates_an_old_entry()
{
    let mut e = env().await;
    let id = insert_item(&e, "g0").await;
    let key = key_of(id);

    // One generation field evicted: the surviving entry is a miss.
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "g0");
    let before = generation(&mut e, "fence_items").await.unwrap();
    raw(&mut e, &["HDEL", GENERATIONS, "fence_items"]).await;
    set_name_uncached(&e, id, "g1").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "g1", "entry survived a lost generation");
    let after = generation(&mut e, "fence_items").await.unwrap();
    assert_ne!(before, after, "a lost generation came back as its old token");

    // The whole generation hash evicted.
    raw(&mut e, &["DEL", GENERATIONS]).await;
    set_name_uncached(&e, id, "g2").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "g2", "entry survived a lost hash");

    // Reverse index lost: the sweep cannot reach the entry; the generation can.
    raw(&mut e, &["DEL", "sq:t:fence_items"]).await;
    rename(&mut e, id, "g3").await;
    assert!(exists(&mut e, &key).await, "the sweep found an entry it had no index for");
    let name = cached_name(&mut e, id).await.unwrap();
    assert_eq!(name, "g3", "unswept entry served after invalidation");

    // A legacy, unversioned value under the same key: a miss, then replaced.
    let legacy =
        serde_json::json!({"id": id, "name": "legacy", "amount": "0", "big": 0}).to_string();
    raw(&mut e, &["SET", &key, &legacy]).await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "g3", "legacy value accepted");
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "g3");

    // Nothing cached and no generation state: invalidation still advances.
    raw(&mut e, &["FLUSHDB"]).await;
    invalidate_tables(&["fence_items"], &mut e.redis).await.unwrap();
    let g = generation(&mut e, "fence_items").await.expect("invalidation created a generation");
    invalidate_tables(&["fence_items"], &mut e.redis).await.unwrap();
    assert_ne!(generation(&mut e, "fence_items").await.unwrap(), g);
}

/// A flush between a read's snapshot and its fill fences the fill — alone,
/// and when another read re-created generation state in between.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_flush_between_lookup_and_fill_fences_the_fill()
{
    let mut e = env().await;
    let id = insert_item(&e, "pre").await;

    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let read = spawn_read(&e, id);
    pause.reached().await;
    raw(&mut e, &["FLUSHDB"]).await;
    set_name_uncached(&e, id, "post").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "post");
    pause.release();
    assert_fenced(&read.await.unwrap());
    // The interleaved read's entry is the one that stays.
    set_name_uncached(&e, id, "later").await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "post");

    raw(&mut e, &["DEL", &key_of(id)]).await;
    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let read = spawn_read(&e, id);
    pause.reached().await;
    raw(&mut e, &["FLUSHDB"]).await;
    pause.release();
    assert_fenced(&read.await.unwrap());
    assert!(!exists(&mut e, &key_of(id)).await);
}

/// The TTL is the caller's, set by the fill and never refreshed by a hit.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn ttl_is_set_by_the_fill_and_not_refreshed_by_hits()
{
    let mut e = env().await;
    let id = insert_item(&e, "ttl").await;
    let key = key_of(id);
    by_id(id).cached(100).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    let filled: i64 = redis::cmd("PTTL").arg(&key).query_async(&mut e.redis).await.unwrap();
    assert!((98_000..=100_000).contains(&filled), "{filled}");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    by_id(id).cached(100).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    let after_hit: i64 = redis::cmd("PTTL").arg(&key).query_async(&mut e.redis).await.unwrap();
    assert!(after_hit < filled, "a hit refreshed the TTL: {filled} -> {after_hit}");

    // A zero TTL caches nothing and touches no Redis state.
    let other = insert_item(&e, "ttl0").await;
    raw(&mut e, &["FLUSHDB"]).await;
    by_id(other).cached(0).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut e.redis).await.unwrap();
    assert!(keys.is_empty(), "{keys:?}");
}

/// Redis failures and Postgres failures each surface as their own variant,
/// and a script that fails writes nothing.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn failures_are_typed_and_write_nothing()
{
    let mut e = env().await;
    let id = insert_item(&e, "err").await;
    let key = key_of(id);
    let wrongtype = |r: &Result<Item, CacheError>| matches!(r, Err(CacheError::Redis(err)) if err.to_string().contains("WRONGTYPE"));

    // Wrong-typed generation state: the lookup fails; nothing is written.
    raw(&mut e, &["SET", GENERATIONS, "not-a-hash"]).await;
    let r = by_id(id).cached(60).fetch_one(&e.pool, &mut e.redis).await;
    assert!(wrongtype(&r), "{r:?}");
    assert!(!exists(&mut e, &key).await);
    // Invalidation checks every type before it advances or sweeps anything.
    raw(&mut e, &["SADD", "sq:t:fence_items", "sq:e:sentinel"]).await;
    raw(&mut e, &["SET", "sq:e:sentinel", "x"]).await;
    let inv = invalidate_tables(&["fence_items"], &mut e.redis).await;
    assert!(matches!(&inv, Err(sqlx::Error::Protocol(m)) if m.contains("WRONGTYPE")), "{inv:?}");
    assert!(exists(&mut e, "sq:e:sentinel").await, "a failed invalidation swept");
    raw(&mut e, &["FLUSHDB"]).await;

    // Wrong-typed reverse index at fill time: the fill fails before writing.
    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let read = spawn_read(&e, id);
    pause.reached().await;
    raw(&mut e, &["SET", "sq:t:fence_items", "not-a-set"]).await;
    pause.release();
    let r = read.await.unwrap();
    assert!(wrongtype(&r), "{r:?}");
    assert!(!exists(&mut e, &key).await, "a failed fill wrote its entry");
    raw(&mut e, &["FLUSHDB"]).await;

    // Scripts dropped from the server are reloaded transparently.
    raw(&mut e, &["SCRIPT", "FLUSH"]).await;
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "err");

    // A dead connection is a Redis error, never a database one.
    let mut dead = e.client.get_multiplexed_async_connection().await.unwrap();
    let cid: i64 = redis::cmd("CLIENT").arg("ID").query_async(&mut dead).await.unwrap();
    raw(&mut e, &["CLIENT", "KILL", "ID", &cid.to_string()]).await;
    let r = by_id(id).cached(60).fetch_one(&e.pool, &mut dead).await;
    assert!(matches!(r, Err(CacheError::Redis(_))), "{r:?}");

    // A Postgres error keeps its SQLSTATE.
    let r = SqlQ::select::<Missing>()
        .build()
        .unwrap()
        .bind_as::<Missing>()
        .cached(60)
        .fetch_all(&e.pool, &mut e.redis)
        .await;
    let code = match &r
    {
        Err(CacheError::Database(err)) =>
        {
            err.as_database_error().and_then(sqlx::error::DatabaseError::code)
        }
        _ => None,
    };
    assert_eq!(code.as_deref(), Some("42P01"), "{r:?}");
}

/// A read dropped while parked writes nothing; the next read fills normally.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_cancelled_read_leaves_nothing_behind()
{
    let mut e = env().await;
    let id = insert_item(&e, "cancel").await;
    let key = key_of(id);

    for point in [PausePoint::AfterLookup, PausePoint::BeforeFill]
    {
        let mut pause = pause_at(point, move |s| binds_id(s.binds, id));
        let read = spawn_read(&e, id);
        pause.reached().await;
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
        drop(pause);
        assert!(!exists(&mut e, &key).await, "{point:?}: a cancelled read filled");
    }
    assert_eq!(cached_name(&mut e, id).await.unwrap(), "cancel");
    assert!(exists(&mut e, &key).await);
}

/// The payload is serde's bytes, untouched by Lua: an exact decimal and an
/// integer past 2^53 survive the round trip through Redis.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn payloads_are_stored_byte_for_byte()
{
    let mut e = env().await;
    let id = ItemId::new();
    let amount = Decimal::from_str_exact("12345678901234567890.123456789").unwrap();
    let name = "sqc1:1;\"quoted\"\n\u{1F4B1} 9007199254740993";
    SqlQ::insert::<Item>()
        .values([
            ItemCol::Id.eq(id),
            ItemCol::Name.eq(name),
            ItemCol::Amount.eq(amount),
            ItemCol::Big.eq(i64::MAX),
        ])
        .unwrap()
        .build()
        .unwrap()
        .bind()
        .skip_inval()
        .execute(&e.pool)
        .await
        .unwrap();

    let fresh = by_id(id).fetch_one(&e.pool).await.unwrap();
    let miss = by_id(id).cached(60).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    let hit = by_id(id).cached(60).fetch_one(&e.pool, &mut e.redis).await.unwrap();
    assert_eq!(miss, fresh);
    assert_eq!(hit, fresh);
    assert_eq!(hit.amount.to_string(), "12345678901234567890.123456789");
    assert_eq!(hit.big, i64::MAX);

    let stored: Vec<u8> =
        redis::cmd("GET").arg(key_of(id)).query_async(&mut e.redis).await.unwrap();
    let payload = serde_json::to_vec(&fresh).unwrap();
    assert!(stored.starts_with(b"sqc1:"), "unversioned entry");
    assert!(stored.ends_with(&payload), "payload bytes changed in Redis");
}

/// `Clone` on the original query lets a caller re-run it uncached after a
/// fenced fill, without populating Redis.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn a_cloned_query_reruns_uncached_after_a_fence()
{
    let mut e = env().await;
    let id = insert_item(&e, "before").await;
    let original = SqlQ::select::<Item>().filter([ItemCol::Id.eq(id)]).build().unwrap();

    let mut pause = pause_at(PausePoint::BeforeFill, move |s| binds_id(s.binds, id));
    let (pool, mut redis) = (e.pool.clone(), e.redis.clone());
    let attempt = original.clone();
    let read = tokio::spawn(async move {
        attempt.bind_as::<Item>().cached(3600).fetch_one(&pool, &mut redis).await
    });
    pause.reached().await;
    rename(&mut e, id, "after").await;
    pause.release();
    assert_fenced(&read.await.unwrap());

    let refreshed = original.bind_as::<Item>().fetch_one(&e.pool).await.unwrap();
    assert_eq!(refreshed.name, "after");
    assert!(!exists(&mut e, &key_of(id)).await, "the uncached re-read populated Redis");
}

/// Unpaused readers racing a writer: after every round, a cached read equals
/// Postgres. A sample, not a proof; the paused tests are the proof.
#[tokio::test]
#[ignore = "needs isolated Postgres and Redis; run scripts/cache-tests.sh"]
async fn racing_readers_never_outlive_a_completed_write()
{
    let mut e = env().await;
    let id = insert_item(&e, "r0").await;
    let mut fenced = 0usize;
    for round in 1..=300
    {
        let readers: Vec<_> = (0..4).map(|_| spawn_read(&e, id)).collect();
        rename(&mut e, id, &format!("r{round}")).await;
        for r in readers
        {
            match r.await.unwrap()
            {
                Ok(_) =>
                {}
                Err(CacheError::GenerationChanged) => fenced += 1,
                Err(err) => panic!("round {round}: {err}"),
            }
        }
        let cached = cached_name(&mut e, id).await.unwrap();
        assert_eq!(cached, format!("r{round}"), "round {round}: stale after a completed write");
    }
    println!("fenced fills: {fenced} of 1200 racing reads");
}
