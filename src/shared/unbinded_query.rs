use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{
    Executor, FromRow, Postgres,
    postgres::{PgPool, PgQueryResult, PgRow},
};

use crate::shared::{cached, error::SqlQueryError, value::SqlParam};

/// Emits a tracing event for a query about to hit Postgres. SQL and bind
/// count go to debug; full bind values go to trace.
#[inline]
fn trace_sql(sql: &str, binds: &[SqlParam]) {
    tracing::debug!(target: "sql_query", sql = %sql, binds = binds.len(), "executing query");
    tracing::trace!(target: "sql_query", binds = ?binds, "bind values");
}

/// A query whose placeholders have not yet been renumbered or bound.
pub struct UnbindedQuery {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
}

/// A finalized query with renumbered placeholders, ready to execute without row mapping.
pub struct BoundQuery {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
}

/// A finalized query that deserializes each row into `T` via `FromRow`.
pub struct BoundQueryAs<T> {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
    _t: PhantomData<T>,
}

/// A finalized query that returns a single scalar column of type `T`.
pub struct BoundQueryScalar<T> {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
    _t: PhantomData<T>,
}

/// A `BoundQueryAs<T>` with Redis caching enabled.
pub struct CachedBoundQueryAs<T> {
    sql: String,
    binds: Vec<SqlParam>,
    ttl: u64,
    _t: PhantomData<T>,
}

/// A `BoundQueryScalar<T>` with Redis caching enabled.
pub struct CachedBoundQueryScalar<T> {
    sql: String,
    binds: Vec<SqlParam>,
    ttl: u64,
    _t: PhantomData<T>,
}

// ---------------------------------------------------------------------------
// Shared helpers — bind params and run query
// ---------------------------------------------------------------------------

async fn run_query_as<'e, T: for<'r> FromRow<'r, PgRow> + Send + Unpin>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<Vec<T>, sqlx::Error> {
    trace_sql(sql, &binds);
    let mut q = sqlx::query_as::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_all(executor).await
}

async fn run_query_as_one<'e, T: for<'r> FromRow<'r, PgRow> + Send + Unpin>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<T, sqlx::Error> {
    trace_sql(sql, &binds);
    let mut q = sqlx::query_as::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_one(executor).await
}

async fn run_query_as_optional<'e, T: for<'r> FromRow<'r, PgRow> + Send + Unpin>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<Option<T>, sqlx::Error> {
    trace_sql(sql, &binds);
    let mut q = sqlx::query_as::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_optional(executor).await
}

async fn run_scalar<'e, T>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<Vec<T>, sqlx::Error>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    trace_sql(sql, &binds);
    let mut q = sqlx::query_scalar::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_all(executor).await
}

async fn run_scalar_one<'e, T>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<T, sqlx::Error>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    trace_sql(sql, &binds);
    let mut q = sqlx::query_scalar::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_one(executor).await
}

async fn run_scalar_optional<'e, T>(
    sql: &str,
    binds: Vec<SqlParam>,
    executor: impl Executor<'e, Database = Postgres>,
) -> Result<Option<T>, sqlx::Error>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    trace_sql(sql, &binds);
    let mut q = sqlx::query_scalar::<_, T>(sql);
    for b in binds {
        q = q.bind(b);
    }
    q.fetch_optional(executor).await
}

// ---------------------------------------------------------------------------
// Placeholder renumbering
// ---------------------------------------------------------------------------

fn renumber_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut idx = 1usize;
    let mut rest = sql;
    while let Some(pos) = rest.find("$#") {
        out.push_str(&rest[..pos]);
        out.push('$');
        out.push_str(itoa::Buffer::new().format(idx));
        idx += 1;
        rest = &rest[pos + 2..];
    }
    out.push_str(rest);
    out
}

pub(crate) fn push_conditions(
    keyword: &str,
    conditions: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    sql: &mut String,
    binds: &mut Vec<SqlParam>,
) -> Result<(), sqlx::Error> {
    if conditions.is_empty() {
        return Ok(());
    }
    sql.push(' ');
    sql.push_str(keyword);
    sql.push_str(" 1=1");
    // Each filter is wrapped in parens so internal OR operators can't bind
    // tighter than the joining AND: `(a OR b) AND c`, not `a OR b AND c`.
    for result in conditions {
        let (filter, params) = result.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        binds.extend(params);
        sql.push_str(" AND (");
        sql.push_str(&filter);
        sql.push(')');
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// UnbindedQuery
// ---------------------------------------------------------------------------

impl UnbindedQuery {
    pub fn into_raw(self) -> (String, Vec<SqlParam>) {
        (self.sql, self.binds)
    }

    pub fn bind(self) -> BoundQuery {
        let sql = renumber_placeholders(&self.sql);
        BoundQuery { sql, binds: self.binds }
    }

    pub fn bind_as<T>(self) -> BoundQueryAs<T> {
        let sql = renumber_placeholders(&self.sql);
        BoundQueryAs { sql, binds: self.binds, _t: PhantomData }
    }

    pub fn bind_scalar<T>(self) -> BoundQueryScalar<T> {
        let sql = renumber_placeholders(&self.sql);
        BoundQueryScalar { sql, binds: self.binds, _t: PhantomData }
    }
}

// ---------------------------------------------------------------------------
// BoundQuery (no caching — raw rows aren't serializable)
// ---------------------------------------------------------------------------

impl BoundQuery {
    pub async fn execute<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        trace_sql(&self.sql, &self.binds);
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.execute(executor).await
    }

    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        trace_sql(&self.sql, &self.binds);
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_all(executor).await
    }

    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgRow, sqlx::Error> {
        trace_sql(&self.sql, &self.binds);
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(executor).await
    }

    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<PgRow>, sqlx::Error> {
        trace_sql(&self.sql, &self.binds);
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(executor).await
    }
}

// ---------------------------------------------------------------------------
// BoundQueryAs<T>
// ---------------------------------------------------------------------------

impl<T> BoundQueryAs<T> {
    /// Returns a `CachedBoundQueryAs` that checks Redis before hitting Postgres.
    pub fn cached(self, ttl_secs: u64) -> CachedBoundQueryAs<T> {
        CachedBoundQueryAs { sql: self.sql, binds: self.binds, ttl: ttl_secs, _t: PhantomData }
    }
}

impl<T: for<'r> FromRow<'r, PgRow> + Send + Unpin> BoundQueryAs<T> {
    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<T>, sqlx::Error> {
        run_query_as(&self.sql, self.binds, executor).await
    }

    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<T, sqlx::Error> {
        run_query_as_one(&self.sql, self.binds, executor).await
    }

    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<T>, sqlx::Error> {
        run_query_as_optional(&self.sql, self.binds, executor).await
    }

    pub async fn execute<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        trace_sql(&self.sql, &self.binds);
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.execute(executor).await
    }

    pub async fn fetch_paginated(self, pool: &PgPool) -> Result<(Vec<T>, i64), sqlx::Error> {
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS _sq", self.sql);
        trace_sql(&count_sql, &self.binds);
        let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
        for b in &self.binds {
            count_q = count_q.bind(b.clone());
        }
        let total: i64 = count_q.fetch_one(&*pool).await?;
        let items = run_query_as(&self.sql, self.binds, &*pool).await?;
        Ok((items, total))
    }
}

// ---------------------------------------------------------------------------
// CachedBoundQueryAs<T>
// ---------------------------------------------------------------------------

impl<T> CachedBoundQueryAs<T>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin + Serialize + DeserializeOwned,
{
    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<Vec<T>, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_query_as(&sql, binds, executor)).await
    }

    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<T, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_query_as_one(&sql, binds, executor)).await
    }

    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<Option<T>, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_query_as_optional(&sql, binds, executor))
            .await
    }
}

// ---------------------------------------------------------------------------
// BoundQueryScalar<T>
// ---------------------------------------------------------------------------

impl<T> BoundQueryScalar<T> {
    /// Returns a `CachedBoundQueryScalar` that checks Redis before hitting Postgres.
    pub fn cached(self, ttl_secs: u64) -> CachedBoundQueryScalar<T> {
        CachedBoundQueryScalar { sql: self.sql, binds: self.binds, ttl: ttl_secs, _t: PhantomData }
    }
}

impl<T> BoundQueryScalar<T>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<T, sqlx::Error> {
        run_scalar_one(&self.sql, self.binds, executor).await
    }

    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<T>, sqlx::Error> {
        run_scalar_optional(&self.sql, self.binds, executor).await
    }

    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<T>, sqlx::Error> {
        run_scalar(&self.sql, self.binds, executor).await
    }
}

// ---------------------------------------------------------------------------
// CachedBoundQueryScalar<T>
// ---------------------------------------------------------------------------

impl<T> CachedBoundQueryScalar<T>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin + Serialize + DeserializeOwned,
{
    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<T, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_scalar_one(&sql, binds, executor)).await
    }

    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<Option<T>, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_scalar_optional(&sql, binds, executor))
            .await
    }

    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<Vec<T>, sqlx::Error> {
        let key = cached::cache_key(&self.sql, &self.binds);
        let sql = self.sql;
        let binds = self.binds;
        cached::with_cache(&key, self.ttl, redis, || run_scalar(&sql, binds, executor)).await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renumber_single() {
        assert_eq!(
            renumber_placeholders("SELECT * FROM t WHERE x = $#"),
            "SELECT * FROM t WHERE x = $1",
        );
    }

    #[test]
    fn renumber_multiple() {
        assert_eq!(
            renumber_placeholders("SELECT * FROM t WHERE a = $# AND b = $# AND c = $#"),
            "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3",
        );
    }

    #[test]
    fn renumber_no_placeholders() {
        assert_eq!(renumber_placeholders("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn renumber_adjacent_to_text() {
        assert_eq!(renumber_placeholders("$#,$#"), "$1,$2");
    }

    #[test]
    fn renumber_does_not_match_dollar_one() {
        assert_eq!(renumber_placeholders("'costs $1' $#"), "'costs $1' $1");
    }

    #[test]
    fn renumber_in_limit_offset() {
        assert_eq!(
            renumber_placeholders("WHERE a = $# LIMIT $# OFFSET $#"),
            "WHERE a = $1 LIMIT $2 OFFSET $3",
        );
    }
}
