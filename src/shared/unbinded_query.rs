use std::{fmt::Write, marker::PhantomData};

use sqlx::{
    Executor, FromRow, Postgres, QueryBuilder,
    postgres::{PgPool, PgQueryResult, PgRow},
};

use crate::shared::{error::SqlQueryError, value::SqlParam};

/// A query whose placeholders have not yet been renumbered or bound.
pub struct UnbindedQuery<'q> {
    pub(crate) qb: QueryBuilder<'q, Postgres>,
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

fn renumber_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut idx = 1usize;
    let mut rest = sql;
    while let Some(pos) = rest.find("$#") {
        out.push_str(&rest[..pos]);
        let _ = write!(out, "${idx}");
        idx += 1;
        rest = &rest[pos + 2..];
    }
    out.push_str(rest);
    out
}

pub(crate) fn push_conditions(
    keyword: &str,
    conditions: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    qb: &mut QueryBuilder<'_, Postgres>,
    binds: &mut Vec<SqlParam>,
) -> Result<(), sqlx::Error> {
    if conditions.is_empty() {
        return Ok(());
    }
    qb.push(" ");
    qb.push(keyword);
    qb.push(" 1=1");
    for result in conditions {
        let (filter, params) = result.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        binds.extend(params);
        qb.push(" AND ");
        qb.push(&filter);
    }
    Ok(())
}

impl<'q> UnbindedQuery<'q> {
    /// Consumes the query and returns the raw SQL string and bind values.
    pub fn into_raw(self) -> (String, Vec<SqlParam>) {
        (self.qb.into_sql(), self.binds)
    }

    /// Renumbers placeholders and produces a `BoundQuery` for execution.
    pub fn build(self) -> BoundQuery {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQuery { sql, binds: self.binds }
    }

    /// Renumbers placeholders and produces a `BoundQueryAs<T>` for typed row fetching.
    pub fn build_as<T>(self) -> BoundQueryAs<T> {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQueryAs { sql, binds: self.binds, _t: PhantomData }
    }

    /// Renumbers placeholders and produces a `BoundQueryScalar<T>` for single-column fetching.
    pub fn build_scalar<T>(self) -> BoundQueryScalar<T> {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQueryScalar { sql, binds: self.binds, _t: PhantomData }
    }
}

impl BoundQuery {
    /// Binds all parameters and executes the query, returning the raw result.
    pub async fn execute<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.execute(executor).await
    }

    /// Fetches all matching rows as raw `PgRow`s.
    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_all(executor).await
    }

    /// Fetches exactly one row as a raw `PgRow`.
    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgRow, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(executor).await
    }

    /// Fetches at most one row as a raw `PgRow`, returning `None` if no rows match.
    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<PgRow>, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(executor).await
    }
}

impl<T: for<'r> FromRow<'r, PgRow> + Send + Unpin> BoundQueryAs<T> {
    /// Fetches all matching rows, deserializing each into `T`.
    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<T>, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_all(executor).await
    }

    /// Fetches exactly one row, returning an error if zero or more than one row is found.
    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<T, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(executor).await
    }

    /// Fetches at most one row, returning `None` if no rows match.
    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<T>, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(executor).await
    }

    /// Binds all parameters and executes the query, returning the raw result.
    pub async fn execute<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.execute(executor).await
    }

    /// Runs a COUNT(*) query and the SELECT query, returning (items, total_count).
    pub async fn fetch_paginated(
        self,
        pool: &PgPool,
    ) -> Result<(Vec<T>, i64), sqlx::Error> {
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS _sq", self.sql);
        let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
        for b in &self.binds {
            count_q = count_q.bind(b.clone());
        }
        let total: i64 = count_q.fetch_one(&*pool).await?;

        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        let items = q.fetch_all(&*pool).await?;

        Ok((items, total))
    }
}

impl<T> BoundQueryScalar<T>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    /// Fetches exactly one scalar value, returning an error if no rows match.
    pub async fn fetch_one<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<T, sqlx::Error> {
        let mut q = sqlx::query_scalar::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(executor).await
    }

    /// Fetches at most one scalar value, returning `None` if no rows match.
    pub async fn fetch_optional<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Option<T>, sqlx::Error> {
        let mut q = sqlx::query_scalar::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(executor).await
    }

    /// Fetches all matching scalar values.
    pub async fn fetch_all<'e>(
        self,
        executor: impl Executor<'e, Database = Postgres>,
    ) -> Result<Vec<T>, sqlx::Error> {
        let mut q = sqlx::query_scalar::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_all(executor).await
    }
}

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
