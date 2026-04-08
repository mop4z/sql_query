use std::{fmt::Write, marker::PhantomData};

use sqlx::{
    FromRow, Postgres, QueryBuilder,
    postgres::{PgPool, PgQueryResult, PgRow},
};

use crate::shared::{error::SqlQueryError, value::SqlParam};

pub struct UnbindedQuery<'q> {
    pub(crate) qb: QueryBuilder<'q, Postgres>,
    pub(crate) binds: Vec<SqlParam>,
}

pub struct BoundQuery {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
}

pub struct BoundQueryAs<T> {
    pub(crate) sql: String,
    pub(crate) binds: Vec<SqlParam>,
    _t: PhantomData<T>,
}

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
    pub fn into_raw(self) -> (String, Vec<SqlParam>) {
        (self.qb.into_sql(), self.binds)
    }

    pub fn build(self) -> BoundQuery {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQuery { sql, binds: self.binds }
    }

    pub fn build_as<T>(self) -> BoundQueryAs<T> {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQueryAs { sql, binds: self.binds, _t: PhantomData }
    }

    pub fn build_scalar<T>(self) -> BoundQueryScalar<T> {
        let sql = renumber_placeholders(&self.qb.into_sql());
        BoundQueryScalar { sql, binds: self.binds, _t: PhantomData }
    }
}

impl BoundQuery {
    pub async fn execute(self, pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
        let mut q = sqlx::query(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.execute(pool).await
    }
}

impl<T: for<'r> FromRow<'r, PgRow> + Send + Unpin> BoundQueryAs<T> {
    pub async fn fetch_all(self, pool: &PgPool) -> Result<Vec<T>, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_all(pool).await
    }

    pub async fn fetch_one(self, pool: &PgPool) -> Result<T, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(pool).await
    }

    pub async fn fetch_optional(self, pool: &PgPool) -> Result<Option<T>, sqlx::Error> {
        let mut q = sqlx::query_as::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(pool).await
    }
}

impl<T> BoundQueryScalar<T>
where
    (T,): for<'r> FromRow<'r, PgRow>,
    T: Send + Unpin,
{
    pub async fn fetch_one(self, pool: &PgPool) -> Result<T, sqlx::Error> {
        let mut q = sqlx::query_scalar::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_one(pool).await
    }

    pub async fn fetch_optional(self, pool: &PgPool) -> Result<Option<T>, sqlx::Error> {
        let mut q = sqlx::query_scalar::<_, T>(&self.sql);
        for b in self.binds {
            q = q.bind(b);
        }
        q.fetch_optional(pool).await
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
