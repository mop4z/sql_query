//! Type-safe dynamic SQL query builder for `PostgreSQL`, built on sqlx.
//!
//! All queries start from [`SqlQ`]:
//! ```ignore
//! let users = SqlQ::select::<Users>()
//!     .filter([UsersCol::Name.eq("alice")])
//!     .build()?
//!     .build_as::<Users>()
//!     .fetch_all(&pool).await?;
//! ```

extern crate self as sql_query;

use crate::{delete::SqlDelete, insert::SqlInsert, shared::Cte, update::SqlUpdate};

pub use select::SqlSelect;
pub use set_op::SqlSetOp;
pub use shared::{
    Id, SqlColId, SqlConflict, Table,
    cached::{cache_key, invalidate_tables},
    error::SqlQueryError,
    expr::{ColOps, EvalExpr, Expr, FrameBound, SqlJoin, SqlOrder, WindowSpec},
    unbinded_query::{
        BoundQuery, BoundQueryAs, BoundQueryScalar, CachedBoundQueryAs, CachedBoundQueryScalar,
        InvalidatingBoundQuery, InvalidatingBoundQueryAs, UnbindedQuery,
    },
    value::{SqlEnum, SqlParam},
};
pub use sql_query_derive::{SqlCols, SqlParamEnum};

mod delete;
mod insert;
mod select;
pub(crate) mod set_op;
mod shared;
mod update;

/// Trait implemented by all statement builders (SELECT, INSERT, UPDATE, DELETE).
/// Call `.build()` to finalise the query into an `UnbindedQuery`.
pub trait SqlBase {
    /// # Errors
    /// Returns `sqlx::Error::Protocol` if the underlying expressions fail to compose into valid SQL.
    fn build(self) -> Result<UnbindedQuery, sqlx::Error>;
}

/// Builder returned by `SqlQ::with()`. Holds pre-built CTEs and provides
/// the same factory methods as `SqlQ` to start a statement within a CTE context.
pub struct SqlWith {
    ctes: Vec<Cte>,
}

impl SqlWith {
    /// `WITH ... SELECT * FROM "T"`.
    #[must_use] 
    pub fn select<T: Table>(self) -> SqlSelect {
        SqlSelect::new_with::<T>(self.ctes)
    }

    /// `WITH ... DELETE FROM "T"`.
    #[must_use] 
    pub fn delete<T: Table>(self) -> SqlDelete<T> {
        SqlDelete::new_with(self.ctes)
    }

    /// `WITH ... INSERT INTO "T"`.
    #[must_use] 
    pub fn insert<T: Table>(self) -> SqlInsert<T> {
        SqlInsert::new_with(self.ctes)
    }

    /// `WITH ... UPDATE "T"`.
    #[must_use] 
    pub fn update<T: Table>(self) -> SqlUpdate {
        SqlUpdate::new_with::<T>(self.ctes)
    }
}

/// Entry point for building SQL queries.
///
/// ```ignore
/// // SELECT
/// SqlQ::select::<Users>().filter([...]).build()?.bind_as::<Users>().fetch_all(&pool).await?;
/// // INSERT
/// SqlQ::insert::<Users>().values([...])?.build()?.bind().execute(&pool).await?;
/// // UPDATE
/// SqlQ::update::<Users>().set([...]).filter([...]).build()?.bind().execute(&pool).await?;
/// // DELETE
/// SqlQ::delete::<Users>().filter([...]).build()?.bind().execute(&pool).await?;
/// // CTE
/// SqlQ::with([("active", SqlQ::select::<Users>().filter([...]))]).select::<Users>()...
/// ```
pub struct SqlQ;

impl SqlQ {
    /// Start a `SELECT * FROM "T"` builder.
    #[must_use] 
    pub fn select<T: Table>() -> SqlSelect {
        SqlSelect::new::<T>()
    }

    /// Start a `DELETE FROM "T"` builder.
    #[must_use]
    pub const fn delete<T: Table>() -> SqlDelete<T> {
        SqlDelete::new()
    }

    /// Start an `INSERT INTO "T"` builder.
    #[must_use]
    pub const fn insert<T: Table>() -> SqlInsert<T> {
        SqlInsert::new()
    }

    /// Start an `UPDATE "T"` builder.
    #[must_use] 
    pub fn update<T: Table>() -> SqlUpdate {
        SqlUpdate::new::<T>()
    }

    /// # Errors
    /// Propagates `sqlx::Error` from the underlying database call.
    pub fn select_one_id<T: Table>(id: T::Id) -> Result<BoundQueryAs<T>, sqlx::Error>
    where
        T::Col: SqlColId,
    {
        Ok(Self::select::<T>()
            .filter([Expr::<T>::new().column(T::Col::id()).eq(id)])
            .build()?
            .bind_as::<T>())
    }

    /// # Errors
    /// Propagates `sqlx::Error` from the underlying database call.
    pub fn delete_one_id<T: Table>(id: T::Id) -> Result<BoundQuery, sqlx::Error>
    where
        T::Col: SqlColId,
    {
        Ok(Self::delete::<T>()
            .filter([Expr::<T>::new().column(T::Col::id()).eq(id)])
            .build()?
            .bind())
    }

    /// # Panics
    /// Panics if any sub-expression fails to evaluate or build (`.eval().unwrap()` / `.build().expect()`).
    /// Start a `WITH name AS (query), ... ` CTE block.
    /// Returns a `SqlWith` that provides the same `.select()`, `.insert()`, etc. methods.
    pub fn with(ctes: impl IntoIterator<Item = (&'static str, impl SqlBase)>) -> SqlWith {
        let built_ctes: Vec<Cte> = ctes
            .into_iter()
            .map(|(name, query)| {
                let uq = query.build().expect("CTE query build failed");
                let (sql, binds, tables) = uq.into_raw_with_tables();
                Cte { name: name.to_string(), sql, binds, tables }
            })
            .collect();
        SqlWith { ctes: built_ctes }
    }
}
