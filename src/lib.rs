//! Type-safe dynamic SQL query builder for PostgreSQL, built on sqlx.
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
pub use shared::{
    Id, SqlColId, SqlConflict, Table,
    error::SqlQueryError,
    expr::{ColOps, EvalExpr, Expr, SqlJoin, SqlOrder},
    unbinded_query::{
        BoundQuery, BoundQueryAs, BoundQueryScalar, CachedBoundQueryAs, CachedBoundQueryScalar,
        UnbindedQuery,
    },
    value::{SqlEnum, SqlParam},
};
pub use sql_query_derive::{SqlCols, SqlParamEnum};

mod delete;
mod insert;
mod select;
mod shared;
mod update;

/// Trait implemented by all statement builders (SELECT, INSERT, UPDATE, DELETE).
pub trait SqlBase {
    fn build(self) -> Result<UnbindedQuery, sqlx::Error>;
}

pub struct SqlWith {
    ctes: Vec<Cte>,
}

impl SqlWith {
    pub fn select<T: Table>(self) -> SqlSelect {
        SqlSelect::new_with::<T>(self.ctes)
    }

    pub fn delete<T: Table>(self) -> SqlDelete<T> {
        SqlDelete::new_with(self.ctes)
    }

    pub fn insert<T: Table>(self) -> SqlInsert<T> {
        SqlInsert::new_with(self.ctes)
    }

    pub fn update<T: Table>(self) -> SqlUpdate {
        SqlUpdate::new_with::<T>(self.ctes)
    }
}

/// Entry point for building SQL queries.
///
/// Provides factory methods for each statement type and CTE support.
pub struct SqlQ;

impl SqlQ {
    pub fn select<T: Table>() -> SqlSelect {
        SqlSelect::new::<T>()
    }

    pub fn delete<T: Table>() -> SqlDelete<T> {
        SqlDelete::new()
    }

    pub fn insert<T: Table>() -> SqlInsert<T> {
        SqlInsert::new()
    }

    pub fn update<T: Table>() -> SqlUpdate {
        SqlUpdate::new::<T>()
    }

    /// Builds a ```SELECT * WHERE id = $1``` query for a single row by primary key.
    pub fn select_one_id<T: Table>(id: T::Id) -> Result<BoundQueryAs<T>, sqlx::Error>
    where
        T::Col: SqlColId,
    {
        Ok(Self::select::<T>()
            .filter([Expr::<T>::new().column(T::Col::id()).eq().val(id)])
            .build()?
            .bind_as::<T>())
    }

    /// Builds a ```DELETE WHERE id = $1``` query for a single row by primary key.
    pub fn delete_one_id<T: Table>(id: T::Id) -> Result<BoundQuery, sqlx::Error>
    where
        T::Col: SqlColId,
    {
        Ok(Self::delete::<T>()
            .filter([Expr::<T>::new().column(T::Col::id()).eq().val(id)])
            .build()?
            .bind())
    }

    pub fn with(ctes: impl IntoIterator<Item = (&'static str, impl SqlBase)>) -> SqlWith {
        let built_ctes: Vec<Cte> = ctes
            .into_iter()
            .map(|(name, query)| {
                let uq = query.build().expect("CTE query build failed");
                let (sql, binds) = uq.into_raw();
                Cte { name: name.to_string(), sql, binds }
            })
            .collect();
        SqlWith { ctes: built_ctes }
    }
}
