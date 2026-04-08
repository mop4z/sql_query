extern crate self as sql_query;

use crate::{
    delete::SqlDelete,
    insert::SqlInsert,
    select::SqlSelect,
    shared::{Cte, UnbindedQuery},
    update::SqlUpdate,
};

pub use shared::{
    Id, SqlColId, SqlConflict, Table,
    error::SqlQueryError,
    expr::{SqlExpr, SqlFn, SqlJoin, SqlOp, SqlOrder},
    unbinded_query::{BoundQuery, BoundQueryAs, BoundQueryScalar},
    value::SqlParam,
};
pub use sql_query_derive::SqlCols;

mod delete;
mod insert;
mod select;
mod shared;
mod update;

pub(crate) trait SqlBase {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error>;
}

pub(crate) struct SqlWith {
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
