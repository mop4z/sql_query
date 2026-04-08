extern crate self as sql_query;

use crate::{
    select::SqlSelect,
    shared::{Table, UnbindedQuery},
};

pub use sql_query_derive::SqlCols;

mod select;
mod shared;

pub trait SqlBase {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error>;
}

pub enum SqlConflict {
    DoNothing,
    DoUpdate { conflict_cols: Vec<String>, update_cols: Vec<String> },
    OnConstraint { name: String, update_cols: Vec<String> },
}

pub trait SqlColId {
    fn id() -> Self;
}

pub struct SqlQ;

impl SqlQ {
    pub fn select<T: Table>() -> SqlSelect {
        SqlSelect::new::<T>()
    }
}
