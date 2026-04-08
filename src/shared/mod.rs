use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Encode, FromRow, Postgres, Type, postgres::PgRow};
use uuid::Uuid;

pub mod error;
pub mod expr;
pub mod unbinded_query;
pub mod value;

pub use unbinded_query::UnbindedQuery;
pub(crate) use unbinded_query::push_conditions;

use crate::shared::value::SqlParam;

/// Conflict resolution strategy for INSERT ON CONFLICT clauses.
pub enum SqlConflict<C: AsRef<str>> {
    DoNothing,
    DoUpdate { conflict_cols: Vec<C>, update_cols: Vec<C> },
    OnConstraint { name: &'static str, update_cols: Vec<C> },
}

/// Column enum variant that represents the primary-key / id column.
pub trait SqlColId {
    fn id() -> Self;
}

pub(crate) struct Cte {
    pub name: String,
    pub sql: String,
    pub binds: Vec<SqlParam>,
}

pub(crate) fn prepend_ctes(ctes: Vec<Cte>, sql: &mut String, binds: &mut Vec<SqlParam>) {
    if ctes.is_empty() {
        return;
    }
    let mut prefix = String::from("WITH ");
    let mut cte_binds = vec![];
    for (i, cte) in ctes.into_iter().enumerate() {
        if i > 0 {
            prefix.push_str(", ");
        }
        prefix.push_str(&cte.name);
        prefix.push_str(" AS (");
        prefix.push_str(&cte.sql);
        prefix.push(')');
        cte_binds.extend(cte.binds);
    }
    prefix.push(' ');
    prefix.push_str(sql);
    *sql = prefix;
    cte_binds.extend(binds.drain(..));
    *binds = cte_binds;
}

pub(crate) enum Returning {
    None,
    All,
    Columns(Vec<String>),
}

pub(crate) fn push_returning(
    returning: Returning,
    qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
) {
    match returning {
        Returning::None => {}
        Returning::All => {
            qb.push(" RETURNING *");
        }
        Returning::Columns(cols) => {
            qb.push(" RETURNING ");
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    qb.push(", ");
                }
                qb.push(col);
            }
        }
    }
}

/// Describes a Postgres table, its column enum, and its primary-key type.
pub trait Table: for<'r> FromRow<'r, PgRow> + Send + Unpin + Debug + 'static {
    type Col: AsRef<str> + Display + Copy;
    type Id: Id + Into<SqlParam>;
    const TABLE_NAME: &'static str;
    const PRIMARY_KEY: &'static str;
}

/// Trait shared by all newtype ID wrappers.
///
/// Provides conversion to/from the raw `Uuid` and bounds required by
/// handlers, services, and the query layer.
pub trait Id:
    Copy
    + Clone
    + Debug
    + Display
    + PartialEq
    + Eq
    + Hash
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + for<'q> Encode<'q, Postgres>
    + Type<Postgres>
    + 'static
{
    fn new() -> Self;
    fn from_raw(id: Uuid) -> Self;
    fn raw(self) -> Uuid;
}

/// Generate a newtype ID wrapper with all required trait implementations.
///
/// ```ignore
/// define_id!(AccountId);
/// ```
#[macro_export]
macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Copy,
            Clone,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::sqlx::Type,
        )]
        #[serde(transparent)]
        #[sqlx(transparent)]
        pub struct $name(pub ::uuid::Uuid);

        impl $crate::Id for $name {
            #[inline]
            fn new() -> Self {
                Self(::uuid::Uuid::now_v7())
            }

            #[inline]
            fn from_raw(id: ::uuid::Uuid) -> Self {
                Self(id)
            }

            #[inline]
            fn raw(self) -> ::uuid::Uuid {
                self.0
            }
        }

        impl ::std::default::Default for $name {
            #[inline]
            fn default() -> Self {
                Self(::uuid::Uuid::nil())
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = ::uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<::uuid::Uuid>().map(Self)
            }
        }

        impl From<::uuid::Uuid> for $name {
            #[inline]
            fn from(id: ::uuid::Uuid) -> Self {
                Self(id)
            }
        }

        impl From<$name> for ::uuid::Uuid {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl From<$name> for $crate::SqlParam {
            fn from(id: $name) -> Self {
                $crate::SqlParam::Uuid(id.0)
            }
        }

        impl ::ts_rs::TS for $name {
            type WithoutGenerics = Self;
            type OptionInnerType = Self;

            fn name(cfg: &::ts_rs::Config) -> String {
                <::uuid::Uuid as ::ts_rs::TS>::name(cfg)
            }

            fn inline(cfg: &::ts_rs::Config) -> String {
                <::uuid::Uuid as ::ts_rs::TS>::inline(cfg)
            }

            fn output_path() -> Option<std::path::PathBuf> {
                None
            }
        }
    };
}
