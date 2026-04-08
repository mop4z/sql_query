use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Encode, FromRow, Postgres, Type, postgres::PgRow};
use uuid::Uuid;

pub mod error;
pub mod expr;
mod unbinded_query;
pub mod value;

pub use unbinded_query::UnbindedQuery;

use crate::shared::value::SqlParam;

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

        impl $crate::shared::Id for $name {
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

        impl From<$name> for $crate::shared::value::SqlParam {
            fn from(id: $name) -> Self {
                $crate::shared::value::SqlParam::Uuid(id.0)
            }
        }

        // impl ::ts_rs::TS for $name {
        //     type WithoutGenerics = Self;
        //     type OptionInnerType = Self;

        //     fn name(cfg: &::ts_rs::Config) -> String {
        //         <::uuid::Uuid as ::ts_rs::TS>::name(cfg)
        //     }

        //     fn inline(cfg: &::ts_rs::Config) -> String {
        //         <::uuid::Uuid as ::ts_rs::TS>::inline(cfg)
        //     }

        //     fn output_path() -> Option<std::path::PathBuf> {
        //         None
        //     }
        // }
    };
}
