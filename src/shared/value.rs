use std::fmt;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{
    Database, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo},
};
use uuid::Uuid;

/// Marker trait for custom Postgres enums derived with `SqlParamEnum`.
///
/// Enables a blanket `From<Vec<T>> for SqlParam` without conflicting
/// with the concrete `From<Vec<String>>`, `From<Vec<i32>>`, etc. impls.
pub trait SqlEnum:
    for<'q> Encode<'q, Postgres>
    + Type<Postgres>
    + PgHasArrayType
    + Clone
    + Send
    + Sync
    + fmt::Debug
    + 'static
{
}

/// Object-safe trait for type-erased Postgres bind parameters.
/// Implemented via blanket impl for any `Encode + Type + Clone + Send + Sync + Debug`.
/// Used internally by `SqlParam::Custom` to box arbitrary Postgres types.
pub trait SqlParamCustom: Send + Sync {
    fn encode_param(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError>;
    fn type_info_param(&self) -> PgTypeInfo;
    fn clone_box(&self) -> Box<dyn SqlParamCustom>;
    fn debug_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl<T> SqlParamCustom for T
where
    T: for<'q> Encode<'q, Postgres> + Type<Postgres> + Clone + Send + Sync + fmt::Debug + 'static,
{
    fn encode_param(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        self.encode_by_ref(buf)
    }
    fn type_info_param(&self) -> PgTypeInfo {
        <T as Type<Postgres>>::type_info()
    }
    fn clone_box(&self) -> Box<dyn SqlParamCustom> {
        Box::new(self.clone())
    }
    fn debug_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A type-erased Postgres bind parameter.
///
/// Wraps all supported scalar and array types into a single enum that can be
/// stored in a `Vec` alongside the SQL string. `From` impls exist for all
/// common Rust types (`String`, `i32`, `Uuid`, `Vec<i64>`, etc.), `Option<T>`
/// maps to the inner variant or `Null`, and custom Postgres enums go through
/// the `Custom` variant via `#[derive(SqlParamEnum)]`.
pub enum SqlParam {
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    Decimal(Decimal),
    Json(Value),
    DateTimeUtc(DateTime<Utc>),
    Uuid(Uuid),
    StringArray(Vec<String>),
    I32Array(Vec<i32>),
    I64Array(Vec<i64>),
    F64Array(Vec<f64>),
    BoolArray(Vec<bool>),
    DecimalArray(Vec<Decimal>),
    DateTimeUtcArray(Vec<DateTime<Utc>>),
    UuidArray(Vec<Uuid>),
    Custom(Box<dyn SqlParamCustom>),
    CustomArray(Box<dyn SqlParamCustom>),
    Null,
}

impl SqlParam {
    pub fn is_null(&self) -> bool {
        matches!(self, SqlParam::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(
            self,
            SqlParam::StringArray(_)
                | SqlParam::I32Array(_)
                | SqlParam::I64Array(_)
                | SqlParam::F64Array(_)
                | SqlParam::BoolArray(_)
                | SqlParam::DecimalArray(_)
                | SqlParam::DateTimeUtcArray(_)
                | SqlParam::UuidArray(_)
                | SqlParam::CustomArray(_)
        )
    }

    pub fn custom<T>(val: T) -> Self
    where
        T: for<'q> Encode<'q, Postgres>
            + Type<Postgres>
            + Clone
            + Send
            + Sync
            + fmt::Debug
            + 'static,
    {
        SqlParam::Custom(Box::new(val))
    }

    /// Serializes any `Serialize` value to `SqlParam::Json`.
    pub fn json<T: serde::Serialize>(val: T) -> Self {
        SqlParam::Json(serde_json::to_value(val).expect("json serialization failed"))
    }
}

impl fmt::Debug for SqlParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(v) => f.debug_tuple("String").field(v).finish(),
            Self::I16(v) => f.debug_tuple("I16").field(v).finish(),
            Self::I32(v) => f.debug_tuple("I32").field(v).finish(),
            Self::I64(v) => f.debug_tuple("I64").field(v).finish(),
            Self::F64(v) => f.debug_tuple("F64").field(v).finish(),
            Self::Bool(v) => f.debug_tuple("Bool").field(v).finish(),
            Self::Decimal(v) => f.debug_tuple("Decimal").field(v).finish(),
            Self::Json(v) => f.debug_tuple("Json").field(v).finish(),
            Self::DateTimeUtc(v) => f.debug_tuple("DateTimeUtc").field(v).finish(),
            Self::Uuid(v) => f.debug_tuple("Uuid").field(v).finish(),
            Self::StringArray(v) => f.debug_tuple("StringArray").field(v).finish(),
            Self::I32Array(v) => f.debug_tuple("I32Array").field(v).finish(),
            Self::I64Array(v) => f.debug_tuple("I64Array").field(v).finish(),
            Self::F64Array(v) => f.debug_tuple("F64Array").field(v).finish(),
            Self::BoolArray(v) => f.debug_tuple("BoolArray").field(v).finish(),
            Self::DecimalArray(v) => f.debug_tuple("DecimalArray").field(v).finish(),
            Self::DateTimeUtcArray(v) => f.debug_tuple("DateTimeUtcArray").field(v).finish(),
            Self::UuidArray(v) => f.debug_tuple("UuidArray").field(v).finish(),
            Self::Custom(v) => v.debug_fmt(f),
            Self::CustomArray(v) => v.debug_fmt(f),
            Self::Null => write!(f, "Null"),
        }
    }
}

impl Clone for SqlParam {
    fn clone(&self) -> Self {
        match self {
            Self::String(v) => Self::String(v.clone()),
            Self::I16(v) => Self::I16(*v),
            Self::I32(v) => Self::I32(*v),
            Self::I64(v) => Self::I64(*v),
            Self::F64(v) => Self::F64(*v),
            Self::Bool(v) => Self::Bool(*v),
            Self::Decimal(v) => Self::Decimal(*v),
            Self::Json(v) => Self::Json(v.clone()),
            Self::DateTimeUtc(v) => Self::DateTimeUtc(*v),
            Self::Uuid(v) => Self::Uuid(*v),
            Self::StringArray(v) => Self::StringArray(v.clone()),
            Self::I32Array(v) => Self::I32Array(v.clone()),
            Self::I64Array(v) => Self::I64Array(v.clone()),
            Self::F64Array(v) => Self::F64Array(v.clone()),
            Self::BoolArray(v) => Self::BoolArray(v.clone()),
            Self::DecimalArray(v) => Self::DecimalArray(v.clone()),
            Self::DateTimeUtcArray(v) => Self::DateTimeUtcArray(v.clone()),
            Self::UuidArray(v) => Self::UuidArray(v.clone()),
            Self::Custom(v) => Self::Custom(v.clone_box()),
            Self::CustomArray(v) => Self::CustomArray(v.clone_box()),
            Self::Null => Self::Null,
        }
    }
}

impl PartialEq for SqlParam {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(a), Self::String(b)) => a == b,
            (Self::I16(a), Self::I16(b)) => a == b,
            (Self::I32(a), Self::I32(b)) => a == b,
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::F64(a), Self::F64(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Decimal(a), Self::Decimal(b)) => a == b,
            (Self::Json(a), Self::Json(b)) => a == b,
            (Self::DateTimeUtc(a), Self::DateTimeUtc(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::StringArray(a), Self::StringArray(b)) => a == b,
            (Self::I32Array(a), Self::I32Array(b)) => a == b,
            (Self::I64Array(a), Self::I64Array(b)) => a == b,
            (Self::F64Array(a), Self::F64Array(b)) => a == b,
            (Self::BoolArray(a), Self::BoolArray(b)) => a == b,
            (Self::DecimalArray(a), Self::DecimalArray(b)) => a == b,
            (Self::DateTimeUtcArray(a), Self::DateTimeUtcArray(b)) => a == b,
            (Self::UuidArray(a), Self::UuidArray(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

macro_rules! impl_sql_param_from {
    ($($type:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$type> for SqlParam {
                fn from(value: $type) -> Self {
                    SqlParam::$variant(value)
                }
            }
            impl From<&$type> for SqlParam
            where
                $type: Clone,
            {
                fn from(value: &$type) -> Self {
                    SqlParam::$variant(value.clone())
                }
            }
        )*
    };
}

impl_sql_param_from! {
    String => String,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    f64 => F64,
    bool => Bool,
    Decimal => Decimal,
    Value => Json,
    DateTime<Utc> => DateTimeUtc,
    Uuid => Uuid,
    Vec<String> => StringArray,
    Vec<i32> => I32Array,
    Vec<i64> => I64Array,
    Vec<f64> => F64Array,
    Vec<bool> => BoolArray,
    Vec<Decimal> => DecimalArray,
    Vec<DateTime<Utc>> => DateTimeUtcArray,
    Vec<Uuid> => UuidArray,
}

impl From<&str> for SqlParam {
    fn from(value: &str) -> Self {
        SqlParam::String(value.to_string())
    }
}

impl From<Vec<&str>> for SqlParam {
    fn from(value: Vec<&str>) -> Self {
        SqlParam::StringArray(value.into_iter().map(String::from).collect())
    }
}

macro_rules! impl_sql_param_from_ref_vec {
    ($($elem:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<Vec<&$elem>> for SqlParam {
                fn from(value: Vec<&$elem>) -> Self {
                    SqlParam::$variant(value.into_iter().cloned().collect())
                }
            }
        )*
    };
}

impl_sql_param_from_ref_vec! {
    i32 => I32Array,
    i64 => I64Array,
    f64 => F64Array,
    bool => BoolArray,
    Decimal => DecimalArray,
    DateTime<Utc> => DateTimeUtcArray,
    Uuid => UuidArray,
}

impl From<NaiveDate> for SqlParam {
    fn from(value: NaiveDate) -> Self {
        SqlParam::DateTimeUtc(
            value.and_hms_opt(0, 0, 0).expect("midnight is always valid").and_utc(),
        )
    }
}

impl<T> From<Option<T>> for SqlParam
where
    SqlParam: From<T>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => SqlParam::Null,
        }
    }
}

impl<T: SqlEnum> From<Vec<T>> for SqlParam {
    fn from(value: Vec<T>) -> Self {
        SqlParam::CustomArray(Box::new(value))
    }
}

macro_rules! encode_dispatch {
    ($self:expr, $buf:expr, $method:ident; $($variant:ident($inner:ty)),* $(,)?) => {
        match $self {
            $(SqlParam::$variant(v) => <$inner as Encode<'_, Postgres>>::$method(v, $buf),)*
            _ => unreachable!(),
        }
    };
}

macro_rules! type_info_dispatch {
    ($self:expr; $($variant:ident($inner:ty)),* $(,)?) => {
        match $self {
            $(SqlParam::$variant(_) => <$inner as Type<Postgres>>::type_info(),)*
            _ => unreachable!(),
        }
    };
}

impl<'q> Encode<'q, Postgres> for SqlParam {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        match self {
            SqlParam::Custom(v) | SqlParam::CustomArray(v) => v.encode_param(buf),
            SqlParam::Null => Ok(IsNull::Yes),
            other => {
                encode_dispatch!(other, buf, encode_by_ref;
                    String(String), I16(i16), I32(i32), I64(i64), F64(f64), Bool(bool),
                    Decimal(Decimal), Json(Value), DateTimeUtc(DateTime<Utc>),
                    Uuid(Uuid),
                    StringArray(Vec<String>), I32Array(Vec<i32>), I64Array(Vec<i64>),
                    F64Array(Vec<f64>), BoolArray(Vec<bool>), DecimalArray(Vec<Decimal>),
                    DateTimeUtcArray(Vec<DateTime<Utc>>), UuidArray(Vec<Uuid>),
                )
            }
        }
    }

    fn produces(&self) -> Option<<Postgres as Database>::TypeInfo> {
        match self {
            SqlParam::Custom(v) | SqlParam::CustomArray(v) => Some(v.type_info_param()),
            SqlParam::Null => Some(<() as Type<Postgres>>::type_info()),
            other => Some(type_info_dispatch!(other;
                String(String), I16(i16), I32(i32), I64(i64), F64(f64), Bool(bool),
                Decimal(Decimal), Json(Value), DateTimeUtc(DateTime<Utc>),
                Uuid(Uuid),
                StringArray(Vec<String>), I32Array(Vec<i32>), I64Array(Vec<i64>),
                F64Array(Vec<f64>), BoolArray(Vec<bool>), DecimalArray(Vec<Decimal>),
                DateTimeUtcArray(Vec<DateTime<Utc>>), UuidArray(Vec<Uuid>),
            )),
        }
    }
}

impl Type<Postgres> for SqlParam {
    fn type_info() -> PgTypeInfo {
        <() as Type<Postgres>>::type_info()
    }
}
