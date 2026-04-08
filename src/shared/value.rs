use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{
    Database, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgTypeInfo},
};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone)]
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
    // Array(Vec<SqlParam>),
    Null,
}

impl SqlParam {
    pub fn is_null(&self) -> bool {
        matches!(self, SqlParam::Null)
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
    // Vec<SqlParam> => Array,
}

impl From<&str> for SqlParam {
    fn from(value: &str) -> Self {
        SqlParam::String(value.to_string())
    }
}

impl From<NaiveDate> for SqlParam {
    fn from(value: NaiveDate) -> Self {
        SqlParam::DateTimeUtc(value.and_hms_opt(0, 0, 0).unwrap().and_utc())
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

macro_rules! encode_dispatch {
    ($self:expr, $buf:expr, $method:ident; $($variant:ident($inner:ty)),* $(,)?) => {
        match $self {
            $(SqlParam::$variant(v) => <$inner as Encode<'_, Postgres>>::$method(v, $buf),)*
            SqlParam::Null => Ok(IsNull::Yes),
        }
    };
}

macro_rules! type_info_dispatch {
    ($self:expr; $($variant:ident($inner:ty)),* $(,)?) => {
        match $self {
            $(SqlParam::$variant(_) => <$inner as Type<Postgres>>::type_info(),)*
            SqlParam::Null => <() as Type<Postgres>>::type_info(),
        }
    };
}

impl<'q> Encode<'q, Postgres> for SqlParam {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        encode_dispatch!(self, buf, encode_by_ref;
            String(String), I16(i16), I32(i32), I64(i64), F64(f64), Bool(bool),
            Decimal(Decimal), Json(Value), DateTimeUtc(DateTime<Utc>),
            Uuid(Uuid), // Array(Vec<SqlParam>),
        )
    }

    fn produces(&self) -> Option<<Postgres as Database>::TypeInfo> {
        Some(type_info_dispatch!(self;
            String(String), I16(i16), I32(i32), I64(i64), F64(f64), Bool(bool),
            Decimal(Decimal), Json(Value), DateTimeUtc(DateTime<Utc>),
            Uuid(Uuid), // Array(Vec<SqlParam>),
        ))
    }
}

impl Type<Postgres> for SqlParam {
    fn type_info() -> PgTypeInfo {
        <() as Type<Postgres>>::type_info()
    }
}
