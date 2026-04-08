use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro that generates a `{Struct}Col` enum with expression helper methods.
///
/// # Example
///
/// ```ignore
/// #[derive(SqlCols)]
/// pub struct Currency {
///     pub id: CurrencyId,
///     pub name: String,
///     pub symbol: String,
///     pub currency_type: CurrencyType,
/// }
///
/// // Generates CurrencyCol enum with variants Id, Name, Symbol, CurrencyType
/// // plus helper methods: .eq(), .neq(), .gt(), .lt(), .is_null(), .count(), etc.
/// // and From<CurrencyCol> for SqlExpr<Currency>
///
/// // Usage:
/// CurrencyCol::Name.eq("USD")
/// CurrencyCol::Id.count().alias("total")
/// CurrencyCol::Name.into() // SqlExpr<Currency> via From
/// ```
#[proc_macro_derive(SqlCols)]
pub fn derive_sql_cols(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("SqlCols only supports structs with named fields"),
        },
        _ => panic!("SqlCols can only be derived for structs"),
    };

    let field_names: Vec<_> =
        fields.iter().map(|f| f.ident.as_ref().expect("named field")).collect();

    let enum_name = format_ident!("{struct_name}Col");

    let variants: Vec<_> =
        field_names.iter().map(|f| format_ident!("{}", to_pascal_case(&f.to_string()))).collect();

    let has_id_field = field_names.iter().any(|f| f.to_string() == "id");

    let col_id_impl = if has_id_field {
        quote! {
            impl ::sql_query::SqlColId for #enum_name {
                fn id() -> Self { Self::Id }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, strum::AsRefStr, strum::Display)]
        #[strum(serialize_all = "snake_case")]
        pub enum #enum_name {
            #( #variants, )*
        }

        #col_id_impl

        impl From<#enum_name> for ::sql_query::SqlExpr<#struct_name> {
            fn from(col: #enum_name) -> Self {
                ::sql_query::SqlExpr::column(col)
            }
        }

        impl #enum_name {
            pub fn is_null(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::is_null(self)
            }

            pub fn is_not_null(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::is_not_null(self)
            }

            pub fn eq(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::eq(self, val)
            }

            pub fn neq(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::neq(self, val)
            }

            pub fn gt(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::gt(self, val)
            }

            pub fn gte(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::gte(self, val)
            }

            pub fn lt(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::lt(self, val)
            }

            pub fn lte(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::lte(self, val)
            }

            pub fn like(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::like(self, val)
            }

            pub fn ilike(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::ilike(self, val)
            }

            pub fn in_(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::in_(self, val)
            }

            pub fn not_in(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::not_in(self, val)
            }

            pub fn between(self, lo: impl Into<::sql_query::SqlParam>, hi: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::between(self, lo, hi)
            }

            pub fn in_select(self, select: ::sql_query::SqlSelect) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::in_select(self, select)
            }

            pub fn not_in_select(self, select: ::sql_query::SqlSelect) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::not_in_select(self, select)
            }

            pub fn count(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::count(self)
            }

            pub fn sum(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::sum(self)
            }

            pub fn avg(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::avg(self)
            }

            pub fn min(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::min(self)
            }

            pub fn max(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::max(self)
            }

            pub fn lower(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::lower(self)
            }

            pub fn upper(self) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::upper(self)
            }

            pub fn json_get(self, key: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::json_get(self, key)
            }

            pub fn json_get_text(self, key: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::json_get_text(self, key)
            }

            pub fn any(self, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::any(self, val)
            }

            pub fn jsonb_text_eq(self, key: impl Into<::sql_query::SqlParam>, val: impl Into<::sql_query::SqlParam>) -> ::sql_query::SqlExpr<#struct_name> {
                ::sql_query::SqlExpr::jsonb_text_eq(self, key, val)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derives `From<Enum> for SqlParam` and `From<&Enum> for SqlParam` for a Postgres enum type.
///
/// The enum must implement `sqlx::Type<Postgres>`, `sqlx::Encode<Postgres>`, `Clone`,
/// `Debug`, `Send`, and `Sync`. Typically achieved via `#[derive(sqlx::Type, Clone, Debug)]`.
///
/// # Example
///
/// ```ignore
/// #[derive(Clone, Debug, sqlx::Type, SqlParamEnum)]
/// #[sqlx(type_name = "currency_type", rename_all = "snake_case")]
/// pub enum CurrencyType {
///     Fiat,
///     Crypto,
/// }
///
/// // Now you can use CurrencyType directly as a bind value:
/// SqlExpr::eq(CurrencyCol::CurrencyType, CurrencyType::Fiat)
/// ```
#[proc_macro_derive(SqlParamEnum)]
pub fn derive_sql_param_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    match &input.data {
        Data::Enum(_) => {}
        _ => panic!("SqlParamEnum can only be derived for enums"),
    }

    let expanded = quote! {
        impl From<#name> for ::sql_query::SqlParam {
            fn from(value: #name) -> Self {
                ::sql_query::SqlParam::custom(value)
            }
        }

        impl From<&#name> for ::sql_query::SqlParam {
            fn from(value: &#name) -> Self {
                ::sql_query::SqlParam::custom(value.clone())
            }
        }
    };

    TokenStream::from(expanded)
}

/// Convert `currency_type` → `CurrencyType`
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + &chars.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect()
}
