use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro that generates a `{model_snake}_cols` module with a `Col` enum
/// and SCREAMING_SNAKE_CASE constants for each field.
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
/// // Generates:
/// #[derive(Debug, Copy, Clone, PartialEq, Eq, strum::AsRefStr, strum::Display)]
/// pub enum CurrencyCol {
///     Id,
///     Name,
///     Symbol,
///     CurrencyType
/// }
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
