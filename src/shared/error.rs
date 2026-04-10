use std::fmt;

/// Errors produced when building a SQL query from invalid builder state.
///
/// These are compile-time-ish errors — they indicate the builder was used
/// incorrectly, not that the database rejected a query.
#[derive(Debug)]
pub enum SqlQueryError {
    /// Both `.and()` and `.or()` were chained on the same expression.
    AndOrBothSet,
    /// `.between()` was called without both bounds.
    BetweenMissingBounds,
    /// `.exists()` or `.not_exists()` was called without a subquery.
    ExistsMissingSelect,
    /// `.values()` or `.values_nested()` was called more than once,
    /// or after `.from_select()`.
    InsertValuesAlreadySet,
    /// `.from_select()` was called after `.values()` / `.values_nested()`,
    /// or called more than once.
    InsertSourceAlreadySet,
    /// `DELETE` was built without `.filter()` or `.delete_all()`.
    /// This guard prevents accidental full-table deletes.
    DeleteRequiresFilterOrDeleteAll,
    /// `CASE WHEN` was started but `.then_()` or `.else_()` was missing.
    CaseRequiresThenAndElse,
    /// `.jsonb_text_eq()` requires both a key and a value argument.
    JsonbTextEqMissingArgs,
}

impl fmt::Display for SqlQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AndOrBothSet => write!(f, "both .and() and .or() set on the same SqlExpr"),
            Self::BetweenMissingBounds => write!(f, "BETWEEN requires both val and val2"),
            Self::ExistsMissingSelect => write!(f, "EXISTS/NOT EXISTS requires .select()"),
            Self::InsertValuesAlreadySet => {
                write!(f, "values already set, use values() or values_nested() but not both")
            }
            Self::InsertSourceAlreadySet => {
                write!(
                    f,
                    "insert source already set (use only one of values, values_nested, or from_select)"
                )
            }
            Self::DeleteRequiresFilterOrDeleteAll => {
                write!(f, "DELETE requires .filter() or .delete_all()")
            }
            Self::CaseRequiresThenAndElse => {
                write!(f, "CASE requires both .then() and .else_()")
            }
            Self::JsonbTextEqMissingArgs => {
                write!(f, "jsonb_text_eq requires both key and value")
            }
        }
    }
}

impl std::error::Error for SqlQueryError {}
