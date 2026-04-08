use std::fmt;

/// Errors produced when building a SQL query from invalid builder state.
#[derive(Debug)]
pub enum SqlQueryError {
    AndOrBothSet,
    BetweenMissingBounds,
    ExistsMissingSelect,
    InsertValuesAlreadySet,
    DeleteRequiresFilterOrDeleteAll,
    CaseRequiresThenAndElse,
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
