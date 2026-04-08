use std::fmt;

#[derive(Debug)]
pub enum SqlQueryError {
    AndOrBothSet,
    BetweenMissingBounds,
    ExistsMissingSelect,
    InsertValuesAlreadySet,
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
        }
    }
}

impl std::error::Error for SqlQueryError {}
