use std::fmt::Write;

use crate::{
    SqlBase,
    select::SqlSelect,
    shared::{Table, error::SqlQueryError, value::SqlParam},
};

/// A composable SQL expression that can represent columns, operators, values, and subqueries.
pub struct SqlExpr<T: Table> {
    pub(crate) col: Option<T::Col>,
    col_fn: Option<SqlFn>,
    pub(crate) op: Option<SqlOp>,
    pub(crate) val: Option<SqlParam>,
    val2: Option<SqlParam>,
    val_fn: Option<SqlFn>,
    alias: Option<&'static str>,
    select: Option<SqlSelect>,
    and: Option<Box<SqlExpr<T>>>,
    or: Option<Box<SqlExpr<T>>>,
    then: Option<Box<SqlExpr<T>>>,
    else_: Option<Box<SqlExpr<T>>>,
    negate: bool,
}

impl<T: Table> SqlExpr<T> {
    fn empty() -> Self {
        Self {
            col: None,
            col_fn: None,
            op: None,
            val: None,
            val2: None,
            val_fn: None,
            alias: None,
            select: None,
            and: None,
            or: None,
            then: None,
            else_: None,
            negate: false,
        }
    }

    /// Creates a new expression referencing the given column.
    pub fn column(col: T::Col) -> Self {
        Self::empty().col(col)
    }

    /// Creates an IS NULL check on the given column.
    pub fn is_null(col: T::Col) -> Self {
        Self::empty().col(col).op(SqlOp::IsNull)
    }

    /// Creates an IS NOT NULL check on the given column.
    pub fn is_not_null(col: T::Col) -> Self {
        Self::empty().col(col).op(SqlOp::IsNotNull)
    }

    /// Creates an equality comparison: col = val.
    pub fn eq(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Eq).val(val.into())
    }

    /// Creates a not-equal comparison: col != val.
    pub fn neq(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Neq).val(val.into())
    }

    /// Creates a greater-than comparison: col > val.
    pub fn gt(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Gt).val(val.into())
    }

    /// Creates a greater-than-or-equal comparison: col >= val.
    pub fn gte(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Gte).val(val.into())
    }

    /// Creates a less-than comparison: col < val.
    pub fn lt(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Lt).val(val.into())
    }

    /// Creates a less-than-or-equal comparison: col <= val.
    pub fn lte(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Lte).val(val.into())
    }

    /// Creates a LIKE pattern match: col LIKE val.
    pub fn like(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Like).val(val.into())
    }

    /// Creates a case-insensitive ILIKE pattern match: col ILIKE val.
    pub fn ilike(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::ILike).val(val.into())
    }

    /// Creates an IN check: col IN val.
    pub fn in_(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::In).val(val.into())
    }

    /// Creates a NOT IN check: col NOT IN val.
    pub fn not_in(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::NotIn).val(val.into())
    }

    /// Creates a BETWEEN check on the given column with lower and upper bounds.
    pub fn between(col: T::Col, lo: impl Into<SqlParam>, hi: impl Into<SqlParam>) -> Self {
        let mut e = Self::empty().col(col).op(SqlOp::Between).val(lo.into());
        e.val2 = Some(hi.into());
        e
    }

    /// Creates an IN subquery check: col IN (SELECT ...).
    pub fn in_select(col: T::Col, select: SqlSelect) -> Self {
        Self::empty().col(col).op(SqlOp::In).select(select)
    }

    /// Creates a NOT IN subquery check: col NOT IN (SELECT ...).
    pub fn not_in_select(col: T::Col, select: SqlSelect) -> Self {
        Self::empty().col(col).op(SqlOp::NotIn).select(select)
    }

    /// Creates an EXISTS check wrapping the given subquery.
    pub fn exists(select: SqlSelect) -> Self {
        Self::empty().op(SqlOp::Exists).select(select)
    }

    /// Creates a NOT EXISTS check wrapping the given subquery.
    pub fn not_exists(select: SqlSelect) -> Self {
        Self::empty().op(SqlOp::NotExists).select(select)
    }

    /// Creates a COUNT(col) expression.
    pub fn count(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Count)
    }

    /// Creates a SUM(col) expression.
    pub fn sum(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Sum)
    }

    /// Creates an AVG(col) expression.
    pub fn avg(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Avg)
    }

    /// Creates a MIN(col) expression.
    pub fn min(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Min)
    }

    /// Creates a MAX(col) expression.
    pub fn max(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Max)
    }

    /// Creates a LOWER(col) expression.
    pub fn lower(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Lower)
    }

    /// Creates an UPPER(col) expression.
    pub fn upper(col: T::Col) -> Self {
        Self::column(col).col_fn(SqlFn::Upper)
    }

    /// Creates a col -> key JSON get expression.
    pub fn json_get(col: T::Col, key: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::JsonGet).val(key.into())
    }

    /// Creates a col ->> key JSON get text expression.
    pub fn json_get_text(col: T::Col, key: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::JsonGetText).val(key.into())
    }

    /// Creates an = ANY(val) check on the given column.
    pub fn any(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Any).val(val.into())
    }

    /// Sets the THEN branch for a CASE WHEN expression.
    pub fn then(mut self, expr: SqlExpr<T>) -> Self {
        self.then = Some(Box::new(expr));
        self
    }

    /// Sets the ELSE branch for a CASE WHEN expression.
    pub fn else_(mut self, expr: SqlExpr<T>) -> Self {
        self.else_ = Some(Box::new(expr));
        self
    }

    /// Sets the column for this expression.
    pub fn col(mut self, col: T::Col) -> Self {
        self.col = Some(col);
        self
    }

    /// Wraps the column in a SQL function (e.g. COUNT, LOWER).
    pub fn col_fn(mut self, col_fn: SqlFn) -> Self {
        self.col_fn = Some(col_fn);
        self
    }

    /// Sets the SQL operator for this expression.
    pub fn op(mut self, op: SqlOp) -> Self {
        self.op = Some(op);
        self
    }

    /// Sets the bound parameter value for this expression.
    pub fn val(mut self, val: SqlParam) -> Self {
        self.val = Some(val);
        self
    }

    /// Wraps the value in a SQL function (e.g. LOWER, NOW).
    pub fn val_fn(mut self, val_fn: SqlFn) -> Self {
        self.val_fn = Some(val_fn);
        self
    }

    /// Sets an AS alias for the expression output.
    pub fn alias(mut self, alias: &'static str) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Sets a subquery as the value side of this expression.
    pub fn select(mut self, select: SqlSelect) -> Self {
        self.select = Some(select);
        self
    }

    /// Combines this expression with another using AND.
    pub fn and(mut self, other: SqlExpr<T>) -> Self {
        self.and = Some(Box::new(other));
        self
    }

    /// Combines this expression with another using OR.
    pub fn or(mut self, other: SqlExpr<T>) -> Self {
        self.or = Some(Box::new(other));
        self
    }

    /// Negates the entire expression with NOT.
    pub fn not(mut self) -> Self {
        self.negate = true;
        self
    }

    pub(crate) fn into_col_and_val(mut self) -> (Option<String>, Self) {
        let col = self.col.take().map(|c| c.as_ref().to_string());
        self.op = None;
        (col, self)
    }

    /// Evaluates the expression into a SQL string and its bound parameters.
    pub fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        if self.and.is_some() && self.or.is_some() {
            return Err(SqlQueryError::AndOrBothSet);
        }

        let mut out = String::new();
        let mut binds = Vec::new();

        if let Some(col) = &self.col {
            match &self.col_fn {
                Some(f) => write!(out, "{}(\"{}\".{})", f.as_ref(), T::TABLE_NAME, col.as_ref()),
                None => write!(out, "\"{}\".{}", T::TABLE_NAME, col.as_ref()),
            }
            .unwrap();
        }

        match &self.op {
            Some(SqlOp::IsNull | SqlOp::IsNotNull) => {
                write!(out, " {}", self.op.as_ref().unwrap().as_ref()).unwrap();
            }
            Some(SqlOp::Exists | SqlOp::NotExists) => {
                if self.select.is_none() {
                    return Err(SqlQueryError::ExistsMissingSelect);
                }
                write!(out, "{} ", self.op.as_ref().unwrap().as_ref()).unwrap();
                Self::write_val(self.select, self.val, &self.val_fn, &mut out, &mut binds);
            }
            Some(SqlOp::Between) => {
                if self.val.is_none() || self.val2.is_none() {
                    return Err(SqlQueryError::BetweenMissingBounds);
                }
                write!(out, " BETWEEN $# AND $#").unwrap();
                binds.push(self.val.unwrap());
                binds.push(self.val2.unwrap());
            }
            Some(SqlOp::Any | SqlOp::All) => {
                write!(out, " {} ", self.op.as_ref().unwrap().as_ref()).unwrap();
                write!(out, "($#)").unwrap();
                if let Some(v) = self.val {
                    binds.push(v);
                }
            }
            Some(op) => {
                write!(out, " {} ", op.as_ref()).unwrap();
                Self::write_val(self.select, self.val, &self.val_fn, &mut out, &mut binds);
            }
            None if self.val.is_some() || self.val_fn.is_some() || self.select.is_some() => {
                Self::write_val(self.select, self.val, &self.val_fn, &mut out, &mut binds);
            }
            None => {}
        }

        if let Some(right) = self.and {
            let (right_sql, right_binds) = right.eval()?;
            out.insert(0, '(');
            write!(out, " AND {right_sql})").unwrap();
            binds.extend(right_binds);
        } else if let Some(right) = self.or {
            let (right_sql, right_binds) = right.eval()?;
            out.insert(0, '(');
            write!(out, " OR {right_sql})").unwrap();
            binds.extend(right_binds);
        }

        if self.negate {
            out.insert_str(0, "NOT (");
            out.push(')');
        }

        if self.then.is_some() != self.else_.is_some() {
            return Err(SqlQueryError::CaseRequiresThenAndElse);
        }

        if let Some(then_expr) = self.then {
            let (then_sql, then_binds) = then_expr.eval()?;
            let (else_sql, else_binds) = self.else_.unwrap().eval()?;
            let condition = out;
            out = format!("CASE WHEN {condition} THEN {then_sql} ELSE {else_sql} END");
            binds.extend(then_binds);
            binds.extend(else_binds);
        }

        if let Some(alias) = &self.alias {
            write!(out, " AS {}", alias).unwrap();
        }

        Ok((out, binds))
    }

    fn write_val(
        select: Option<SqlSelect>,
        val: Option<SqlParam>,
        val_fn: &Option<SqlFn>,
        out: &mut String,
        binds: &mut Vec<SqlParam>,
    ) {
        if let Some(select) = select {
            let uq = SqlBase::build(select).expect("subquery build failed");
            let (sub_sql, sub_binds) = uq.into_raw();
            write!(out, "({})", sub_sql).unwrap();
            binds.extend(sub_binds);
            return;
        }
        match val_fn {
            Some(f @ SqlFn::Now) => write!(out, "{}()", f.as_ref()),
            Some(f @ (SqlFn::True | SqlFn::False)) => write!(out, "{}", f.as_ref()),
            Some(f) => {
                write!(out, "{}($#)", f.as_ref()).unwrap();
                if let Some(v) = val {
                    binds.push(v);
                }
                return;
            }
            None => {
                write!(out, "$#").unwrap();
                if let Some(v) = val {
                    binds.push(v);
                }
                return;
            }
        }
        .unwrap();
    }
}

/// SQL operators for comparisons, arithmetic, null checks, and JSON access.
pub enum SqlOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Like,
    ILike,
    Between,
    Add,
    Sub,
    Mul,
    Div,
    IsNull,
    IsNotNull,
    Exists,
    NotExists,
    Any,
    All,
    JsonGet,
    JsonGetText,
    JsonPath,
    JsonPathText,
}

/// SQL functions that can be applied to columns or values.
#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum SqlFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Now,
    Lower,
    Upper,
    Coalesce,
    Concat,
    Substring,
    Length,
    Trim,
    True,
    False,
}

impl AsRef<str> for SqlOp {
    fn as_ref(&self) -> &str {
        match self {
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::In => "IN",
            Self::NotIn => "NOT IN",
            Self::Like => "LIKE",
            Self::ILike => "ILIKE",
            Self::Between => "BETWEEN",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
            Self::Exists => "EXISTS",
            Self::NotExists => "NOT EXISTS",
            Self::Any => "= ANY",
            Self::All => "= ALL",
            Self::JsonGet => "->",
            Self::JsonGetText => "->>",
            Self::JsonPath => "#>",
            Self::JsonPathText => "#>>",
        }
    }
}

/// Sort directions for ORDER BY clauses.
#[derive(strum::AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum SqlOrder {
    Asc,
    Desc,
    #[strum(serialize = "ASC NULLS LAST")]
    AscNullsLast,
    #[strum(serialize = "DESC NULLS FIRST")]
    DescNullsFirst,
}

/// SQL join types for combining tables.
#[derive(strum::AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum SqlJoin {
    Inner,
    Left,
    Right,
    #[strum(serialize = "FULL OUTER")]
    FullOuter,
    Cross,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqlCols, define_id};
    use sqlx::FromRow;

    define_id!(TestId);

    #[derive(Debug, FromRow, SqlCols)]
    struct TestTable {
        id: TestId,
        name: String,
        age: i32,
    }

    impl Table for TestTable {
        type Col = TestTableCol;
        type Id = TestId;
        const TABLE_NAME: &'static str = "test_table";
        const PRIMARY_KEY: &'static str = "id";
    }

    type Expr = SqlExpr<TestTable>;
    use TestTableCol as TC;

    // --- col only ---

    #[test]
    fn col_only() {
        let e = Expr::empty().col(TC::Name);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name"#);
    }

    #[test]
    fn col_only_different_columns() {
        assert_eq!(Expr::empty().col(TC::Id).eval().unwrap().0, r#""test_table".id"#);
        assert_eq!(Expr::empty().col(TC::Age).eval().unwrap().0, r#""test_table".age"#);
    }

    // --- col + col_fn ---

    #[test]
    fn col_with_count() {
        let e = Expr::empty().col(TC::Id).col_fn(SqlFn::Count);
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".id)"#);
    }

    #[test]
    fn col_with_lower() {
        let e = Expr::empty().col(TC::Name).col_fn(SqlFn::Lower);
        assert_eq!(e.eval().unwrap().0, r#"LOWER("test_table".name)"#);
    }

    // --- col + alias ---

    #[test]
    fn col_with_alias() {
        let e = Expr::empty().col(TC::Name).alias("full_name");
        assert_eq!(e.eval().unwrap().0, r#""test_table".name AS full_name"#);
    }

    // --- col + col_fn + alias ---

    #[test]
    fn col_fn_with_alias() {
        let e = Expr::empty().col(TC::Id).col_fn(SqlFn::Count).alias("total");
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".id) AS total"#);
    }

    // --- col + op + val (base case) ---

    #[test]
    fn col_eq_val() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::Eq).val(SqlParam::String("alice".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = $#"#);
    }

    #[test]
    fn col_neq_val() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::Neq).val(SqlParam::String("bob".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name != $#"#);
    }

    #[test]
    fn col_in_val() {
        let e = Expr::empty().col(TC::Id).op(SqlOp::In).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".id IN $#"#);
    }

    #[test]
    fn col_not_in_val() {
        let e = Expr::empty().col(TC::Id).op(SqlOp::NotIn).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".id NOT IN $#"#);
    }

    // --- math operators ---

    #[test]
    fn col_add_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Add).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age + $#"#);
    }

    #[test]
    fn col_sub_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Sub).val(SqlParam::I32(5));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age - $#"#);
    }

    #[test]
    fn col_mul_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Mul).val(SqlParam::I32(2));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age * $#"#);
    }

    #[test]
    fn col_div_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Div).val(SqlParam::I32(3));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age / $#"#);
    }

    // --- IS NULL / IS NOT NULL ---

    #[test]
    fn col_is_null() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::IsNull);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name IS NULL"#);
    }

    #[test]
    fn col_is_not_null() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::IsNotNull);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name IS NOT NULL"#);
    }

    #[test]
    fn col_is_null_ignores_val() {
        let e =
            Expr::empty().col(TC::Name).op(SqlOp::IsNull).val(SqlParam::String("ignored".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name IS NULL"#);
    }

    // --- val_fn variants ---

    #[test]
    fn col_eq_val_fn_lower() {
        let e = Expr::empty()
            .col(TC::Name)
            .op(SqlOp::Eq)
            .val(SqlParam::String("alice".into()))
            .val_fn(SqlFn::Lower);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = LOWER($#)"#);
    }

    #[test]
    fn col_eq_val_fn_now() {
        let e = Expr::empty()
            .col(TC::Name)
            .op(SqlOp::Eq)
            .val(SqlParam::String("placeholder".into()))
            .val_fn(SqlFn::Now);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = NOW()"#);
    }

    #[test]
    fn col_eq_val_fn_true() {
        let e =
            Expr::empty().col(TC::Name).op(SqlOp::Eq).val(SqlParam::Bool(true)).val_fn(SqlFn::True);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = TRUE"#);
    }

    #[test]
    fn col_eq_val_fn_false() {
        let e = Expr::empty()
            .col(TC::Name)
            .op(SqlOp::Eq)
            .val(SqlParam::Bool(false))
            .val_fn(SqlFn::False);
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = FALSE"#);
    }

    #[test]
    fn val_fn_only_no_col() {
        let e = Expr::empty().val_fn(SqlFn::Now);
        assert_eq!(e.eval().unwrap().0, "NOW()");
    }

    #[test]
    fn val_fn_true_no_col() {
        let e = Expr::empty().val_fn(SqlFn::True);
        assert_eq!(e.eval().unwrap().0, "TRUE");
    }

    // --- col_fn + op + val combinations ---

    #[test]
    fn col_fn_count_eq_val() {
        let e = Expr::empty().col(TC::Id).col_fn(SqlFn::Count).op(SqlOp::Eq).val(SqlParam::I32(10));
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".id) = $#"#);
    }

    #[test]
    fn col_fn_lower_eq_val_fn_lower() {
        let e = Expr::empty()
            .col(TC::Name)
            .col_fn(SqlFn::Lower)
            .op(SqlOp::Eq)
            .val(SqlParam::String("alice".into()))
            .val_fn(SqlFn::Lower);
        assert_eq!(e.eval().unwrap().0, r#"LOWER("test_table".name) = LOWER($#)"#);
    }

    // --- col_fn + op + val + alias ---

    #[test]
    fn full_chain_with_alias() {
        let e = Expr::empty()
            .col(TC::Age)
            .col_fn(SqlFn::Count)
            .op(SqlOp::Eq)
            .val(SqlParam::I32(5))
            .alias("age_count");
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".age) = $# AS age_count"#);
    }

    // --- eq helper ---

    #[test]
    fn eq_helper() {
        let (sql, binds) = Expr::eq(TC::Name, "alice").eval().unwrap();
        assert_eq!(sql, r#""test_table".name = $#"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn eq_helper_with_i32() {
        let (sql, binds) = Expr::eq(TC::Age, 42i32).eval().unwrap();
        assert_eq!(sql, r#""test_table".age = $#"#);
        assert_eq!(binds, vec![SqlParam::I32(42)]);
    }

    // --- edge cases ---

    #[test]
    fn no_fields_set() {
        let e = Expr::empty();
        assert_eq!(e.eval().unwrap().0, "");
    }

    #[test]
    fn val_only_no_col_no_op() {
        let e = Expr::empty().val(SqlParam::I32(42));
        assert_eq!(e.eval().unwrap().0, "$#");
    }

    #[test]
    fn alias_on_val_only() {
        let e = Expr::empty().val(SqlParam::I32(1)).alias("constant");
        assert_eq!(e.eval().unwrap().0, "$# AS constant");
    }

    #[test]
    fn alias_on_val_fn_only() {
        let e = Expr::empty().val_fn(SqlFn::Now).alias("current_time");
        assert_eq!(e.eval().unwrap().0, "NOW() AS current_time");
    }

    #[test]
    fn is_null_with_col_fn() {
        let e = Expr::empty().col(TC::Name).col_fn(SqlFn::Lower).op(SqlOp::IsNull);
        assert_eq!(e.eval().unwrap().0, r#"LOWER("test_table".name) IS NULL"#);
    }

    #[test]
    fn is_not_null_with_alias() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::IsNotNull).alias("check");
        assert_eq!(e.eval().unwrap().0, r#""test_table".name IS NOT NULL AS check"#);
    }

    #[test]
    fn err_and_or_both_set() {
        let e = Expr::eq(TC::Name, "a").and(Expr::eq(TC::Age, 1i32)).or(Expr::eq(TC::Age, 2i32));
        assert!(matches!(e.eval(), Err(SqlQueryError::AndOrBothSet)));
    }

    #[test]
    fn err_between_missing_bounds() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Between).val(SqlParam::I32(1));
        assert!(matches!(e.eval(), Err(SqlQueryError::BetweenMissingBounds)));
    }

    #[test]
    fn err_between_missing_both() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Between);
        assert!(matches!(e.eval(), Err(SqlQueryError::BetweenMissingBounds)));
    }

    #[test]
    fn err_exists_missing_select() {
        let e = Expr::empty().op(SqlOp::Exists);
        assert!(matches!(e.eval(), Err(SqlQueryError::ExistsMissingSelect)));
    }

    #[test]
    fn not_simple() {
        let e = Expr::eq(TC::Name, "alice").not();
        assert_eq!(e.eval().unwrap().0, r#"NOT ("test_table".name = $#)"#);
    }

    #[test]
    fn not_with_or() {
        let e = Expr::eq(TC::Name, "alice").or(Expr::eq(TC::Name, "bob")).not();
        assert_eq!(
            e.eval().unwrap().0,
            r#"NOT (("test_table".name = $# OR "test_table".name = $#))"#,
        );
    }

    #[test]
    fn not_with_and() {
        let e = Expr::eq(TC::Name, "alice").and(Expr::eq(TC::Age, 30i32)).not();
        assert_eq!(
            e.eval().unwrap().0,
            r#"NOT (("test_table".name = $# AND "test_table".age = $#))"#,
        );
    }

    #[test]
    fn not_is_null() {
        let e = Expr::is_null(TC::Name).not();
        assert_eq!(e.eval().unwrap().0, r#"NOT ("test_table".name IS NULL)"#);
    }

    #[test]
    fn not_with_alias() {
        let e = Expr::eq(TC::Name, "alice").not().alias("excluded");
        assert_eq!(e.eval().unwrap().0, r#"NOT ("test_table".name = $#) AS excluded"#);
    }

    #[test]
    fn case_when_then_else() {
        let (sql, binds) = Expr::eq(TC::Age, 18i32)
            .then(Expr::empty().val(SqlParam::String("minor".into())))
            .else_(Expr::empty().val(SqlParam::String("adult".into())))
            .eval()
            .unwrap();
        assert_eq!(sql, r#"CASE WHEN "test_table".age = $# THEN $# ELSE $# END"#);
        assert_eq!(
            binds,
            vec![
                SqlParam::I32(18),
                SqlParam::String("minor".into()),
                SqlParam::String("adult".into()),
            ],
        );
    }

    #[test]
    fn case_when_then_else_with_alias() {
        let (sql, _) = Expr::eq(TC::Age, 18i32)
            .then(Expr::empty().val(SqlParam::String("minor".into())))
            .else_(Expr::empty().val(SqlParam::String("adult".into())))
            .alias("age_group")
            .eval()
            .unwrap();
        assert_eq!(sql, r#"CASE WHEN "test_table".age = $# THEN $# ELSE $# END AS age_group"#);
    }

    #[test]
    fn json_get() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::JsonGet).val(SqlParam::String("key".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name -> $#"#);
    }

    #[test]
    fn json_get_text() {
        let e =
            Expr::empty().col(TC::Name).op(SqlOp::JsonGetText).val(SqlParam::String("key".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name ->> $#"#);
    }

    #[test]
    fn json_path() {
        let e =
            Expr::empty().col(TC::Name).op(SqlOp::JsonPath).val(SqlParam::String("{a,b}".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name #> $#"#);
    }

    #[test]
    fn json_path_text() {
        let e = Expr::empty()
            .col(TC::Name)
            .op(SqlOp::JsonPathText)
            .val(SqlParam::String("{a,b}".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name #>> $#"#);
    }

    #[test]
    fn err_then_without_else() {
        let e = Expr::eq(TC::Age, 18i32).then(Expr::empty().val(SqlParam::String("minor".into())));
        assert!(matches!(e.eval(), Err(SqlQueryError::CaseRequiresThenAndElse)));
    }

    #[test]
    fn err_else_without_then() {
        let e = Expr::eq(TC::Age, 18i32).else_(Expr::empty().val(SqlParam::String("adult".into())));
        assert!(matches!(e.eval(), Err(SqlQueryError::CaseRequiresThenAndElse)));
    }
}
