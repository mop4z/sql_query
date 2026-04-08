use std::fmt::Write;

use crate::{
    SqlBase,
    select::SqlSelect,
    shared::{Table, error::SqlQueryError, value::SqlParam},
};

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
        }
    }

    pub fn column(col: T::Col) -> Self {
        Self::empty().col(col)
    }

    pub fn is_null(col: T::Col) -> Self {
        Self::empty().col(col).op(SqlOp::IsNull)
    }

    pub fn eq(col: T::Col, val: impl Into<SqlParam>) -> Self {
        Self::empty().col(col).op(SqlOp::Eq).val(val.into())
    }

    pub fn between(col: T::Col, lo: impl Into<SqlParam>, hi: impl Into<SqlParam>) -> Self {
        let mut e = Self::empty().col(col).op(SqlOp::Between).val(lo.into());
        e.val2 = Some(hi.into());
        e
    }

    pub fn exists(select: SqlSelect) -> Self {
        Self::empty().op(SqlOp::Exists).select(select)
    }

    pub fn not_exists(select: SqlSelect) -> Self {
        Self::empty().op(SqlOp::NotExists).select(select)
    }

    pub fn col(mut self, col: T::Col) -> Self {
        self.col = Some(col);
        self
    }

    pub fn col_fn(mut self, col_fn: SqlFn) -> Self {
        self.col_fn = Some(col_fn);
        self
    }

    pub fn op(mut self, op: SqlOp) -> Self {
        self.op = Some(op);
        self
    }

    pub fn val(mut self, val: SqlParam) -> Self {
        self.val = Some(val);
        self
    }

    pub fn val_fn(mut self, val_fn: SqlFn) -> Self {
        self.val_fn = Some(val_fn);
        self
    }

    pub fn alias(mut self, alias: &'static str) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn select(mut self, select: SqlSelect) -> Self {
        self.select = Some(select);
        self
    }

    pub fn and(mut self, other: SqlExpr<T>) -> Self {
        self.and = Some(Box::new(other));
        self
    }

    pub fn or(mut self, other: SqlExpr<T>) -> Self {
        self.or = Some(Box::new(other));
        self
    }

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
                write!(out, " BETWEEN $1 AND $1").unwrap();
                binds.push(self.val.unwrap());
                binds.push(self.val2.unwrap());
            }
            Some(SqlOp::Any | SqlOp::All) => {
                write!(out, " {} ", self.op.as_ref().unwrap().as_ref()).unwrap();
                write!(out, "($1)").unwrap();
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
                write!(out, "{}($1)", f.as_ref()).unwrap();
                if let Some(v) = val {
                    binds.push(v);
                }
                return;
            }
            None => {
                write!(out, "$1").unwrap();
                if let Some(v) = val {
                    binds.push(v);
                }
                return;
            }
        }
        .unwrap();
    }
}

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
}

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
        }
    }
}

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

#[derive(strum::AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum SqlJoin {
    Left,
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
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = $1"#);
    }

    #[test]
    fn col_neq_val() {
        let e = Expr::empty().col(TC::Name).op(SqlOp::Neq).val(SqlParam::String("bob".into()));
        assert_eq!(e.eval().unwrap().0, r#""test_table".name != $1"#);
    }

    #[test]
    fn col_in_val() {
        let e = Expr::empty().col(TC::Id).op(SqlOp::In).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".id IN $1"#);
    }

    #[test]
    fn col_not_in_val() {
        let e = Expr::empty().col(TC::Id).op(SqlOp::NotIn).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".id NOT IN $1"#);
    }

    // --- math operators ---

    #[test]
    fn col_add_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Add).val(SqlParam::I32(1));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age + $1"#);
    }

    #[test]
    fn col_sub_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Sub).val(SqlParam::I32(5));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age - $1"#);
    }

    #[test]
    fn col_mul_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Mul).val(SqlParam::I32(2));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age * $1"#);
    }

    #[test]
    fn col_div_val() {
        let e = Expr::empty().col(TC::Age).op(SqlOp::Div).val(SqlParam::I32(3));
        assert_eq!(e.eval().unwrap().0, r#""test_table".age / $1"#);
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
        assert_eq!(e.eval().unwrap().0, r#""test_table".name = LOWER($1)"#);
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
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".id) = $1"#);
    }

    #[test]
    fn col_fn_lower_eq_val_fn_lower() {
        let e = Expr::empty()
            .col(TC::Name)
            .col_fn(SqlFn::Lower)
            .op(SqlOp::Eq)
            .val(SqlParam::String("alice".into()))
            .val_fn(SqlFn::Lower);
        assert_eq!(e.eval().unwrap().0, r#"LOWER("test_table".name) = LOWER($1)"#);
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
        assert_eq!(e.eval().unwrap().0, r#"COUNT("test_table".age) = $1 AS age_count"#);
    }

    // --- eq helper ---

    #[test]
    fn eq_helper() {
        let (sql, binds) = Expr::eq(TC::Name, "alice").eval().unwrap();
        assert_eq!(sql, r#""test_table".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn eq_helper_with_i32() {
        let (sql, binds) = Expr::eq(TC::Age, 42i32).eval().unwrap();
        assert_eq!(sql, r#""test_table".age = $1"#);
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
        assert_eq!(e.eval().unwrap().0, "$1");
    }

    #[test]
    fn alias_on_val_only() {
        let e = Expr::empty().val(SqlParam::I32(1)).alias("constant");
        assert_eq!(e.eval().unwrap().0, "$1 AS constant");
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
}
