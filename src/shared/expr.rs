use std::fmt::Display;
use std::marker::PhantomData;

use smallvec::SmallVec;

use crate::{
    SqlBase,
    select::SqlSelect,
    shared::{Table, error::SqlQueryError, value::SqlParam},
};

// ---------------------------------------------------------------------------
// EvalExpr — trait for any expression type that can be evaluated to SQL
// ---------------------------------------------------------------------------

/// Types that can be evaluated into a SQL string and bound parameters.
pub trait EvalExpr {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError>;
}

// ---------------------------------------------------------------------------
// Internal buffer shared by Expr and CASE WHEN typestates
// ---------------------------------------------------------------------------

struct ExprBuf<T: Table> {
    buf: String,
    binds: SmallVec<[SqlParam; 2]>,
    _t: PhantomData<T>,
}

impl<T: Table> ExprBuf<T> {
    fn new() -> Self {
        Self { buf: String::with_capacity(64), binds: SmallVec::new(), _t: PhantomData }
    }

    fn push(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    fn push_col(&mut self, col: T::Col) {
        self.push_col_of::<T>(col);
    }

    fn push_col_of<U: Table>(&mut self, col: U::Col) {
        self.buf.push('"');
        self.buf.push_str(U::TABLE_NAME);
        self.buf.push_str("\".");
        self.buf.push_str(col.as_ref());
    }

    fn push_eval(&mut self, e: impl EvalExpr) {
        let (sql, binds) = e.eval().unwrap();
        self.buf.push_str(&sql);
        self.binds.extend(binds);
    }

    fn wrap_fn(&mut self, name: &str) {
        let mut new_buf = String::with_capacity(name.len() + 1 + self.buf.len() + 1);
        new_buf.push_str(name);
        new_buf.push('(');
        new_buf.push_str(&self.buf);
        new_buf.push(')');
        self.buf = new_buf;
    }

    fn wrap_fn_expr(&mut self, name: &str, expr_sql: &str, expr_binds: Vec<SqlParam>) {
        let mut new_buf =
            String::with_capacity(name.len() + 1 + self.buf.len() + 2 + expr_sql.len() + 1);
        new_buf.push_str(name);
        new_buf.push('(');
        new_buf.push_str(&self.buf);
        new_buf.push_str(", ");
        new_buf.push_str(expr_sql);
        new_buf.push(')');
        self.buf = new_buf;
        self.binds.extend(expr_binds);
    }

    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        Ok((self.buf, self.binds.into_vec()))
    }
}

// ---------------------------------------------------------------------------
// Expr<T> — the single expression type
// ---------------------------------------------------------------------------

pub struct Expr<T: Table>(ExprBuf<T>);

impl<T: Table> Expr<T> {
    pub fn new() -> Self {
        Self(ExprBuf::new())
    }

    // -- column references ---------------------------------------------------

    pub fn column(mut self, col: T::Col) -> Self {
        self.0.push_col(col);
        self
    }

    pub fn column_of<U: Table>(mut self, col: U::Col) -> Self {
        self.0.push_col_of::<U>(col);
        self
    }

    // -- values / literals ---------------------------------------------------

    pub fn val(mut self, v: impl EvalExpr) -> Self {
        self.0.push_eval(v);
        self
    }

    pub fn now(mut self) -> Self {
        self.0.push("NOW()");
        self
    }

    pub fn null(mut self) -> Self {
        self.0.push("NULL");
        self
    }

    pub fn true_(mut self) -> Self {
        self.0.push("TRUE");
        self
    }

    pub fn false_(mut self) -> Self {
        self.0.push("FALSE");
        self
    }

    pub fn raw(mut self, s: &str) -> Self {
        self.0.push(s);
        self
    }

    // -- comparison operators ------------------------------------------------

    pub fn eq(mut self) -> Self {
        self.0.push(" = ");
        self
    }

    pub fn neq(mut self) -> Self {
        self.0.push(" != ");
        self
    }

    pub fn gt(mut self) -> Self {
        self.0.push(" > ");
        self
    }

    pub fn gte(mut self) -> Self {
        self.0.push(" >= ");
        self
    }

    pub fn lt(mut self) -> Self {
        self.0.push(" < ");
        self
    }

    pub fn lte(mut self) -> Self {
        self.0.push(" <= ");
        self
    }

    // -- arithmetic operators ------------------------------------------------

    pub fn add(mut self) -> Self {
        self.0.push(" + ");
        self
    }

    pub fn sub(mut self) -> Self {
        self.0.push(" - ");
        self
    }

    pub fn mul(mut self) -> Self {
        self.0.push(" * ");
        self
    }

    pub fn div(mut self) -> Self {
        self.0.push(" / ");
        self
    }

    // -- set operators -------------------------------------------------------

    pub fn in_(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" IN (");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    pub fn not_in(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" NOT IN (");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    // -- any / all -----------------------------------------------------------

    pub fn any(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" = ANY(");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    pub fn all(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" = ALL(");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    // -- subquery set ops ----------------------------------------------------

    pub fn in_select(mut self, q: SqlSelect) -> Self {
        self.0.push(" IN ");
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        self.0.buf.push('(');
        self.0.buf.push_str(&sub_sql);
        self.0.buf.push(')');
        self.0.binds.extend(sub_binds);
        self
    }

    pub fn not_in_select(mut self, q: SqlSelect) -> Self {
        self.0.push(" NOT IN ");
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        self.0.buf.push('(');
        self.0.buf.push_str(&sub_sql);
        self.0.buf.push(')');
        self.0.binds.extend(sub_binds);
        self
    }

    // -- pattern matching ----------------------------------------------------

    pub fn like(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" LIKE ");
        self.0.push_eval(v);
        self
    }

    pub fn ilike(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" ILIKE ");
        self.0.push_eval(v);
        self
    }

    // -- null checks ---------------------------------------------------------

    pub fn is_null(mut self) -> Self {
        self.0.push(" IS NULL");
        self
    }

    pub fn is_not_null(mut self) -> Self {
        self.0.push(" IS NOT NULL");
        self
    }

    // -- range ---------------------------------------------------------------

    pub fn between(mut self, lo: impl EvalExpr, hi: impl EvalExpr) -> Self {
        self.0.push(" BETWEEN ");
        self.0.push_eval(lo);
        self.0.push(" AND ");
        self.0.push_eval(hi);
        self
    }

    // -- aggregate / scalar wraps -------------------------------------------

    pub fn count(mut self) -> Self {
        self.0.wrap_fn("COUNT");
        self
    }

    pub fn sum(mut self) -> Self {
        self.0.wrap_fn("SUM");
        self
    }

    pub fn avg(mut self) -> Self {
        self.0.wrap_fn("AVG");
        self
    }

    pub fn min(mut self) -> Self {
        self.0.wrap_fn("MIN");
        self
    }

    pub fn max(mut self) -> Self {
        self.0.wrap_fn("MAX");
        self
    }

    pub fn lower(mut self) -> Self {
        self.0.wrap_fn("LOWER");
        self
    }

    pub fn upper(mut self) -> Self {
        self.0.wrap_fn("UPPER");
        self
    }

    pub fn abs(mut self) -> Self {
        self.0.wrap_fn("ABS");
        self
    }

    pub fn unnest(mut self) -> Self {
        self.0.wrap_fn("UNNEST");
        self
    }

    pub fn date(mut self) -> Self {
        self.0.wrap_fn("DATE");
        self
    }

    pub fn greatest(mut self, other: impl EvalExpr) -> Self {
        let (sql, binds) = other.eval().unwrap();
        self.0.wrap_fn_expr("GREATEST", &sql, binds);
        self
    }

    pub fn least(mut self, other: impl EvalExpr) -> Self {
        let (sql, binds) = other.eval().unwrap();
        self.0.wrap_fn_expr("LEAST", &sql, binds);
        self
    }

    // -- json ----------------------------------------------------------------

    pub fn json_get(mut self, key: impl EvalExpr) -> Self {
        self.0.push(" -> ");
        self.0.push_eval(key);
        self
    }

    pub fn json_get_text(mut self, key: impl EvalExpr) -> Self {
        self.0.push(" ->> ");
        self.0.push_eval(key);
        self
    }

    pub fn jsonb_text_eq(mut self, key: impl EvalExpr, val: impl EvalExpr) -> Self {
        self.0.push(" ->> ");
        self.0.push_eval(key);
        self.0.push(" = ");
        self.0.push_eval(val);
        self
    }

    pub fn json_path(mut self, path: impl EvalExpr) -> Self {
        self.0.push(" #> ");
        self.0.push_eval(path);
        self
    }

    pub fn json_path_text(mut self, path: impl EvalExpr) -> Self {
        self.0.push(" #>> ");
        self.0.push_eval(path);
        self
    }

    // -- coalesce ------------------------------------------------------------

    pub fn coalesce(mut self, fallback: impl EvalExpr) -> Self {
        self.0.buf.insert_str(0, "COALESCE(");
        self.0.push(", ");
        self.0.push_eval(fallback);
        self.0.push(")");
        self
    }

    // -- string functions ----------------------------------------------------

    pub fn concat(mut self) -> Self {
        self.0.wrap_fn("CONCAT");
        self
    }

    pub fn length(mut self) -> Self {
        self.0.wrap_fn("LENGTH");
        self
    }

    pub fn trim(mut self) -> Self {
        self.0.wrap_fn("TRIM");
        self
    }

    pub fn substring(mut self) -> Self {
        self.0.wrap_fn("SUBSTRING");
        self
    }

    // -- cast / wrap ---------------------------------------------------------

    pub fn cast(mut self, ty: &str) -> Self {
        self.0.buf.push_str("::");
        self.0.buf.push_str(ty);
        self
    }

    /// Wrap the buffer with an arbitrary function: `name(buf)`.
    pub fn wrap_raw(mut self, name: &str) -> Self {
        self.0.wrap_fn(name);
        self
    }

    // -- alias ---------------------------------------------------------------

    pub fn alias(mut self, name: &str) -> Self {
        self.0.buf.push_str(" AS ");
        self.0.buf.push_str(name);
        self
    }

    // -- logical operators ---------------------------------------------------

    pub fn and(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") AND ");
        self
    }

    pub fn or(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") OR ");
        self
    }

    pub fn and_bare(mut self) -> Self {
        self.0.push(" AND ");
        self
    }

    pub fn or_bare(mut self) -> Self {
        self.0.push(" OR ");
        self
    }

    pub fn not(mut self) -> Self {
        self.0.buf.insert_str(0, "NOT ");
        self
    }

    pub fn paren(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(")");
        self
    }

    // -- function call -------------------------------------------------------

    pub fn func(mut self, name: &str, prefix: &str, v: impl EvalExpr) -> Self {
        self.0.buf.push_str(name);
        self.0.buf.push('(');
        self.0.buf.push_str(prefix);
        self.0.push_eval(v);
        self.0.buf.push(')');
        self
    }

    // -- splice expression ---------------------------------------------------

    pub fn expr(mut self, e: impl Into<Expr<T>>) -> Self {
        let e: Expr<T> = e.into();
        let (sql, binds) = e.0.eval().unwrap();
        self.0.buf.push_str(&sql);
        self.0.binds.extend(binds);
        self
    }

    // -- subqueries ----------------------------------------------------------

    pub fn exists(self, q: SqlSelect) -> Self {
        self.raw("EXISTS ").select(q)
    }

    pub fn not_exists(self, q: SqlSelect) -> Self {
        self.raw("NOT EXISTS ").select(q)
    }

    pub fn select(mut self, q: SqlSelect) -> Self {
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        self.0.buf.push('(');
        self.0.buf.push_str(&sub_sql);
        self.0.buf.push(')');
        self.0.binds.extend(sub_binds);
        self
    }

    // -- CASE WHEN -----------------------------------------------------------

    pub fn if_(mut self, condition: Expr<T>) -> ExprIf<T> {
        let (cond_sql, cond_binds) = condition.0.eval().unwrap();
        self.0.push("CASE WHEN ");
        self.0.push(&cond_sql);
        self.0.binds.extend(cond_binds);
        ExprIf(self.0)
    }

    // -- internal ------------------------------------------------------------

    pub(crate) fn into_col_and_val(self) -> (Option<String>, String, Vec<SqlParam>) {
        let sql = self.0.buf;
        let binds = self.0.binds;
        if let Some(eq_pos) = sql.find(" = ") {
            let col_part = &sql[..eq_pos];
            if let Some(dot_pos) = col_part.rfind('.') {
                let col_name: String = col_part[dot_pos + 1..].into();
                let val_sql: String = sql[eq_pos + 3..].into();
                return (Some(col_name), val_sql, binds.into_vec());
            }
        }
        (None, sql, binds.into_vec())
    }
}

impl<T: Table> EvalExpr for Expr<T> {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        self.0.eval()
    }
}

impl<T: Into<SqlParam>> EvalExpr for T {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        Ok(("$#".to_string(), vec![self.into()]))
    }
}

impl<T: Table> From<SqlParam> for Expr<T> {
    fn from(val: SqlParam) -> Self {
        Self::new().val(val)
    }
}

// ---------------------------------------------------------------------------
// ColOps<T> — shorthand methods for column enums
// ---------------------------------------------------------------------------

/// Shorthand methods available on every column enum derived with `SqlCols`.
pub trait ColOps<T: Table<Col = Self>>: AsRef<str> + Display + Copy {
    fn eq(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).eq().val(val)
    }

    fn neq(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).neq().val(val)
    }

    fn gt(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).gt().val(val)
    }

    fn gte(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).gte().val(val)
    }

    fn lt(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).lt().val(val)
    }

    fn lte(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).lte().val(val)
    }

    fn like(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).like(val)
    }

    fn ilike(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).ilike(val)
    }

    fn in_(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).in_(val)
    }

    fn not_in(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).not_in(val)
    }

    fn between(self, lo: impl EvalExpr, hi: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).between(lo, hi)
    }

    fn in_select(self, select: SqlSelect) -> Expr<T> {
        Expr::new().column(self).in_select(select)
    }

    fn not_in_select(self, select: SqlSelect) -> Expr<T> {
        Expr::new().column(self).not_in_select(select)
    }

    fn is_null(self) -> Expr<T> {
        Expr::new().column(self).is_null()
    }

    fn is_not_null(self) -> Expr<T> {
        Expr::new().column(self).is_not_null()
    }

    fn count(self) -> Expr<T> {
        Expr::new().column(self).count()
    }

    fn sum(self) -> Expr<T> {
        Expr::new().column(self).sum()
    }

    fn avg(self) -> Expr<T> {
        Expr::new().column(self).avg()
    }

    fn min(self) -> Expr<T> {
        Expr::new().column(self).min()
    }

    fn max(self) -> Expr<T> {
        Expr::new().column(self).max()
    }

    fn greatest(self, other: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).greatest(other)
    }

    fn least(self, other: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).least(other)
    }

    fn lower(self) -> Expr<T> {
        Expr::new().column(self).lower()
    }

    fn upper(self) -> Expr<T> {
        Expr::new().column(self).upper()
    }

    fn abs(self) -> Expr<T> {
        Expr::new().column(self).abs()
    }

    fn date(self) -> Expr<T> {
        Expr::new().column(self).date()
    }

    fn json_get(self, key: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_get(key)
    }

    fn json_get_text(self, key: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_get_text(key)
    }

    fn any(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).any(val)
    }

    fn jsonb_text_eq(self, key: impl EvalExpr, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).jsonb_text_eq(key, val)
    }

    fn col(self) -> Expr<T> {
        Expr::new().column(self)
    }
}

// ---------------------------------------------------------------------------
// ExprIf<T> / ExprThen<T> — CASE WHEN typestate
// ---------------------------------------------------------------------------

pub struct ExprIf<T: Table>(ExprBuf<T>);

impl<T: Table> ExprIf<T> {
    pub fn then_(mut self, val: impl Into<Expr<T>>) -> ExprThen<T> {
        let expr: Expr<T> = val.into();
        let (sql, binds) = expr.0.eval().unwrap();
        self.0.push(" THEN ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        ExprThen(self.0)
    }
}

pub struct ExprThen<T: Table>(ExprBuf<T>);

impl<T: Table> ExprThen<T> {
    pub fn else_(mut self, val: impl Into<Expr<T>>) -> Expr<T> {
        let expr: Expr<T> = val.into();
        let (sql, binds) = expr.0.eval().unwrap();
        self.0.push(" ELSE ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        self.0.push(" END");
        Expr(self.0)
    }
}

// ---------------------------------------------------------------------------
// Enums used by the query builders
// ---------------------------------------------------------------------------

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
    Inner,
    Left,
    Right,
    #[strum(serialize = "FULL OUTER")]
    FullOuter,
    Cross,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        email: String,
        data: String,
        created_at: String,
    }

    impl Table for TestTable {
        type Col = TestTableCol;
        type Id = TestId;
        const TABLE_NAME: &'static str = "test_table";
        const PRIMARY_KEY: &'static str = "id";
    }

    type TC = TestTableCol;
    type E = Expr<TestTable>;

    fn eval(e: Expr<TestTable>) -> (String, Vec<SqlParam>) {
        e.eval().unwrap()
    }

    // -- Basic column + val --------------------------------------------------

    #[test]
    fn column_eq_val() {
        let (sql, binds) = eval(E::new().column(TC::Name).eq().val("alice"));
        assert_eq!(sql, r#""test_table".name = $#"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn column_neq_val() {
        let (sql, _) = eval(E::new().column(TC::Name).neq().val("bob"));
        assert_eq!(sql, r#""test_table".name != $#"#);
    }

    #[test]
    fn column_gt_val() {
        let (sql, _) = eval(E::new().column(TC::Age).gt().val(SqlParam::I32(18)));
        assert_eq!(sql, r#""test_table".age > $#"#);
    }

    #[test]
    fn column_gte_val() {
        let (sql, _) = eval(E::new().column(TC::Age).gte().val(SqlParam::I32(18)));
        assert_eq!(sql, r#""test_table".age >= $#"#);
    }

    #[test]
    fn column_lt_val() {
        let (sql, _) = eval(E::new().column(TC::Age).lt().val(SqlParam::I32(18)));
        assert_eq!(sql, r#""test_table".age < $#"#);
    }

    #[test]
    fn column_lte_val() {
        let (sql, _) = eval(E::new().column(TC::Age).lte().val(SqlParam::I32(18)));
        assert_eq!(sql, r#""test_table".age <= $#"#);
    }

    // -- Self-referential arithmetic -----------------------------------------

    #[test]
    fn self_ref_add() {
        let (sql, binds) =
            eval(E::new().column(TC::Age).eq().column(TC::Age).add().val(SqlParam::I32(1)));
        assert_eq!(sql, r#""test_table".age = "test_table".age + $#"#);
        assert_eq!(binds, vec![SqlParam::I32(1)]);
    }

    #[test]
    fn self_ref_sub() {
        let (sql, _) =
            eval(E::new().column(TC::Age).eq().column(TC::Age).sub().val(SqlParam::I32(5)));
        assert_eq!(sql, r#""test_table".age = "test_table".age - $#"#);
    }

    #[test]
    fn self_ref_mul() {
        let (sql, _) =
            eval(E::new().column(TC::Age).eq().column(TC::Age).mul().val(SqlParam::I32(2)));
        assert_eq!(sql, r#""test_table".age = "test_table".age * $#"#);
    }

    #[test]
    fn self_ref_div() {
        let (sql, _) =
            eval(E::new().column(TC::Age).eq().column(TC::Age).div().val(SqlParam::I32(3)));
        assert_eq!(sql, r#""test_table".age = "test_table".age / $#"#);
    }

    // -- NOW / NULL / TRUE / FALSE -------------------------------------------

    #[test]
    fn eq_now() {
        let (sql, binds) = eval(E::new().column(TC::CreatedAt).eq().now());
        assert_eq!(sql, r#""test_table".created_at = NOW()"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn eq_null() {
        let (sql, _) = eval(E::new().column(TC::Email).eq().null());
        assert_eq!(sql, r#""test_table".email = NULL"#);
    }

    #[test]
    fn val_true() {
        let (sql, _) = eval(E::new().true_());
        assert_eq!(sql, "TRUE");
    }

    #[test]
    fn val_false() {
        let (sql, _) = eval(E::new().false_());
        assert_eq!(sql, "FALSE");
    }

    // -- Null checks ---------------------------------------------------------

    #[test]
    fn is_null() {
        let (sql, _) = eval(E::new().column(TC::Email).is_null());
        assert_eq!(sql, r#""test_table".email IS NULL"#);
    }

    #[test]
    fn is_not_null() {
        let (sql, _) = eval(E::new().column(TC::Email).is_not_null());
        assert_eq!(sql, r#""test_table".email IS NOT NULL"#);
    }

    // -- IN / NOT IN ---------------------------------------------------------

    #[test]
    fn in_array() {
        let (sql, binds) = eval(E::new().column(TC::Age).in_(SqlParam::I32Array(vec![1, 2, 3])));
        assert_eq!(sql, r#""test_table".age IN ($#)"#);
        assert_eq!(binds, vec![SqlParam::I32Array(vec![1, 2, 3])]);
    }

    #[test]
    fn not_in_array() {
        let (sql, _) = eval(E::new().column(TC::Age).not_in(SqlParam::I32Array(vec![1])));
        assert_eq!(sql, r#""test_table".age NOT IN ($#)"#);
    }

    // -- LIKE / ILIKE --------------------------------------------------------

    #[test]
    fn like_pattern() {
        let (sql, _) = eval(E::new().column(TC::Name).like("%alice%"));
        assert_eq!(sql, r#""test_table".name LIKE $#"#);
    }

    #[test]
    fn ilike_pattern() {
        let (sql, _) = eval(E::new().column(TC::Name).ilike("%alice%"));
        assert_eq!(sql, r#""test_table".name ILIKE $#"#);
    }

    // -- BETWEEN -------------------------------------------------------------

    #[test]
    fn between_range() {
        let (sql, binds) =
            eval(E::new().column(TC::Age).between(SqlParam::I32(10), SqlParam::I32(20)));
        assert_eq!(sql, r#""test_table".age BETWEEN $# AND $#"#);
        assert_eq!(binds, vec![SqlParam::I32(10), SqlParam::I32(20)]);
    }

    // -- Aggregate wraps -----------------------------------------------------

    #[test]
    fn count_col() {
        let (sql, _) = eval(E::new().column(TC::Age).count());
        assert_eq!(sql, r#"COUNT("test_table".age)"#);
    }

    #[test]
    fn sum_col() {
        let (sql, _) = eval(E::new().column(TC::Age).sum());
        assert_eq!(sql, r#"SUM("test_table".age)"#);
    }

    #[test]
    fn avg_col() {
        let (sql, _) = eval(E::new().column(TC::Age).avg());
        assert_eq!(sql, r#"AVG("test_table".age)"#);
    }

    #[test]
    fn min_col() {
        let (sql, _) = eval(E::new().column(TC::Age).min());
        assert_eq!(sql, r#"MIN("test_table".age)"#);
    }

    #[test]
    fn max_col() {
        let (sql, _) = eval(E::new().column(TC::Age).max());
        assert_eq!(sql, r#"MAX("test_table".age)"#);
    }

    #[test]
    fn lower_col() {
        let (sql, _) = eval(E::new().column(TC::Name).lower());
        assert_eq!(sql, r#"LOWER("test_table".name)"#);
    }

    #[test]
    fn upper_col() {
        let (sql, _) = eval(E::new().column(TC::Name).upper());
        assert_eq!(sql, r#"UPPER("test_table".name)"#);
    }

    // -- Alias ---------------------------------------------------------------

    #[test]
    fn column_alias() {
        let (sql, _) = eval(E::new().column(TC::Name).alias("n"));
        assert_eq!(sql, r#""test_table".name AS n"#);
    }

    // -- Raw -----------------------------------------------------------------

    #[test]
    fn raw_pass_through() {
        let (sql, _) = eval(E::new().raw("1 = 1"));
        assert_eq!(sql, "1 = 1");
    }

    // -- AND / OR / NOT ------------------------------------------------------

    #[test]
    fn and_chain() {
        let (sql, _) = eval(
            E::new()
                .column(TC::Name)
                .eq()
                .val("alice")
                .and()
                .column(TC::Age)
                .gt()
                .val(SqlParam::I32(18)),
        );
        assert_eq!(sql, r#"("test_table".name = $#) AND "test_table".age > $#"#);
    }

    #[test]
    fn or_chain() {
        let (sql, _) =
            eval(E::new().column(TC::Name).eq().val("alice").or().column(TC::Name).eq().val("bob"));
        assert_eq!(sql, r#"("test_table".name = $#) OR "test_table".name = $#"#);
    }

    #[test]
    fn and_bare_chain() {
        let (sql, _) = eval(
            E::new()
                .column(TC::Name)
                .eq()
                .val("alice")
                .and_bare()
                .column(TC::Age)
                .gt()
                .val(SqlParam::I32(18)),
        );
        assert_eq!(sql, r#""test_table".name = $# AND "test_table".age > $#"#);
    }

    #[test]
    fn or_bare_chain() {
        let (sql, _) = eval(
            E::new().column(TC::Name).eq().val("alice").or_bare().column(TC::Name).eq().val("bob"),
        );
        assert_eq!(sql, r#""test_table".name = $# OR "test_table".name = $#"#);
    }

    #[test]
    fn not_expr() {
        let (sql, _) = eval(E::new().column(TC::Email).is_null().not());
        assert_eq!(sql, r#"NOT "test_table".email IS NULL"#);
    }

    // -- CASE WHEN / THEN / ELSE ---------------------------------------------

    #[test]
    fn if_then_else() {
        let (sql, binds) = eval(
            E::new()
                .column(TC::Name)
                .eq()
                .if_(E::new().val(SqlParam::Bool(true)))
                .then_(E::new().val(SqlParam::String("yes".into())))
                .else_(E::new().null()),
        );
        assert_eq!(sql, r#""test_table".name = CASE WHEN $# THEN $# ELSE NULL END"#);
        assert_eq!(binds, vec![SqlParam::Bool(true), SqlParam::String("yes".into())]);
    }

    #[test]
    fn if_then_else_with_column() {
        let (sql, binds) = eval(
            E::new()
                .column(TC::CreatedAt)
                .eq()
                .if_(E::new().val(SqlParam::Bool(true)))
                .then_(E::new().column(TC::CreatedAt))
                .else_(E::new().null()),
        );
        assert_eq!(
            sql,
            r#""test_table".created_at = CASE WHEN $# THEN "test_table".created_at ELSE NULL END"#,
        );
        assert_eq!(binds, vec![SqlParam::Bool(true)]);
    }

    // -- JSON ----------------------------------------------------------------

    #[test]
    fn json_get_key() {
        let (sql, _) = eval(E::new().column(TC::Data).json_get("key"));
        assert_eq!(sql, r#""test_table".data -> $#"#);
    }

    #[test]
    fn json_get_text_key() {
        let (sql, _) = eval(E::new().column(TC::Data).json_get_text("key"));
        assert_eq!(sql, r#""test_table".data ->> $#"#);
    }

    // -- ANY / ALL -----------------------------------------------------------

    #[test]
    fn any_val() {
        let (sql, binds) = eval(E::new().column(TC::Age).any(SqlParam::I32Array(vec![1, 2])));
        assert_eq!(sql, r#""test_table".age = ANY($#)"#);
        assert_eq!(binds, vec![SqlParam::I32Array(vec![1, 2])]);
    }

    #[test]
    fn all_val() {
        let (sql, _) = eval(E::new().column(TC::Age).all(SqlParam::I32Array(vec![1])));
        assert_eq!(sql, r#""test_table".age = ALL($#)"#);
    }

    // -- JSONB text eq / JSON path -------------------------------------------

    #[test]
    fn jsonb_text_eq() {
        let (sql, binds) = eval(E::new().column(TC::Data).jsonb_text_eq("key", "val"));
        assert_eq!(sql, r#""test_table".data ->> $# = $#"#);
        assert_eq!(binds, vec![SqlParam::String("key".into()), SqlParam::String("val".into())]);
    }

    #[test]
    fn json_path() {
        let (sql, _) = eval(E::new().column(TC::Data).json_path("{a,b}"));
        assert_eq!(sql, r#""test_table".data #> $#"#);
    }

    #[test]
    fn json_path_text() {
        let (sql, _) = eval(E::new().column(TC::Data).json_path_text("{a,b}"));
        assert_eq!(sql, r#""test_table".data #>> $#"#);
    }

    // -- COALESCE ------------------------------------------------------------

    #[test]
    fn coalesce_col() {
        let (sql, binds) = eval(E::new().column(TC::Age).coalesce(SqlParam::I32(0)));
        assert_eq!(sql, r#"COALESCE("test_table".age, $#)"#);
        assert_eq!(binds, vec![SqlParam::I32(0)]);
    }

    // -- EXISTS / NOT EXISTS -------------------------------------------------

    #[test]
    fn exists_subquery() {
        use crate::SqlSelect;
        let sub = SqlSelect::new::<TestTable>();
        let (sql, _) = eval(E::new().exists(sub));
        assert!(sql.starts_with("EXISTS ("));
    }

    #[test]
    fn not_exists_subquery() {
        use crate::SqlSelect;
        let sub = SqlSelect::new::<TestTable>();
        let (sql, _) = eval(E::new().not_exists(sub));
        assert!(sql.starts_with("NOT EXISTS ("));
    }

    // -- into_col_and_val ----------------------------------------------------

    #[test]
    fn into_col_and_val_splits() {
        let e = E::new().column(TC::Name).eq().val("alice");
        let (col, val_sql, binds) = e.into_col_and_val();
        assert_eq!(col, Some("name".to_string()));
        assert_eq!(val_sql, "$#");
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn into_col_and_val_no_col() {
        let e = E::new().val(SqlParam::I32(1));
        let (col, val_sql, binds) = e.into_col_and_val();
        assert_eq!(col, None);
        assert_eq!(val_sql, "$#");
        assert_eq!(binds, vec![SqlParam::I32(1)]);
    }

    // -- CAST ----------------------------------------------------------------

    #[test]
    fn cast_on_expr() {
        let (sql, _) = eval(E::new().val(SqlParam::I32(1)).cast("text"));
        assert_eq!(sql, "$#::text");
    }

    #[test]
    fn cast_on_col() {
        let (sql, _) = eval(E::new().column(TC::Age).cast("text"));
        assert_eq!(sql, r#""test_table".age::text"#);
    }

    // -- PAREN ---------------------------------------------------------------

    #[test]
    fn paren_wrap() {
        let (sql, _) = eval(
            E::new()
                .column(TC::Name)
                .eq()
                .val("a")
                .or_bare()
                .column(TC::Name)
                .eq()
                .val("b")
                .paren(),
        );
        assert_eq!(sql, r#"("test_table".name = $# OR "test_table".name = $#)"#);
    }

    // -- WRAP_RAW ------------------------------------------------------------

    #[test]
    fn wrap_raw_on_expr() {
        let (sql, _) = eval(E::new().val(SqlParam::I32(1)).wrap_raw("MY_FUNC"));
        assert_eq!(sql, "MY_FUNC($#)");
    }

    #[test]
    fn wrap_raw_on_col() {
        let (sql, _) = eval(E::new().column(TC::Name).wrap_raw("UNACCENT"));
        assert_eq!(sql, r#"UNACCENT("test_table".name)"#);
    }

    // -- String functions ----------------------------------------------------

    #[test]
    fn concat_col() {
        let (sql, _) = eval(E::new().column(TC::Name).concat());
        assert_eq!(sql, r#"CONCAT("test_table".name)"#);
    }

    #[test]
    fn length_col() {
        let (sql, _) = eval(E::new().column(TC::Name).length());
        assert_eq!(sql, r#"LENGTH("test_table".name)"#);
    }

    #[test]
    fn trim_col() {
        let (sql, _) = eval(E::new().column(TC::Name).trim());
        assert_eq!(sql, r#"TRIM("test_table".name)"#);
    }

    #[test]
    fn substring_col() {
        let (sql, _) = eval(E::new().column(TC::Name).substring());
        assert_eq!(sql, r#"SUBSTRING("test_table".name)"#);
    }

    // -- GREATEST / LEAST ----------------------------------------------------

    #[test]
    fn greatest_val() {
        let (sql, binds) = eval(E::new().val(SqlParam::I32(1)).greatest(SqlParam::I32(2)));
        assert_eq!(sql, "GREATEST($#, $#)");
        assert_eq!(binds, vec![SqlParam::I32(1), SqlParam::I32(2)]);
    }

    #[test]
    fn greatest_col() {
        let (sql, binds) = eval(TC::CreatedAt.greatest(SqlParam::String("ts".into())));
        assert_eq!(sql, r#"GREATEST("test_table".created_at, $#)"#);
        assert_eq!(binds, vec![SqlParam::String("ts".into())]);
    }

    #[test]
    fn least_val() {
        let (sql, binds) = eval(E::new().val(SqlParam::I32(10)).least(SqlParam::I32(20)));
        assert_eq!(sql, "LEAST($#, $#)");
        assert_eq!(binds, vec![SqlParam::I32(10), SqlParam::I32(20)]);
    }

    #[test]
    fn greatest_in_case_when() {
        let (sql, binds) = eval(
            E::new()
                .column(TC::CreatedAt)
                .eq()
                .if_(E::new().val(SqlParam::Bool(true)))
                .then_(TC::CreatedAt.greatest(SqlParam::String("ts".into())))
                .else_(E::new().null()),
        );
        assert_eq!(
            sql,
            r#""test_table".created_at = CASE WHEN $# THEN GREATEST("test_table".created_at, $#) ELSE NULL END"#,
        );
        assert_eq!(binds, vec![SqlParam::Bool(true), SqlParam::String("ts".into())]);
    }

    // -- From<SqlParam> ------------------------------------------------------

    #[test]
    fn from_sql_param() {
        let e: E = SqlParam::I32(42).into();
        let (sql, binds) = eval(e);
        assert_eq!(sql, "$#");
        assert_eq!(binds, vec![SqlParam::I32(42)]);
    }

    // -- expr() --------------------------------------------------------------

    #[test]
    fn expr_splices_inner() {
        let inner = E::new().column(TC::Name).greatest(SqlParam::I32(24));
        let (sql, binds) = eval(E::new().column(TC::Name).eq().expr(inner));
        assert_eq!(sql, r#""test_table".name = GREATEST("test_table".name, $#)"#);
        assert_eq!(binds, vec![SqlParam::I32(24)]);
    }

    // -- func() --------------------------------------------------------------

    #[test]
    fn func_on_expr() {
        let (sql, binds) = eval(E::new().func("make_interval", "hours => ", 24i32));
        assert_eq!(sql, "make_interval(hours => $#)");
        assert_eq!(binds, vec![SqlParam::I32(24)]);
    }

    #[test]
    fn func_after_eq() {
        let (sql, binds) =
            eval(E::new().column(TC::Age).eq().func("make_interval", "hours => ", 5i32));
        assert_eq!(sql, r#""test_table".age = make_interval(hours => $#)"#);
        assert_eq!(binds, vec![SqlParam::I32(5)]);
    }
}
