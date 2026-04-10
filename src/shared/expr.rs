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

#[cfg(test)]
#[path = "expr_tests.rs"]
mod tests;
