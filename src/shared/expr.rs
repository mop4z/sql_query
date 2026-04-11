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

/// Anything that can produce a SQL fragment and bound parameters.
///
/// Implemented automatically for all `Into<SqlParam>` types (scalars produce `$#`
/// with one bind), for `Expr<T>` (produces its accumulated SQL), and for
/// derive-generated column enums (produce `"table".col`).
///
/// This is the primary parameter bound throughout the expression API — methods
/// like `.val()`, `.eq()` on `ColOps`, `.in_()`, etc. all accept `impl EvalExpr`,
/// so you can pass plain values, column references, or full sub-expressions.
pub trait EvalExpr {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError>;
}

// ---------------------------------------------------------------------------
// Internal buffer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
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

/// A SQL expression bound to table `T`.
///
/// All expression building flows through this type. Chain methods to
/// incrementally build SQL, then pass to query builder methods like
/// `.filter()`, `.set()`, or `.from()` which call `.eval()` internally.
///
/// ```ignore
/// type E = Expr<Users>;
///
/// // col = val
/// E::new().column(UC::Name).eq().val("alice")
///
/// // col = col + val  (self-referential arithmetic)
/// E::new().column(UC::Balance).eq().column(UC::Balance).add().val(amount)
///
/// // CASE WHEN ... THEN ... ELSE ... END
/// E::new().column(UC::Status).eq()
///     .if_(E::new().val(is_active))
///     .then_(E::new().raw("'active'"))
///     .else_(E::new().raw("'inactive'"))
/// ```
#[derive(Debug, Clone)]
pub struct Expr<T: Table>(ExprBuf<T>);

impl<T: Table> Expr<T> {
    /// Start a new empty expression.
    pub fn new() -> Self {
        Self(ExprBuf::new())
    }

    // -- column references ---------------------------------------------------

    /// Append a qualified column: `"table".col`.
    pub fn column(mut self, col: T::Col) -> Self {
        self.0.push_col(col);
        self
    }

    /// Append a qualified column from a different table: `"other_table".col`.
    ///
    /// Useful in UPDATE ... SET or JOIN conditions that reference across tables.
    pub fn column_of<U: Table>(mut self, col: U::Col) -> Self {
        self.0.push_col_of::<U>(col);
        self
    }

    /// Reinterpret this expression as bound to table `U`. The SQL and binds are
    /// preserved verbatim — only the phantom table changes. Lets an expression
    /// built against one table be spliced into a query against another.
    pub fn coerce<U: Table>(self) -> Expr<U> {
        Expr(ExprBuf { buf: self.0.buf, binds: self.0.binds, _t: PhantomData })
    }

    // -- values / literals ---------------------------------------------------

    /// Append a value or expression. Scalars become `$#` with a bind parameter;
    /// expressions splice their SQL and binds directly.
    pub fn val(mut self, v: impl EvalExpr) -> Self {
        self.0.push_eval(v);
        self
    }

    /// Start an expression with `*` — for use with aggregates like `COUNT(*)`.
    pub fn star() -> Self {
        Self::new().raw("*")
    }

    /// Append `NOW()` — the current timestamp.
    pub fn now(mut self) -> Self {
        self.0.push("NOW()");
        self
    }

    /// Append `NULL`.
    pub fn null(mut self) -> Self {
        self.0.push("NULL");
        self
    }

    /// Append `TRUE`.
    pub fn true_(mut self) -> Self {
        self.0.push("TRUE");
        self
    }

    /// Append `FALSE`.
    pub fn false_(mut self) -> Self {
        self.0.push("FALSE");
        self
    }

    /// Append raw SQL verbatim. No escaping or parameterisation.
    pub fn raw(mut self, s: &str) -> Self {
        self.0.push(s);
        self
    }

    // -- comparison operators ------------------------------------------------

    /// Append ` = val`.
    pub fn eq(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" = ");
        self.0.push_eval(val);
        self
    }

    /// Append ` != val`.
    pub fn neq(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" != ");
        self.0.push_eval(val);
        self
    }

    /// Append ` > val`.
    pub fn gt(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" > ");
        self.0.push_eval(val);
        self
    }

    /// Append ` >= val`.
    pub fn gte(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" >= ");
        self.0.push_eval(val);
        self
    }

    /// Append ` < val`.
    pub fn lt(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" < ");
        self.0.push_eval(val);
        self
    }

    /// Append ` <= val`.
    pub fn lte(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" <= ");
        self.0.push_eval(val);
        self
    }

    // -- arithmetic operators ------------------------------------------------

    /// Append ` + val`.
    pub fn add(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" + ");
        self.0.push_eval(val);
        self
    }

    /// Append ` - val`.
    pub fn sub(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" - ");
        self.0.push_eval(val);
        self
    }

    /// Append ` * val`.
    pub fn mul(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" * ");
        self.0.push_eval(val);
        self
    }

    /// Append ` / val`.
    pub fn div(mut self, val: impl EvalExpr) -> Self {
        self.0.push(" / ");
        self.0.push_eval(val);
        self
    }

    // -- array operators -----------------------------------------------------

    /// Append ` && ` — Postgres array overlap operator.
    pub fn overlap(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" && ");
        self.0.push_eval(v);
        self
    }

    // -- set operators -------------------------------------------------------

    /// Append ` IN ($1)` for a scalar, or ` = ANY($1)` when `v` evaluates to a
    /// single array bind — Postgres rejects `col IN (array)` (it expects a
    /// tuple), so array inputs are rewritten into the equivalent ANY form.
    pub fn in_(mut self, v: impl EvalExpr) -> Self {
        let (sql, binds) = v.eval().unwrap();
        if binds.len() == 1 && binds[0].is_array() {
            self.0.push(" = ANY(");
            self.0.push(&sql);
            self.0.push(")");
        } else {
            self.0.push(" IN (");
            self.0.push(&sql);
            self.0.push(")");
        }
        self.0.binds.extend(binds);
        self
    }

    /// Append ` NOT IN ($1)` for a scalar, or ` <> ALL($1)` when `v` evaluates
    /// to a single array bind.
    pub fn not_in(mut self, v: impl EvalExpr) -> Self {
        let (sql, binds) = v.eval().unwrap();
        if binds.len() == 1 && binds[0].is_array() {
            self.0.push(" <> ALL(");
            self.0.push(&sql);
            self.0.push(")");
        } else {
            self.0.push(" NOT IN (");
            self.0.push(&sql);
            self.0.push(")");
        }
        self.0.binds.extend(binds);
        self
    }

    // -- any / all -----------------------------------------------------------

    /// Append ` = ANY($1)`. Use with array params for Postgres array matching.
    pub fn any(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" = ANY(");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    /// Append ` = ALL($1)`.
    pub fn all(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" = ALL(");
        self.0.push_eval(v);
        self.0.push(")");
        self
    }

    // -- subquery set ops ----------------------------------------------------

    /// Append ` IN (SELECT ...)` with a subquery.
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

    /// Append ` NOT IN (SELECT ...)` with a subquery.
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

    /// Append ` LIKE $1`. Case-sensitive pattern match.
    pub fn like(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" LIKE ");
        self.0.push_eval(v);
        self
    }

    /// Append ` ILIKE $1`. Case-insensitive pattern match (Postgres-specific).
    pub fn ilike(mut self, v: impl EvalExpr) -> Self {
        self.0.push(" ILIKE ");
        self.0.push_eval(v);
        self
    }

    // -- null checks ---------------------------------------------------------

    /// Append ` IS NULL`.
    pub fn is_null(mut self) -> Self {
        self.0.push(" IS NULL");
        self
    }

    /// Append ` IS NOT NULL`.
    pub fn is_not_null(mut self) -> Self {
        self.0.push(" IS NOT NULL");
        self
    }

    // -- range ---------------------------------------------------------------

    /// Append ` BETWEEN $1 AND $2`.
    pub fn between(mut self, lo: impl EvalExpr, hi: impl EvalExpr) -> Self {
        self.0.push(" BETWEEN ");
        self.0.push_eval(lo);
        self.0.push(" AND ");
        self.0.push_eval(hi);
        self
    }

    // -- aggregate / scalar wraps -------------------------------------------

    /// Wrap as `COUNT(buf)`.
    pub fn count(mut self) -> Self {
        self.0.wrap_fn("COUNT");
        self
    }

    /// Wrap as `SUM(buf)`.
    pub fn sum(mut self) -> Self {
        self.0.wrap_fn("SUM");
        self
    }

    /// Wrap as `AVG(buf)`.
    pub fn avg(mut self) -> Self {
        self.0.wrap_fn("AVG");
        self
    }

    /// Wrap as `MIN(buf)` (SQL aggregate — smallest value in group).
    pub fn min(mut self) -> Self {
        self.0.wrap_fn("MIN");
        self
    }

    /// Wrap as `MAX(buf)` (SQL aggregate — largest value in group).
    pub fn max(mut self) -> Self {
        self.0.wrap_fn("MAX");
        self
    }

    /// Wrap as `LOWER(buf)`.
    pub fn lower(mut self) -> Self {
        self.0.wrap_fn("LOWER");
        self
    }

    /// Wrap as `UPPER(buf)`.
    pub fn upper(mut self) -> Self {
        self.0.wrap_fn("UPPER");
        self
    }

    /// Wrap as `ABS(buf)`.
    pub fn abs(mut self) -> Self {
        self.0.wrap_fn("ABS");
        self
    }

    /// Wrap as `ROUND(buf, precision)`.
    pub fn round(mut self, precision: i32) -> Self {
        self.0.wrap_fn_expr("ROUND", &precision.to_string(), vec![]);
        self
    }

    /// Append `FILTER (WHERE condition)` — Postgres aggregate filter clause.
    ///
    /// Restricts an aggregate to rows matching the condition without affecting
    /// other aggregates in the same query.
    ///
    /// ```ignore
    /// // COUNT(*) FILTER (WHERE parse_status = 'Parsed')
    /// E::star().count().filter(ParseStatusCol::ParseStatus.eq(ParseStatus::Parsed))
    /// ```
    pub fn filter(mut self, condition: impl EvalExpr) -> Self {
        let (sql, binds) = condition.eval().unwrap();
        self.0.push(" FILTER (WHERE ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        self.0.push(")");
        self
    }

    /// Append `OVER (window_spec)` — window function clause.
    ///
    /// ```ignore
    /// // SUM("employees".salary) OVER (PARTITION BY "employees".dept)
    /// EmployeesCol::Salary.sum().over(
    ///     WindowSpec::new().partition_by(EmployeesCol::Dept.col())
    /// )
    /// ```
    pub fn over(mut self, spec: WindowSpec) -> Self {
        let (sql, binds) = spec.render();
        self.0.push(" ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        self
    }

    /// Wrap as `UNNEST(buf)` — expand a Postgres array into rows.
    pub fn unnest(mut self) -> Self {
        self.0.wrap_fn("UNNEST");
        self
    }

    /// Wrap as `DATE(buf)` — extract the date part of a timestamp.
    pub fn date(mut self) -> Self {
        self.0.wrap_fn("DATE");
        self
    }

    /// Wrap as `GREATEST(buf, other)` — scalar comparison, returns the larger value.
    /// Not to be confused with `MAX()` which is an aggregate over a group.
    pub fn greatest(mut self, other: impl EvalExpr) -> Self {
        let (sql, binds) = other.eval().unwrap();
        self.0.wrap_fn_expr("GREATEST", &sql, binds);
        self
    }

    /// Wrap as `LEAST(buf, other)` — scalar comparison, returns the smaller value.
    /// Not to be confused with `MIN()` which is an aggregate over a group.
    pub fn least(mut self, other: impl EvalExpr) -> Self {
        let (sql, binds) = other.eval().unwrap();
        self.0.wrap_fn_expr("LEAST", &sql, binds);
        self
    }

    // -- json ----------------------------------------------------------------

    /// Append ` -> key` — JSON field access, returns JSON.
    pub fn json_get(mut self, key: impl EvalExpr) -> Self {
        self.0.push(" -> ");
        self.0.push_eval(key);
        self
    }

    /// Append ` ->> key` — JSON field access, returns text.
    pub fn json_get_text(mut self, key: impl EvalExpr) -> Self {
        self.0.push(" ->> ");
        self.0.push_eval(key);
        self
    }

    /// Shorthand for `col ->> key = val` — JSONB text field equality.
    pub fn jsonb_text_eq(mut self, key: impl EvalExpr, val: impl EvalExpr) -> Self {
        self.0.push(" ->> ");
        self.0.push_eval(key);
        self.0.push(" = ");
        self.0.push_eval(val);
        self
    }

    /// Append ` #> path` — JSON path access, returns JSON.
    pub fn json_path(mut self, path: impl EvalExpr) -> Self {
        self.0.push(" #> ");
        self.0.push_eval(path);
        self
    }

    /// Append ` #>> path` — JSON path access, returns text.
    pub fn json_path_text(mut self, path: impl EvalExpr) -> Self {
        self.0.push(" #>> ");
        self.0.push_eval(path);
        self
    }

    // -- coalesce ------------------------------------------------------------

    /// Wrap as `COALESCE(buf, fallback)` — returns the first non-null argument.
    pub fn coalesce(mut self, fallback: impl EvalExpr) -> Self {
        self.0.buf.insert_str(0, "COALESCE(");
        self.0.push(", ");
        self.0.push_eval(fallback);
        self.0.push(")");
        self
    }

    // -- string functions ----------------------------------------------------

    /// Wrap as `CONCAT(buf)`.
    pub fn concat(mut self) -> Self {
        self.0.wrap_fn("CONCAT");
        self
    }

    /// Wrap as `LENGTH(buf)`.
    pub fn length(mut self) -> Self {
        self.0.wrap_fn("LENGTH");
        self
    }

    /// Wrap as `TRIM(buf)`.
    pub fn trim(mut self) -> Self {
        self.0.wrap_fn("TRIM");
        self
    }

    /// Wrap as `SUBSTRING(buf)`.
    pub fn substring(mut self) -> Self {
        self.0.wrap_fn("SUBSTRING");
        self
    }

    // -- cast / wrap ---------------------------------------------------------

    /// Append a Postgres type cast: `::ty` (e.g. `::text`, `::int`).
    pub fn cast(mut self, ty: &str) -> Self {
        self.0.buf.push_str("::");
        self.0.buf.push_str(ty);
        self
    }

    /// Wrap as `name(buf)` — escape hatch for SQL functions not yet built in.
    pub fn wrap_raw(mut self, name: &str) -> Self {
        self.0.wrap_fn(name);
        self
    }

    // -- alias ---------------------------------------------------------------

    /// Append ` AS name` — column alias for SELECT output.
    pub fn alias(mut self, name: &str) -> Self {
        self.0.buf.push_str(" AS ");
        self.0.buf.push_str(name);
        self
    }

    // -- logical operators ---------------------------------------------------

    /// Wrap preceding expression in `()` then append ` AND expr`.
    pub fn and(mut self, expr: impl EvalExpr) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") AND ");
        self.0.push_eval(expr);
        self
    }

    /// Wrap preceding expression in `()` then append ` OR expr`.
    pub fn or(mut self, expr: impl EvalExpr) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") OR ");
        self.0.push_eval(expr);
        self
    }

    /// Append ` AND expr` without parenthesising the preceding expression.
    pub fn and_bare(mut self, expr: impl EvalExpr) -> Self {
        self.0.push(" AND ");
        self.0.push_eval(expr);
        self
    }

    /// Append ` OR expr` without parenthesising the preceding expression.
    pub fn or_bare(mut self, expr: impl EvalExpr) -> Self {
        self.0.push(" OR ");
        self.0.push_eval(expr);
        self
    }

    /// Prepend `NOT ` to the entire expression.
    pub fn not(mut self) -> Self {
        self.0.buf.insert_str(0, "NOT ");
        self
    }

    /// Wrap the entire expression in `(…)`.
    pub fn paren(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(")");
        self
    }

    // -- function call -------------------------------------------------------

    /// Emit `name(prefix val)` — for SQL functions with a named-argument prefix.
    ///
    /// ```ignore
    /// // make_interval(hours => $1)
    /// E::new().func("make_interval", "hours => ", ttl_hours)
    /// ```
    pub fn func(mut self, name: &str, prefix: &str, v: impl EvalExpr) -> Self {
        self.0.buf.push_str(name);
        self.0.buf.push('(');
        self.0.buf.push_str(prefix);
        self.0.push_eval(v);
        self.0.buf.push(')');
        self
    }

    // -- splice expression ---------------------------------------------------

    /// Splice another expression's SQL and binds into the current position.
    pub fn expr(mut self, e: impl EvalExpr) -> Self {
        self.0.push_eval(e);
        self
    }

    // -- subqueries ----------------------------------------------------------

    /// Append `EXISTS (SELECT ...)`.
    pub fn exists(self, q: SqlSelect) -> Self {
        self.raw("EXISTS ").select(q)
    }

    /// Append `NOT EXISTS (SELECT ...)`.
    pub fn not_exists(self, q: SqlSelect) -> Self {
        self.raw("NOT EXISTS ").select(q)
    }

    /// Append a parenthesised subquery: `(SELECT ...)`.
    pub fn select(mut self, q: SqlSelect) -> Self {
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        self.0.buf.push('(');
        self.0.buf.push_str(&sub_sql);
        self.0.buf.push(')');
        self.0.binds.extend(sub_binds);
        self
    }

    // -- window function constructors ----------------------------------------

    fn window_fn(name: &str, col: impl EvalExpr) -> Self {
        let mut e = Self::new();
        e.0.push(name);
        e.0.push("(");
        e.0.push_eval(col);
        e.0.push(")");
        e
    }

    /// `ROW_NUMBER()` — sequential row number within partition.
    pub fn row_number() -> Self {
        Self::new().raw("ROW_NUMBER()")
    }

    /// `RANK()` — rank with gaps for ties.
    pub fn rank() -> Self {
        Self::new().raw("RANK()")
    }

    /// `DENSE_RANK()` — rank without gaps.
    pub fn dense_rank() -> Self {
        Self::new().raw("DENSE_RANK()")
    }

    /// `NTILE(n)` — distribute rows into n buckets.
    pub fn ntile(n: u32) -> Self {
        let mut e = Self::new();
        e.0.push("NTILE(");
        e.0.push(itoa::Buffer::new().format(n));
        e.0.push(")");
        e
    }

    /// `LAG(expr)` — value from previous row in partition.
    pub fn lag(col: impl EvalExpr) -> Self {
        Self::window_fn("LAG", col)
    }

    /// `LEAD(expr)` — value from next row in partition.
    pub fn lead(col: impl EvalExpr) -> Self {
        Self::window_fn("LEAD", col)
    }

    /// `FIRST_VALUE(expr)` — first value in window frame.
    pub fn first_value(col: impl EvalExpr) -> Self {
        Self::window_fn("FIRST_VALUE", col)
    }

    /// `LAST_VALUE(expr)` — last value in window frame.
    pub fn last_value(col: impl EvalExpr) -> Self {
        Self::window_fn("LAST_VALUE", col)
    }

    /// `NTH_VALUE(expr, n)` — nth value in window frame.
    pub fn nth_value(col: impl EvalExpr, n: u32) -> Self {
        let mut e = Self::window_fn("NTH_VALUE", col);
        e.0.buf.pop(); // remove ')'
        e.0.push(", ");
        e.0.push(itoa::Buffer::new().format(n));
        e.0.push(")");
        e
    }

    // -- CASE WHEN -----------------------------------------------------------

    /// Begin a `CASE WHEN condition …` block. Must chain `.then_()` then `.else_()`.
    pub fn if_(mut self, condition: impl EvalExpr) -> ExprIf<T> {
        self.0.push("CASE WHEN ");
        self.0.push_eval(condition);
        ExprIf(self.0)
    }

    // -- internal ------------------------------------------------------------

    /// Split a `col = val` expression into its column name and value SQL.
    /// Used internally by INSERT to extract column names from SET expressions.
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

/// Blanket impl: any `Into<SqlParam>` type (scalars, enums, arrays) can be
/// used as an `EvalExpr`. Produces `$#` (placeholder) with one bind.
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
///
/// Each method creates `Expr::new().column(self)` then chains the operation,
/// so `UC::Name.eq("alice")` is equivalent to `Expr::new().column(UC::Name).eq().val("alice")`.
///
/// All methods have default implementations. The derive macro generates an
/// empty `impl ColOps<Struct> for StructCol {}` plus thin inherent wrappers
/// (needed because `PartialEq::eq` would shadow the trait method).
pub trait ColOps<T: Table<Col = Self>>: AsRef<str> + Display + Copy {
    /// `"table".col = val`
    fn eq(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).eq(val)
    }

    /// `"table".col != val`
    fn neq(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).neq(val)
    }

    /// `"table".col > val`
    fn gt(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).gt(val)
    }

    /// `"table".col >= val`
    fn gte(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).gte(val)
    }

    /// `"table".col < val`
    fn lt(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).lt(val)
    }

    /// `"table".col <= val`
    fn lte(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).lte(val)
    }

    /// `"table".col + val`
    fn add(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).add(val)
    }

    /// `"table".col - val`
    fn sub(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).sub(val)
    }

    /// `"table".col * val`
    fn mul(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).mul(val)
    }

    /// `"table".col / val`
    fn div(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).div(val)
    }

    /// `"table".col LIKE val`
    fn like(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).like(val)
    }

    /// `"table".col ILIKE val`
    fn ilike(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).ilike(val)
    }

    /// `"table".col IN (val)`
    fn in_(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).in_(val)
    }

    /// `"table".col NOT IN (val)`
    fn not_in(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).not_in(val)
    }

    /// `"table".col BETWEEN lo AND hi`
    fn between(self, lo: impl EvalExpr, hi: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).between(lo, hi)
    }

    /// `"table".col IN (SELECT ...)`
    fn in_select(self, select: SqlSelect) -> Expr<T> {
        Expr::new().column(self).in_select(select)
    }

    /// `"table".col NOT IN (SELECT ...)`
    fn not_in_select(self, select: SqlSelect) -> Expr<T> {
        Expr::new().column(self).not_in_select(select)
    }

    /// `"table".col IS NULL`
    fn is_null(self) -> Expr<T> {
        Expr::new().column(self).is_null()
    }

    /// `"table".col IS NOT NULL`
    fn is_not_null(self) -> Expr<T> {
        Expr::new().column(self).is_not_null()
    }

    /// `COUNT("table".col)`
    fn count(self) -> Expr<T> {
        Expr::new().column(self).count()
    }

    /// `SUM("table".col)`
    fn sum(self) -> Expr<T> {
        Expr::new().column(self).sum()
    }

    /// `AVG("table".col)`
    fn avg(self) -> Expr<T> {
        Expr::new().column(self).avg()
    }

    /// `MIN("table".col)` — aggregate minimum.
    fn min(self) -> Expr<T> {
        Expr::new().column(self).min()
    }

    /// `MAX("table".col)` — aggregate maximum.
    fn max(self) -> Expr<T> {
        Expr::new().column(self).max()
    }

    /// `GREATEST("table".col, other)` — scalar comparison.
    fn greatest(self, other: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).greatest(other)
    }

    /// `LEAST("table".col, other)` — scalar comparison.
    fn least(self, other: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).least(other)
    }

    /// `LOWER("table".col)`
    fn lower(self) -> Expr<T> {
        Expr::new().column(self).lower()
    }

    /// `UPPER("table".col)`
    fn upper(self) -> Expr<T> {
        Expr::new().column(self).upper()
    }

    /// `ABS("table".col)`
    fn abs(self) -> Expr<T> {
        Expr::new().column(self).abs()
    }

    /// `DATE("table".col)`
    fn date(self) -> Expr<T> {
        Expr::new().column(self).date()
    }

    /// `"table".col -> key` — JSON field access, returns JSON.
    fn json_get(self, key: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_get(key)
    }

    /// `"table".col ->> key` — JSON field access, returns text.
    fn json_get_text(self, key: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_get_text(key)
    }

    /// `"table".col && val` — Postgres array overlap check.
    fn overlap(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).overlap(val)
    }

    /// `"table".col = ANY(val)` — Postgres array contains check.
    fn any(self, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).any(val)
    }

    /// `"table".col ->> key = val` — JSONB text field equality.
    fn jsonb_text_eq(self, key: impl EvalExpr, val: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).jsonb_text_eq(key, val)
    }

    /// `"table".col #> path` — JSON path access, returns JSON.
    fn json_path(self, path: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_path(path)
    }

    /// `"table".col #>> path` — JSON path access, returns text.
    fn json_path_text(self, path: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).json_path_text(path)
    }

    /// `"table".col AS name`
    fn alias(self, name: &str) -> Expr<T> {
        Expr::new().column(self).alias(name)
    }

    /// `"table".col::ty`
    fn cast(self, ty: &str) -> Expr<T> {
        Expr::new().column(self).cast(ty)
    }

    /// `COALESCE("table".col, fallback)`
    fn coalesce(self, fallback: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).coalesce(fallback)
    }

    /// `ROUND("table".col, precision)`
    fn round(self, precision: i32) -> Expr<T> {
        Expr::new().column(self).round(precision)
    }

    /// `CONCAT("table".col)`
    fn concat(self) -> Expr<T> {
        Expr::new().column(self).concat()
    }

    /// `LENGTH("table".col)`
    fn length(self) -> Expr<T> {
        Expr::new().column(self).length()
    }

    /// `TRIM("table".col)`
    fn trim(self) -> Expr<T> {
        Expr::new().column(self).trim()
    }

    /// `SUBSTRING("table".col)`
    fn substring(self) -> Expr<T> {
        Expr::new().column(self).substring()
    }

    /// `UNNEST("table".col)`
    fn unnest(self) -> Expr<T> {
        Expr::new().column(self).unnest()
    }

    /// `name("table".col)` — escape hatch for SQL functions not yet built in.
    fn wrap_raw(self, name: &str) -> Expr<T> {
        Expr::new().column(self).wrap_raw(name)
    }

    /// Append `FILTER (WHERE condition)` after an aggregate.
    fn filter(self, condition: impl EvalExpr) -> Expr<T> {
        Expr::new().column(self).filter(condition)
    }

    /// Append `OVER (window_spec)` after an aggregate or window function.
    fn over(self, spec: WindowSpec) -> Expr<T> {
        Expr::new().column(self).over(spec)
    }

    /// `LAG("table".col)`
    fn lag(self) -> Expr<T> {
        Expr::lag(self.col())
    }

    /// `LEAD("table".col)`
    fn lead(self) -> Expr<T> {
        Expr::lead(self.col())
    }

    /// `FIRST_VALUE("table".col)`
    fn first_value(self) -> Expr<T> {
        Expr::first_value(self.col())
    }

    /// `LAST_VALUE("table".col)`
    fn last_value(self) -> Expr<T> {
        Expr::last_value(self.col())
    }

    /// Start an `Expr<T>` from this column for further chaining.
    fn col(self) -> Expr<T> {
        Expr::new().column(self)
    }

    /// Qualified column reference reinterpreted as an `Expr<U>`. Lets a foreign
    /// column be spliced into a query built against another table.
    fn coerce<U: Table>(self) -> Expr<U> {
        self.col().coerce::<U>()
    }
}

// ---------------------------------------------------------------------------
// ExprIf<T> / ExprThen<T> — CASE WHEN typestate
// ---------------------------------------------------------------------------

/// Intermediate state after `.if_(condition)`. Must call `.then_()` next.
pub struct ExprIf<T: Table>(ExprBuf<T>);

impl<T: Table> ExprIf<T> {
    /// Append ` THEN val`. Must call `.else_()` after this.
    pub fn then_(mut self, val: impl EvalExpr) -> ExprThen<T> {
        let (sql, binds) = val.eval().unwrap();
        self.0.push(" THEN ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        ExprThen(self.0)
    }
}

/// Intermediate state after `.then_()`. Must call `.else_()` to complete.
pub struct ExprThen<T: Table>(ExprBuf<T>);

impl<T: Table> ExprThen<T> {
    /// Append ` ELSE val END` and return the completed expression.
    pub fn else_(mut self, val: impl EvalExpr) -> Expr<T> {
        let (sql, binds) = val.eval().unwrap();
        self.0.push(" ELSE ");
        self.0.push(&sql);
        self.0.binds.extend(binds);
        self.0.push(" END");
        Expr(self.0)
    }
}

// ---------------------------------------------------------------------------
// WindowSpec — OVER (...) clause builder
// ---------------------------------------------------------------------------

/// Boundary for a window frame (ROWS/RANGE BETWEEN ... AND ...).
#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

impl FrameBound {
    fn to_sql(&self) -> String {
        match self {
            Self::UnboundedPreceding => "UNBOUNDED PRECEDING".into(),
            Self::Preceding(n) => format!("{n} PRECEDING"),
            Self::CurrentRow => "CURRENT ROW".into(),
            Self::Following(n) => format!("{n} FOLLOWING"),
            Self::UnboundedFollowing => "UNBOUNDED FOLLOWING".into(),
        }
    }
}

/// Builder for the `OVER (...)` clause of a window function.
///
/// ```ignore
/// // SUM("employees".salary) OVER (PARTITION BY "employees".dept ORDER BY "employees".name ASC)
/// EmployeesCol::Salary.sum().over(
///     WindowSpec::new()
///         .partition_by(EmployeesCol::Dept.col())
///         .order_by(EmployeesCol::Name.col(), SqlOrder::Asc)
/// )
/// ```
#[derive(Debug, Clone)]
pub struct WindowSpec {
    partition_by: Vec<String>,
    order_by: Vec<String>,
    binds: Vec<SqlParam>,
    frame: Option<(&'static str, FrameBound, FrameBound)>,
}

impl WindowSpec {
    pub fn new() -> Self {
        Self { partition_by: Vec::new(), order_by: Vec::new(), binds: Vec::new(), frame: None }
    }

    /// Add a PARTITION BY expression.
    pub fn partition_by(mut self, col: impl EvalExpr) -> Self {
        let (sql, binds) = col.eval().unwrap();
        self.partition_by.push(sql);
        self.binds.extend(binds);
        self
    }

    /// Add an ORDER BY expression with direction.
    pub fn order_by(mut self, col: impl EvalExpr, order: SqlOrder) -> Self {
        let (mut sql, binds) = col.eval().unwrap();
        sql.push(' ');
        sql.push_str(order.as_ref());
        self.order_by.push(sql);
        self.binds.extend(binds);
        self
    }

    /// Set the frame clause: `ROWS BETWEEN start AND end`.
    pub fn rows_between(mut self, start: FrameBound, end: FrameBound) -> Self {
        self.frame = Some(("ROWS", start, end));
        self
    }

    /// Set the frame clause: `RANGE BETWEEN start AND end`.
    pub fn range_between(mut self, start: FrameBound, end: FrameBound) -> Self {
        self.frame = Some(("RANGE", start, end));
        self
    }

    fn render(self) -> (String, Vec<SqlParam>) {
        let mut sql = String::from("OVER (");

        if !self.partition_by.is_empty() {
            sql.push_str("PARTITION BY ");
            sql.push_str(&self.partition_by.join(", "));
        }

        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                sql.push(' ');
            }
            sql.push_str("ORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        if let Some((kind, start, end)) = self.frame {
            sql.push(' ');
            sql.push_str(kind);
            sql.push_str(" BETWEEN ");
            sql.push_str(&start.to_sql());
            sql.push_str(" AND ");
            sql.push_str(&end.to_sql());
        }

        sql.push(')');
        (sql, self.binds)
    }
}

// ---------------------------------------------------------------------------
// Enums used by the query builders
// ---------------------------------------------------------------------------

/// Sort direction for `ORDER BY` clauses.
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

/// SQL join type for combining tables.
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
#[path = "expr_tests.rs"]
mod tests;
