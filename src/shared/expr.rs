use std::{fmt::Write, marker::PhantomData};

use compact_str::CompactString;
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
///
/// Implemented by all "complete" expression states (`Expr<T>`, `ExprCol<T>`),
/// allowing query-builder methods to accept any of them without `Into` conversion.
pub trait EvalExpr {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError>;
}

// ---------------------------------------------------------------------------
// Internal buffer shared by all typestate structs
// ---------------------------------------------------------------------------

struct ExprBuf<T: Table> {
    buf: CompactString,
    binds: SmallVec<[SqlParam; 2]>,
    _t: PhantomData<T>,
}

impl<T: Table> ExprBuf<T> {
    fn new() -> Self {
        Self { buf: CompactString::default(), binds: SmallVec::new(), _t: PhantomData }
    }

    fn push(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    fn push_col(&mut self, col: T::Col) {
        write!(self.buf, "\"{}\".{}", T::TABLE_NAME, col.as_ref()).unwrap();
    }

    fn push_val(&mut self, v: impl Into<SqlParam>) {
        self.buf.push_str("$#");
        self.binds.push(v.into());
    }

    fn wrap_fn(&mut self, name: &str) {
        let mut new_buf = CompactString::with_capacity(name.len() + 1 + self.buf.len() + 1);
        new_buf.push_str(name);
        new_buf.push('(');
        new_buf.push_str(&self.buf);
        new_buf.push(')');
        self.buf = new_buf;
    }

    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        Ok((self.buf.into(), self.binds.into_vec()))
    }
}

// ---------------------------------------------------------------------------
// Expr<T> — base / terminal state
// ---------------------------------------------------------------------------

/// Base / terminal state of the expression builder.
///
/// Start here with `Expr::new()`, chain methods to build SQL, and call `.eval()` to finalize.
/// Most query builder methods (`.filter()`, `.set()`, etc.) accept `Expr<T>` directly.
///
/// ```ignore
/// use sql_query::Expr;
/// type E = Expr<Users>;
///
/// // Simple: col = val
/// E::new().column(UC::Name).eq().val("alice")
///
/// // Self-ref arithmetic: col = col + val
/// E::new().column(UC::Balance).eq().column(UC::Balance).add().val(amount)
///
/// // CASE WHEN
/// E::new().column(UC::Status).eq()
///     .if_(E::new().val(is_active))
///     .then_(E::new().raw("'active'"))
///     .else_(E::new().raw("'inactive'"))
/// ```
pub struct Expr<T: Table>(ExprBuf<T>);

impl<T: Table> Expr<T> {
    /// Start a new empty expression.
    pub fn new() -> Self {
        Self(ExprBuf::new())
    }

    /// Append a qualified column reference: `"table".col`.
    pub fn column(mut self, col: T::Col) -> ExprCol<T> {
        self.0.push_col(col);
        ExprCol(self.0)
    }

    /// Append a bound parameter placeholder.
    pub fn val(mut self, v: impl Into<SqlParam>) -> Self {
        self.0.push_val(v);
        self
    }

    /// Append `NOW()`.
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

    /// Append raw SQL verbatim.
    pub fn raw(mut self, s: &str) -> Self {
        self.0.push(s);
        self
    }

    /// Wrap the current expression in `(…)`, append ` AND `, and continue.
    pub fn and(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") AND ");
        self
    }

    /// Wrap the current expression in `(…)`, append ` OR `, and continue.
    pub fn or(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(") OR ");
        self
    }

    /// Append ` AND ` without wrapping.
    pub fn and_bare(mut self) -> Self {
        self.0.push(" AND ");
        self
    }

    /// Append ` OR ` without wrapping.
    pub fn or_bare(mut self) -> Self {
        self.0.push(" OR ");
        self
    }

    /// Prepend `NOT ` to the entire expression.
    pub fn not(mut self) -> Self {
        self.0.buf.insert_str(0, "NOT ");
        self
    }

    /// Begin a `CASE WHEN <condition> …` block.
    pub fn if_(mut self, condition: Expr<T>) -> ExprIf<T> {
        let (cond_sql, cond_binds) = condition.0.eval().unwrap();
        self.0.push("CASE WHEN ");
        self.0.push(&cond_sql);
        self.0.binds.extend(cond_binds);
        ExprIf(self.0)
    }

    /// Wrap the entire buffer in parentheses: `(buf)`.
    pub fn paren(mut self) -> Self {
        self.0.buf.insert(0, '(');
        self.0.push(")");
        self
    }

    /// Append a type cast: `::ty`.
    pub fn cast(mut self, ty: &str) -> Self {
        write!(self.0.buf, "::{}", ty).unwrap();
        self
    }

    /// Wrap the buffer with an arbitrary function: `name(buf)`.
    /// Escape hatch for functions not yet supported as dedicated methods.
    pub fn wrap_raw(mut self, name: &str) -> Self {
        self.0.buf.insert(0, '(');
        self.0.buf.insert_str(0, name);
        self.0.push(")");
        self
    }

    /// `GREATEST(a, b)` — returns the larger of two expressions.
    pub fn greatest(a: impl Into<Expr<T>>, b: impl Into<Expr<T>>) -> Self {
        let a: Expr<T> = a.into();
        let b: Expr<T> = b.into();
        let (a_sql, a_binds) = a.0.eval().unwrap();
        let (b_sql, b_binds) = b.0.eval().unwrap();
        let mut e = Self::new();
        write!(e.0.buf, "GREATEST({}, {})", a_sql, b_sql).unwrap();
        e.0.binds.extend(a_binds);
        e.0.binds.extend(b_binds);
        e
    }

    /// `LEAST(a, b)` — returns the smaller of two expressions.
    pub fn least(a: impl Into<Expr<T>>, b: impl Into<Expr<T>>) -> Self {
        let a: Expr<T> = a.into();
        let b: Expr<T> = b.into();
        let (a_sql, a_binds) = a.0.eval().unwrap();
        let (b_sql, b_binds) = b.0.eval().unwrap();
        let mut e = Self::new();
        write!(e.0.buf, "LEAST({}, {})", a_sql, b_sql).unwrap();
        e.0.binds.extend(a_binds);
        e.0.binds.extend(b_binds);
        e
    }

    /// Append `EXISTS (subquery)`.
    pub fn exists(self, q: SqlSelect) -> Self {
        self.raw("EXISTS ").select(q)
    }

    /// Append `NOT EXISTS (subquery)`.
    pub fn not_exists(self, q: SqlSelect) -> Self {
        self.raw("NOT EXISTS ").select(q)
    }

    /// Append a parenthesised subquery.
    pub fn select(mut self, q: SqlSelect) -> Self {
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        write!(self.0.buf, "({})", sub_sql).unwrap();
        self.0.binds.extend(sub_binds);
        self
    }

    /// Split into (column_name, value-only Expr) for INSERT column extraction.
    pub(crate) fn into_col_and_val(self) -> (Option<String>, String, Vec<SqlParam>) {
        let sql = self.0.buf;
        let binds = self.0.binds;
        // The buffer format for col = val is: "table".col_name = <rhs>
        // Extract column name if the expression starts with a qualified column ref
        if let Some(eq_pos) = sql.find(" = ") {
            let col_part = &sql[..eq_pos];
            // Extract bare column name after the last dot
            if let Some(dot_pos) = col_part.rfind('.') {
                let col_name: String = col_part[dot_pos + 1..].into();
                let val_sql: String = sql[eq_pos + 3..].into();
                return (Some(col_name), val_sql, binds.into_vec());
            }
        }
        (None, sql.into(), binds.into_vec())
    }
}

impl<T: Table> EvalExpr for Expr<T> {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        self.0.eval()
    }
}

impl<T: Table> From<SqlParam> for Expr<T> {
    fn from(val: SqlParam) -> Self {
        Self::new().val(val)
    }
}

impl<T: Table> From<ExprCol<T>> for Expr<T> {
    fn from(col: ExprCol<T>) -> Self {
        Expr(col.0)
    }
}

// ---------------------------------------------------------------------------
// ExprCol<T> — after a column reference
// ---------------------------------------------------------------------------

/// After-column state. Reached via `.column()` or `Col::col()`.
///
/// Exposes operators (`.eq()`, `.add()`, ...), function wraps (`.count()`, `.lower()`, ...),
/// null checks, and `.alias()`. Also available: `.cast()`, `.coalesce()`, `.wrap_raw()`.
pub struct ExprCol<T: Table>(ExprBuf<T>);

impl<T: Table> ExprCol<T> {
    // -- comparison operators ------------------------------------------------

    pub fn eq(mut self) -> ExprOp<T> {
        self.0.push(" = ");
        ExprOp(self.0)
    }

    pub fn neq(mut self) -> ExprOp<T> {
        self.0.push(" != ");
        ExprOp(self.0)
    }

    pub fn gt(mut self) -> ExprOp<T> {
        self.0.push(" > ");
        ExprOp(self.0)
    }

    pub fn gte(mut self) -> ExprOp<T> {
        self.0.push(" >= ");
        ExprOp(self.0)
    }

    pub fn lt(mut self) -> ExprOp<T> {
        self.0.push(" < ");
        ExprOp(self.0)
    }

    pub fn lte(mut self) -> ExprOp<T> {
        self.0.push(" <= ");
        ExprOp(self.0)
    }

    // -- arithmetic operators ------------------------------------------------

    pub fn add(mut self) -> ExprOp<T> {
        self.0.push(" + ");
        ExprOp(self.0)
    }

    pub fn sub(mut self) -> ExprOp<T> {
        self.0.push(" - ");
        ExprOp(self.0)
    }

    pub fn mul(mut self) -> ExprOp<T> {
        self.0.push(" * ");
        ExprOp(self.0)
    }

    pub fn div(mut self) -> ExprOp<T> {
        self.0.push(" / ");
        ExprOp(self.0)
    }

    // -- set operators -------------------------------------------------------

    pub fn in_(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" IN (");
        self.0.push_val(v);
        self.0.push(")");
        Expr(self.0)
    }

    pub fn not_in(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" NOT IN (");
        self.0.push_val(v);
        self.0.push(")");
        Expr(self.0)
    }

    // -- any / all -----------------------------------------------------------

    pub fn any(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" = ANY(");
        self.0.push_val(v);
        self.0.push(")");
        Expr(self.0)
    }

    pub fn all(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" = ALL(");
        self.0.push_val(v);
        self.0.push(")");
        Expr(self.0)
    }

    // -- subquery set ops ----------------------------------------------------

    pub fn in_select(mut self, q: SqlSelect) -> Expr<T> {
        self.0.push(" IN ");
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        write!(self.0.buf, "({})", sub_sql).unwrap();
        self.0.binds.extend(sub_binds);
        Expr(self.0)
    }

    pub fn not_in_select(mut self, q: SqlSelect) -> Expr<T> {
        self.0.push(" NOT IN ");
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        write!(self.0.buf, "({})", sub_sql).unwrap();
        self.0.binds.extend(sub_binds);
        Expr(self.0)
    }

    // -- pattern matching ----------------------------------------------------

    pub fn like(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" LIKE ");
        self.0.push_val(v);
        Expr(self.0)
    }

    pub fn ilike(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" ILIKE ");
        self.0.push_val(v);
        Expr(self.0)
    }

    // -- null checks ---------------------------------------------------------

    pub fn is_null(mut self) -> Expr<T> {
        self.0.push(" IS NULL");
        Expr(self.0)
    }

    pub fn is_not_null(mut self) -> Expr<T> {
        self.0.push(" IS NOT NULL");
        Expr(self.0)
    }

    // -- range ---------------------------------------------------------------

    pub fn between(mut self, lo: impl Into<SqlParam>, hi: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" BETWEEN ");
        self.0.push_val(lo);
        self.0.push(" AND ");
        self.0.push_val(hi);
        Expr(self.0)
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

    // -- json ----------------------------------------------------------------

    pub fn json_get(mut self, key: impl Into<SqlParam>) -> ExprCol<T> {
        self.0.push(" -> ");
        self.0.push_val(key);
        self
    }

    pub fn json_get_text(mut self, key: impl Into<SqlParam>) -> ExprCol<T> {
        self.0.push(" ->> ");
        self.0.push_val(key);
        self
    }

    /// `col ->> key = val` JSONB text equality.
    pub fn jsonb_text_eq(mut self, key: impl Into<SqlParam>, val: impl Into<SqlParam>) -> Expr<T> {
        self.0.push(" ->> ");
        self.0.push_val(key);
        self.0.push(" = ");
        self.0.push_val(val);
        Expr(self.0)
    }

    /// `col #> path` JSON path get.
    pub fn json_path(mut self, path: impl Into<SqlParam>) -> ExprCol<T> {
        self.0.push(" #> ");
        self.0.push_val(path);
        self
    }

    /// `col #>> path` JSON path get as text.
    pub fn json_path_text(mut self, path: impl Into<SqlParam>) -> ExprCol<T> {
        self.0.push(" #>> ");
        self.0.push_val(path);
        self
    }

    // -- coalesce ------------------------------------------------------------

    /// Wrap buffer: `COALESCE(buf, fallback)`.
    pub fn coalesce(mut self, fallback: impl Into<SqlParam>) -> Self {
        self.0.buf.insert_str(0, "COALESCE(");
        self.0.push(", ");
        self.0.push_val(fallback);
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

    /// Append a type cast: `::ty`.
    pub fn cast(mut self, ty: &str) -> Self {
        write!(self.0.buf, "::{}", ty).unwrap();
        self
    }

    /// Wrap the buffer with an arbitrary function: `name(buf)`.
    /// Escape hatch for functions not yet supported as dedicated methods.
    pub fn wrap_raw(mut self, name: &str) -> Self {
        self.0.buf.insert(0, '(');
        self.0.buf.insert_str(0, name);
        self.0.push(")");
        self
    }

    // -- alias ---------------------------------------------------------------

    pub fn alias(mut self, name: &str) -> Expr<T> {
        write!(self.0.buf, " AS {}", name).unwrap();
        Expr(self.0)
    }
}

impl<T: Table> EvalExpr for ExprCol<T> {
    fn eval(self) -> Result<(String, Vec<SqlParam>), SqlQueryError> {
        self.0.eval()
    }
}

// ---------------------------------------------------------------------------
// ExprOp<T> — after an operator, expecting a value / column / subquery
// ---------------------------------------------------------------------------

/// After-operator state. Reached after `.eq()`, `.add()`, `.gt()`, etc.
///
/// Exposes value-side methods: `.column()`, `.val()`, `.now()`, `.null()`, `.raw()`,
/// `.select()` (subquery), and `.if_()` (CASE WHEN).
pub struct ExprOp<T: Table>(ExprBuf<T>);

impl<T: Table> ExprOp<T> {
    /// Append a qualified column reference.
    pub fn column(mut self, col: T::Col) -> ExprCol<T> {
        self.0.push_col(col);
        ExprCol(self.0)
    }

    /// Append a bound parameter placeholder.
    pub fn val(mut self, v: impl Into<SqlParam>) -> Expr<T> {
        self.0.push_val(v);
        Expr(self.0)
    }

    /// Append `NOW()`.
    pub fn now(mut self) -> Expr<T> {
        self.0.push("NOW()");
        Expr(self.0)
    }

    /// Append `NULL`.
    pub fn null(mut self) -> Expr<T> {
        self.0.push("NULL");
        Expr(self.0)
    }

    /// Append raw SQL verbatim.
    pub fn raw(mut self, s: &str) -> Expr<T> {
        self.0.push(s);
        Expr(self.0)
    }

    /// Append a parenthesised subquery.
    pub fn select(mut self, q: SqlSelect) -> Expr<T> {
        let uq = SqlBase::build(q).expect("subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        write!(self.0.buf, "({})", sub_sql).unwrap();
        self.0.binds.extend(sub_binds);
        Expr(self.0)
    }

    /// Begin a `CASE WHEN <condition> …` block.
    pub fn if_(mut self, condition: Expr<T>) -> ExprIf<T> {
        let (cond_sql, cond_binds) = condition.0.eval().unwrap();
        self.0.push("CASE WHEN ");
        self.0.push(&cond_sql);
        self.0.binds.extend(cond_binds);
        ExprIf(self.0)
    }
}

// ---------------------------------------------------------------------------
// ExprIf<T> / ExprThen<T> — CASE WHEN typestate
// ---------------------------------------------------------------------------

/// CASE WHEN typestate. Reached via `.if_(condition)`. Must call `.then_()` next.
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

/// CASE THEN typestate. Reached via `.then_()`. Must call `.else_()` to complete.
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
// Enums used by the query builders (SELECT, UPDATE, etc.)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqlCols, define_id};
    use sqlx::FromRow;

    // -- Test helpers --------------------------------------------------------

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

    fn eval_col(e: ExprCol<TestTable>) -> (String, Vec<SqlParam>) {
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
        let (sql, _) = eval_col(E::new().column(TC::Age).count());
        assert_eq!(sql, r#"COUNT("test_table".age)"#);
    }

    #[test]
    fn sum_col() {
        let (sql, _) = eval_col(E::new().column(TC::Age).sum());
        assert_eq!(sql, r#"SUM("test_table".age)"#);
    }

    #[test]
    fn avg_col() {
        let (sql, _) = eval_col(E::new().column(TC::Age).avg());
        assert_eq!(sql, r#"AVG("test_table".age)"#);
    }

    #[test]
    fn min_col() {
        let (sql, _) = eval_col(E::new().column(TC::Age).min());
        assert_eq!(sql, r#"MIN("test_table".age)"#);
    }

    #[test]
    fn max_col() {
        let (sql, _) = eval_col(E::new().column(TC::Age).max());
        assert_eq!(sql, r#"MAX("test_table".age)"#);
    }

    #[test]
    fn lower_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).lower());
        assert_eq!(sql, r#"LOWER("test_table".name)"#);
    }

    #[test]
    fn upper_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).upper());
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
        assert_eq!(sql, r#""test_table".name = CASE WHEN $# THEN $# ELSE NULL END"#,);
        assert_eq!(binds, vec![SqlParam::Bool(true), SqlParam::String("yes".into())],);
    }

    #[test]
    fn if_then_else_with_column() {
        // closed_at = CASE WHEN $1 THEN "t".acquired_at ELSE NULL END
        let then_val: E = E::new().column(TC::CreatedAt).into();
        let (sql, binds) = eval(
            E::new()
                .column(TC::CreatedAt)
                .eq()
                .if_(E::new().val(SqlParam::Bool(true)))
                .then_(then_val)
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
        let (sql, _) = eval_col(E::new().column(TC::Data).json_get("key"));
        assert_eq!(sql, r#""test_table".data -> $#"#);
    }

    #[test]
    fn json_get_text_key() {
        let (sql, _) = eval_col(E::new().column(TC::Data).json_get_text("key"));
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
        assert_eq!(binds, vec![SqlParam::String("key".into()), SqlParam::String("val".into())],);
    }

    #[test]
    fn json_path() {
        let (sql, _) = eval_col(E::new().column(TC::Data).json_path("{a,b}"));
        assert_eq!(sql, r#""test_table".data #> $#"#);
    }

    #[test]
    fn json_path_text() {
        let (sql, _) = eval_col(E::new().column(TC::Data).json_path_text("{a,b}"));
        assert_eq!(sql, r#""test_table".data #>> $#"#);
    }

    // -- COALESCE ------------------------------------------------------------

    #[test]
    fn coalesce_col() {
        let (sql, binds) = eval_col(E::new().column(TC::Age).coalesce(SqlParam::I32(0)));
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
        let (sql, _) = eval_col(E::new().column(TC::Age).cast("text"));
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
        let (sql, _) = eval_col(E::new().column(TC::Name).wrap_raw("UNACCENT"));
        assert_eq!(sql, r#"UNACCENT("test_table".name)"#);
    }

    // -- String functions ----------------------------------------------------

    #[test]
    fn concat_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).concat());
        assert_eq!(sql, r#"CONCAT("test_table".name)"#);
    }

    #[test]
    fn length_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).length());
        assert_eq!(sql, r#"LENGTH("test_table".name)"#);
    }

    #[test]
    fn trim_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).trim());
        assert_eq!(sql, r#"TRIM("test_table".name)"#);
    }

    #[test]
    fn substring_col() {
        let (sql, _) = eval_col(E::new().column(TC::Name).substring());
        assert_eq!(sql, r#"SUBSTRING("test_table".name)"#);
    }

    // -- GREATEST / LEAST ----------------------------------------------------

    #[test]
    fn greatest_two_vals() {
        let (sql, binds) =
            eval(E::greatest(E::new().val(SqlParam::I32(1)), E::new().val(SqlParam::I32(2))));
        assert_eq!(sql, "GREATEST($#, $#)");
        assert_eq!(binds, vec![SqlParam::I32(1), SqlParam::I32(2)]);
    }

    #[test]
    fn greatest_val_and_col() {
        let col: E = E::new().column(TC::CreatedAt).into();
        let (sql, binds) = eval(E::greatest(E::new().val(SqlParam::String("ts".into())), col));
        assert_eq!(sql, r#"GREATEST($#, "test_table".created_at)"#);
        assert_eq!(binds, vec![SqlParam::String("ts".into())]);
    }

    #[test]
    fn least_two_vals() {
        let (sql, binds) =
            eval(E::least(E::new().val(SqlParam::I32(10)), E::new().val(SqlParam::I32(20))));
        assert_eq!(sql, "LEAST($#, $#)");
        assert_eq!(binds, vec![SqlParam::I32(10), SqlParam::I32(20)]);
    }

    #[test]
    fn greatest_in_case_when() {
        // closed_at = CASE WHEN $1 THEN GREATEST($2, "t".acquired_at) ELSE NULL END
        let col: E = E::new().column(TC::CreatedAt).into();
        let (sql, binds) = eval(
            E::new()
                .column(TC::CreatedAt)
                .eq()
                .if_(E::new().val(SqlParam::Bool(true)))
                .then_(E::greatest(E::new().val(SqlParam::String("ts".into())), col))
                .else_(E::new().null()),
        );
        assert_eq!(
            sql,
            r#""test_table".created_at = CASE WHEN $# THEN GREATEST($#, "test_table".created_at) ELSE NULL END"#,
        );
        assert_eq!(binds, vec![SqlParam::Bool(true), SqlParam::String("ts".into())],);
    }

    // -- From<SqlParam> ------------------------------------------------------

    #[test]
    fn from_sql_param() {
        let e: E = SqlParam::I32(42).into();
        let (sql, binds) = eval(e);
        assert_eq!(sql, "$#");
        assert_eq!(binds, vec![SqlParam::I32(42)]);
    }
}
