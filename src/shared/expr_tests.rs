use super::*;
use crate::{SqlCols, define_id};
use sqlx::FromRow;

define_id!(TestId);

#[derive(Debug, FromRow, SqlCols)]
#[allow(dead_code)]
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
    let (sql, binds) = eval(E::new().column(TC::Name).eq("alice"));
    assert_eq!(sql, r#""test_table".name = $#"#);
    assert_eq!(binds, vec![SqlParam::String("alice".into())]);
}

#[test]
fn column_neq_val() {
    let (sql, _) = eval(E::new().column(TC::Name).neq("bob"));
    assert_eq!(sql, r#""test_table".name != $#"#);
}

#[test]
fn column_gt_val() {
    let (sql, _) = eval(E::new().column(TC::Age).gt(SqlParam::I32(18)));
    assert_eq!(sql, r#""test_table".age > $#"#);
}

#[test]
fn column_gte_val() {
    let (sql, _) = eval(E::new().column(TC::Age).gte(SqlParam::I32(18)));
    assert_eq!(sql, r#""test_table".age >= $#"#);
}

#[test]
fn column_lt_val() {
    let (sql, _) = eval(E::new().column(TC::Age).lt(SqlParam::I32(18)));
    assert_eq!(sql, r#""test_table".age < $#"#);
}

#[test]
fn column_lte_val() {
    let (sql, _) = eval(E::new().column(TC::Age).lte(SqlParam::I32(18)));
    assert_eq!(sql, r#""test_table".age <= $#"#);
}

// -- Self-referential arithmetic -----------------------------------------

#[test]
fn self_ref_add() {
    let (sql, binds) =
        eval(E::new().column(TC::Age).eq(E::new().column(TC::Age).add(SqlParam::I32(1))));
    assert_eq!(sql, r#""test_table".age = "test_table".age + $#"#);
    assert_eq!(binds, vec![SqlParam::I32(1)]);
}

#[test]
fn self_ref_sub() {
    let (sql, _) =
        eval(E::new().column(TC::Age).eq(E::new().column(TC::Age).sub(SqlParam::I32(5))));
    assert_eq!(sql, r#""test_table".age = "test_table".age - $#"#);
}

#[test]
fn self_ref_mul() {
    let (sql, _) =
        eval(E::new().column(TC::Age).eq(E::new().column(TC::Age).mul(SqlParam::I32(2))));
    assert_eq!(sql, r#""test_table".age = "test_table".age * $#"#);
}

#[test]
fn self_ref_div() {
    let (sql, _) =
        eval(E::new().column(TC::Age).eq(E::new().column(TC::Age).div(SqlParam::I32(3))));
    assert_eq!(sql, r#""test_table".age = "test_table".age / $#"#);
}

// -- NOW / NULL / TRUE / FALSE -------------------------------------------

#[test]
fn eq_now() {
    let (sql, binds) = eval(E::new().column(TC::CreatedAt).eq(E::new().now()));
    assert_eq!(sql, r#""test_table".created_at = NOW()"#);
    assert!(binds.is_empty());
}

#[test]
fn eq_null() {
    let (sql, _) = eval(E::new().column(TC::Email).eq(E::new().null()));
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
fn in_array_becomes_any() {
    let (sql, binds) = eval(E::new().column(TC::Age).in_(SqlParam::I32Array(vec![1, 2, 3])));
    assert_eq!(sql, r#""test_table".age = ANY($#)"#);
    assert_eq!(binds, vec![SqlParam::I32Array(vec![1, 2, 3])]);
}

#[test]
fn not_in_array_becomes_all() {
    let (sql, _) = eval(E::new().column(TC::Age).not_in(SqlParam::I32Array(vec![1])));
    assert_eq!(sql, r#""test_table".age <> ALL($#)"#);
}

#[test]
fn in_scalar_stays_in() {
    let (sql, _) = eval(E::new().column(TC::Age).in_(SqlParam::I32(1)));
    assert_eq!(sql, r#""test_table".age IN ($#)"#);
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
    let (sql, binds) = eval(E::new().column(TC::Age).between(SqlParam::I32(10), SqlParam::I32(20)));
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
    let (sql, _) = eval(E::new().column(TC::Name).eq("alice").and(TC::Age.gt(SqlParam::I32(18))));
    assert_eq!(sql, r#"("test_table".name = $#) AND "test_table".age > $#"#);
}

#[test]
fn or_chain() {
    let (sql, _) = eval(E::new().column(TC::Name).eq("alice").or(TC::Name.eq("bob")));
    assert_eq!(sql, r#"("test_table".name = $#) OR "test_table".name = $#"#);
}

#[test]
fn and_bare_chain() {
    let (sql, _) =
        eval(E::new().column(TC::Name).eq("alice").and_bare(TC::Age.gt(SqlParam::I32(18))));
    assert_eq!(sql, r#""test_table".name = $# AND "test_table".age > $#"#);
}

#[test]
fn or_bare_chain() {
    let (sql, _) = eval(E::new().column(TC::Name).eq("alice").or_bare(TC::Name.eq("bob")));
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
        E::new().column(TC::Name).eq(E::new()
            .if_(E::new().val(SqlParam::Bool(true)))
            .then_(SqlParam::String("yes".into()))
            .else_(E::new().null())),
    );
    assert_eq!(sql, r#""test_table".name = CASE WHEN $# THEN $# ELSE NULL END"#);
    assert_eq!(binds, vec![SqlParam::Bool(true), SqlParam::String("yes".into())]);
}

#[test]
fn if_then_else_with_column() {
    let (sql, binds) = eval(
        E::new().column(TC::CreatedAt).eq(E::new()
            .if_(E::new().val(SqlParam::Bool(true)))
            .then_(TC::CreatedAt)
            .else_(E::new().null())),
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
    let (sql, binds) = eval(E::new().column(TC::Data).json_get("key"));
    assert_eq!(sql, r#""test_table".data -> 'key'"#);
    assert!(binds.is_empty());
}

#[test]
fn json_get_text_key() {
    let (sql, binds) = eval(E::new().column(TC::Data).json_get_text("key"));
    assert_eq!(sql, r#""test_table".data ->> 'key'"#);
    assert!(binds.is_empty());
}

#[test]
fn json_get_text_escapes_single_quote() {
    let (sql, _) = eval(E::new().column(TC::Data).json_get_text("k'ey"));
    assert_eq!(sql, r#""test_table".data ->> 'k''ey'"#);
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
    assert_eq!(sql, r#""test_table".data ->> 'key' = $#"#);
    assert_eq!(binds, vec![SqlParam::String("val".into())]);
}

#[test]
fn json_path() {
    let (sql, _) = eval(E::new().column(TC::Data).json_path("{a,b}"));
    assert_eq!(sql, r#""test_table".data #> '{a,b}'"#);
}

#[test]
fn json_path_text() {
    let (sql, _) = eval(E::new().column(TC::Data).json_path_text("{a,b}"));
    assert_eq!(sql, r#""test_table".data #>> '{a,b}'"#);
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
    let e = E::new().column(TC::Name).eq("alice");
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
    let (sql, _) = eval(E::new().column(TC::Name).eq("a").or_bare(TC::Name.eq("b")).paren());
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
        E::new().column(TC::CreatedAt).eq(E::new()
            .if_(E::new().val(SqlParam::Bool(true)))
            .then_(TC::CreatedAt.greatest(SqlParam::String("ts".into())))
            .else_(E::new().null())),
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
    let (sql, binds) = eval(E::new().column(TC::Name).eq(inner));
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
        eval(E::new().column(TC::Age).eq(E::new().func("make_interval", "hours => ", 5i32)));
    assert_eq!(sql, r#""test_table".age = make_interval(hours => $#)"#);
    assert_eq!(binds, vec![SqlParam::I32(5)]);
}

// -- Window functions ----------------------------------------------------

#[test]
fn row_number_over_partition_order() {
    let (sql, _) = eval(E::row_number().over(
        WindowSpec::new().partition_by(TC::Name.col()).order_by(TC::Age.col(), SqlOrder::Desc),
    ));
    assert_eq!(
        sql,
        r#"ROW_NUMBER() OVER (PARTITION BY "test_table".name ORDER BY "test_table".age DESC)"#,
    );
}

#[test]
fn sum_over_empty() {
    let (sql, _) = eval(E::new().column(TC::Age).sum().over(WindowSpec::new()));
    assert_eq!(sql, r#"SUM("test_table".age) OVER ()"#);
}

#[test]
fn rank_over_order_only() {
    let (sql, _) = eval(E::rank().over(WindowSpec::new().order_by(TC::Age.col(), SqlOrder::Asc)));
    assert_eq!(sql, r#"RANK() OVER (ORDER BY "test_table".age ASC)"#);
}

#[test]
fn count_over_multiple_partitions() {
    let (sql, _) = eval(
        E::new()
            .column(TC::Age)
            .count()
            .over(WindowSpec::new().partition_by(TC::Name.col()).partition_by(TC::Email.col())),
    );
    assert_eq!(
        sql,
        r#"COUNT("test_table".age) OVER (PARTITION BY "test_table".name, "test_table".email)"#,
    );
}

#[test]
fn lag_over() {
    let (sql, _) =
        eval(E::lag(TC::Age.col()).over(WindowSpec::new().order_by(TC::Age.col(), SqlOrder::Asc)));
    assert_eq!(sql, r#"LAG("test_table".age) OVER (ORDER BY "test_table".age ASC)"#,);
}

#[test]
fn first_value_with_frame() {
    let (sql, _) = eval(
        E::first_value(TC::Name.col()).over(
            WindowSpec::new()
                .order_by(TC::Age.col(), SqlOrder::Asc)
                .rows_between(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
        ),
    );
    assert_eq!(
        sql,
        r#"FIRST_VALUE("test_table".name) OVER (ORDER BY "test_table".age ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"#,
    );
}

#[test]
fn t_star_qualifies_with_table_name() {
    let (sql, binds) = eval(E::t_star());
    assert_eq!(sql, r#""test_table".*"#);
    assert!(binds.is_empty());
}

#[test]
fn filter_and_over() {
    let (sql, binds) = eval(
        E::star()
            .count()
            .filter(TC::Age.gt(18i32))
            .over(WindowSpec::new().partition_by(TC::Name.col())),
    );
    assert_eq!(
        sql,
        r#"COUNT(*) FILTER (WHERE "test_table".age > $#) OVER (PARTITION BY "test_table".name)"#,
    );
    assert_eq!(binds, vec![SqlParam::I32(18)]);
}

#[test]
fn window_with_alias() {
    let (sql, _) = eval(
        E::dense_rank()
            .over(WindowSpec::new().order_by(TC::Age.col(), SqlOrder::Desc))
            .alias("rank"),
    );
    assert_eq!(sql, r#"DENSE_RANK() OVER (ORDER BY "test_table".age DESC) AS rank"#,);
}
