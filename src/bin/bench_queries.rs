#![allow(dead_code)]

use std::hint::black_box;
use std::time::Instant;

use sql_query::*;
use sqlx::FromRow;

// -- Table definitions -------------------------------------------------------

define_id!(UserId);
define_id!(PostId);
define_id!(OrderId);

#[derive(Debug, FromRow, SqlCols)]
struct Users {
    id: UserId,
    name: String,
    email: String,
    age: i32,
    data: String,
    created_at: String,
}

impl Table for Users {
    type Col = UsersCol;
    type Id = UserId;
    const TABLE_NAME: &'static str = "users";
    const PRIMARY_KEY: &'static str = "id";
}

#[derive(Debug, FromRow, SqlCols)]
struct Posts {
    id: PostId,
    user_id: UserId,
    title: String,
    body: String,
}

impl Table for Posts {
    type Col = PostsCol;
    type Id = PostId;
    const TABLE_NAME: &'static str = "posts";
    const PRIMARY_KEY: &'static str = "id";
}

#[derive(Debug, FromRow, SqlCols)]
struct Orders {
    id: OrderId,
    user_id: UserId,
    amount: i32,
    status: String,
    created_at: String,
}

impl Table for Orders {
    type Col = OrdersCol;
    type Id = OrderId;
    const TABLE_NAME: &'static str = "orders";
    const PRIMARY_KEY: &'static str = "id";
}

// -- Helpers -----------------------------------------------------------------

type UExpr = Expr<Users>;


fn build_to_sql(q: impl SqlBase) -> (String, Vec<SqlParam>) {
    q.build().unwrap().into_raw()
}

fn run_bench(name: &str, iters: u64, f: impl Fn()) {
    // warmup
    for _ in 0..1000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        black_box(f());
    }
    let elapsed = start.elapsed();
    let per_iter = elapsed / iters as u32;
    println!("{name:.<50} {per_iter:>10.2?} / iter  ({iters} iters, {elapsed:.2?} total)");
}

// -- Benchmarks --------------------------------------------------------------

fn bench_expr_simple_eq() {
    black_box(UsersCol::Name.eq("alice"));
}

fn bench_expr_chained() {
    black_box(UExpr::new().column(UsersCol::Name).lower().alias("lname").eval().unwrap());
}

fn bench_expr_arithmetic() {
    black_box(
        UExpr::new()
            .column(UsersCol::Age)
            .eq()
            .column(UsersCol::Age)
            .add()
            .val(1i32)
            .eval()
            .unwrap(),
    );
}

fn bench_select_star() {
    black_box(build_to_sql(SqlQ::select::<Users>()));
}

fn bench_select_filtered() {
    black_box(build_to_sql(
        SqlQ::select::<Users>().filter([UsersCol::Name.eq("alice"), UsersCol::Age.gt(18i32)]),
    ));
}

fn bench_select_full() {
    black_box(build_to_sql(
        SqlQ::select::<Users>()
            .distinct()
            .from([
                UExpr::new().column(UsersCol::Name),
                UExpr::new().column(UsersCol::Email),
                UExpr::new().column(UsersCol::Age),
            ])
            .filter([
                UsersCol::Age.gte(18i32),
                UsersCol::Name.neq("admin"),
                UsersCol::Email.ilike("%@example.com"),
            ])
            .order_by(UExpr::new().column(UsersCol::Name), SqlOrder::Asc)
            .limit(50)
            .offset(10),
    ));
}

fn bench_select_with_join() {
    black_box(build_to_sql(
        SqlQ::select::<Users>()
            .from([UExpr::new().column(UsersCol::Name)])
            .join::<Posts, Users>(SqlJoin::Inner, PostsCol::UserId.col(), UsersCol::Id.col())
            .filter([UsersCol::Age.gt(18i32)]),
    ));
}

fn bench_select_with_group_by() {
    black_box(build_to_sql(
        SqlQ::select::<Users>()
            .from([UExpr::from(UsersCol::Age), UsersCol::Id.count().alias("count")])
            .group_by([UExpr::new().column(UsersCol::Age)])
            .having([UExpr::new().column(UsersCol::Id).count().gt().val(5i32)]),
    ));
}

fn bench_select_with_cte() {
    black_box(build_to_sql(
        SqlQ::with([("active_users", SqlQ::select::<Users>().filter([UsersCol::Age.gte(18i32)]))])
            .select::<Users>()
            .filter([UsersCol::Name.neq("deleted")]),
    ));
}

fn bench_insert_single() {
    black_box(build_to_sql(
        SqlQ::insert::<Users>()
            .values([
                UsersCol::Name.eq("alice"),
                UsersCol::Email.eq("alice@example.com"),
                UsersCol::Age.eq(30i32),
            ])
            .unwrap(),
    ));
}

fn bench_insert_batch() {
    black_box(build_to_sql(
        SqlQ::insert::<Users>()
            .values_nested([
                vec![
                    UsersCol::Name.eq("alice"),
                    UsersCol::Email.eq("alice@example.com"),
                    UsersCol::Age.eq(30i32),
                ],
                vec![
                    UsersCol::Name.eq("bob"),
                    UsersCol::Email.eq("bob@example.com"),
                    UsersCol::Age.eq(25i32),
                ],
                vec![
                    UsersCol::Name.eq("charlie"),
                    UsersCol::Email.eq("c@example.com"),
                    UsersCol::Age.eq(35i32),
                ],
                vec![
                    UsersCol::Name.eq("diana"),
                    UsersCol::Email.eq("d@example.com"),
                    UsersCol::Age.eq(28i32),
                ],
                vec![
                    UsersCol::Name.eq("eve"),
                    UsersCol::Email.eq("e@example.com"),
                    UsersCol::Age.eq(22i32),
                ],
            ])
            .unwrap(),
    ));
}

fn bench_update_with_filter() {
    black_box(build_to_sql(
        SqlQ::update::<Users>()
            .set([UsersCol::Name.eq("bob"), UsersCol::Age.eq(31i32)])
            .filter([UsersCol::Email.eq("bob@example.com")]),
    ));
}

fn bench_delete_with_filter() {
    black_box(build_to_sql(
        SqlQ::delete::<Users>().filter([UsersCol::Name.eq("alice"), UsersCol::Age.lt(18i32)]),
    ));
}

fn bench_complex_expr() {
    black_box(
        UExpr::new()
            .column(UsersCol::Age)
            .gte()
            .val(18i32)
            .and()
            .column(UsersCol::Name)
            .neq()
            .val("admin")
            .and()
            .column(UsersCol::Email)
            .ilike("%@company.com")
            .or()
            .column(UsersCol::Data)
            .is_not_null()
            .eval()
            .unwrap(),
    );
}

fn bench_raw_string_baseline() {
    // Hand-written equivalent of bench_select_full for comparison
    black_box({
        let sql = format!(
            r#"SELECT DISTINCT "users".name, "users".email, "users".age FROM "users" WHERE 1=1 AND "users".age >= $1 AND "users".name <> $2 AND "users".email ILIKE $3 ORDER BY "users".name ASC LIMIT $4 OFFSET $5"#
        );
        let binds: Vec<SqlParam> = vec![
            SqlParam::I32(18),
            SqlParam::String("admin".into()),
            SqlParam::String("%@example.com".into()),
            SqlParam::I64(50),
            SqlParam::I64(10),
        ];
        (sql, binds)
    });
}

// -- Main --------------------------------------------------------------------

fn main() {
    let iters = 100_000;

    println!("sql_query benchmarks ({iters} iterations each)");
    println!("{}", "=".repeat(80));

    println!("\n--- Expressions ---");
    run_bench("expr: simple col = val", iters, bench_expr_simple_eq);
    run_bench("expr: column + lower + alias", iters, bench_expr_chained);
    run_bench("expr: col = col + val (arithmetic)", iters, bench_expr_arithmetic);
    run_bench("expr: complex AND/OR chain", iters, bench_complex_expr);

    println!("\n--- SELECT ---");
    run_bench("select: star", iters, bench_select_star);
    run_bench("select: 2 filters", iters, bench_select_filtered);
    run_bench("select: full (distinct/cols/filters/order/limit)", iters, bench_select_full);
    run_bench("select: inner join", iters, bench_select_with_join);
    run_bench("select: group by + having", iters, bench_select_with_group_by);
    run_bench("select: with CTE", iters, bench_select_with_cte);

    println!("\n--- INSERT ---");
    run_bench("insert: single row (3 cols)", iters, bench_insert_single);
    run_bench("insert: batch (5 rows x 3 cols)", iters, bench_insert_batch);

    println!("\n--- UPDATE ---");
    run_bench("update: set 2 cols + filter", iters, bench_update_with_filter);

    println!("\n--- DELETE ---");
    run_bench("delete: 2 filters", iters, bench_delete_with_filter);

    println!("\n--- Baseline ---");
    run_bench("raw format! string (same as select full)", iters, bench_raw_string_baseline);

    println!("\n{}", "=".repeat(80));
    println!("Done. Use `cargo flamegraph --bin bench_queries` or `samply record` to profile.");
}
