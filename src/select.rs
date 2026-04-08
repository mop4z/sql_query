use sqlx::QueryBuilder;

use crate::{
    SqlBase,
    shared::{
        Table, UnbindedQuery,
        error::SqlQueryError,
        expr::{SqlExpr, SqlJoin, SqlOp, SqlOrder},
        push_conditions,
        value::SqlParam,
    },
};

pub struct SqlSelect {
    table: &'static str,
    pub(super) columns: Vec<String>,
    joined_tables: Vec<String>,
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    having: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    group_by: Vec<String>,
    order_by: Vec<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    distinct: bool,
}

impl SqlSelect {
    pub(super) fn new<T: Table>() -> Self {
        Self {
            table: T::TABLE_NAME,
            columns: Vec::new(),
            joined_tables: Vec::new(),
            filters: Vec::new(),
            having: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
        }
    }

    pub fn from<T: Table>(mut self, columns: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        for c in columns {
            self.columns.push(c.eval().unwrap().0);
        }
        self
    }

    pub fn join<T1: Table, T2: Table>(
        mut self,
        sql_join: SqlJoin,
        t1_on: SqlExpr<T1>,
        op: SqlOp,
        t2_on: SqlExpr<T2>,
    ) -> Self {
        self.joined_tables.push(format!(
            "{} JOIN \"{}\" ON {} {} {}",
            sql_join.as_ref(),
            T1::TABLE_NAME,
            t1_on.eval().unwrap().0,
            op.as_ref(),
            t2_on.eval().unwrap().0,
        ));
        self
    }

    pub fn group_by<T: Table>(mut self, columns: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        for c in columns {
            self.group_by.push(c.eval().unwrap().0);
        }
        self
    }

    pub fn order_by<T: Table>(mut self, column: SqlExpr<T>, order: SqlOrder) -> Self {
        self.order_by.push(format!("{} {}", column.eval().unwrap().0, order.as_ref()));
        self
    }

    pub fn limit(mut self, n: i64) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: i64) -> Self {
        self.offset = Some(n);
        self
    }

    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub fn filter<T: Table>(mut self, filters: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    pub fn having<T: Table>(mut self, conditions: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        self.having.extend(conditions.into_iter().map(|x| x.eval()));
        self
    }
}

impl SqlBase for SqlSelect {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error> {
        let select = if self.distinct { "SELECT DISTINCT" } else { "SELECT" };
        let columns =
            if self.columns.is_empty() { "*".to_string() } else { self.columns.join(", ") };

        let mut sql = format!("{select} {columns} FROM \"{}\"", self.table);
        for join in &self.joined_tables {
            sql.push(' ');
            sql.push_str(join);
        }

        let mut qb = QueryBuilder::new(sql);
        let mut binds = vec![];
        push_conditions("WHERE", self.filters, &mut qb, &mut binds)?;

        if !self.group_by.is_empty() {
            qb.push(" GROUP BY ");
            qb.push(self.group_by.join(", "));
        }

        push_conditions("HAVING", self.having, &mut qb, &mut binds)?;

        if !self.order_by.is_empty() {
            qb.push(" ORDER BY ");
            qb.push(self.order_by.join(", "));
        }

        if let Some(limit) = self.limit {
            qb.push(" LIMIT $1");
            binds.push(limit.into());
        }
        if let Some(offset) = self.offset {
            qb.push(" OFFSET $1");
            binds.push(offset.into());
        }

        Ok(UnbindedQuery { qb, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::expr::SqlFn;
    use crate::{SqlCols, define_id};
    use sqlx::FromRow;

    define_id!(TestId);

    #[derive(Debug, FromRow, SqlCols)]
    struct Users {
        id: TestId,
        name: String,
        age: i32,
    }

    impl Table for Users {
        type Col = UsersCol;
        type Id = TestId;
        const TABLE_NAME: &'static str = "users";
        const PRIMARY_KEY: &'static str = "id";
    }

    #[derive(Debug, FromRow, SqlCols)]
    struct Posts {
        id: TestId,
        user_id: TestId,
        title: String,
    }

    impl Table for Posts {
        type Col = PostsCol;
        type Id = TestId;
        const TABLE_NAME: &'static str = "posts";
        const PRIMARY_KEY: &'static str = "id";
    }

    type UExpr = SqlExpr<Users>;
    type PExpr = SqlExpr<Posts>;

    fn build(select: SqlSelect) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(select).unwrap();
        let bq = uq.build();
        (bq.sql, bq.binds)
    }

    #[test]
    fn select_star() {
        let (sql, binds) = build(SqlSelect::new::<Users>());
        assert_eq!(sql, r#"SELECT * FROM "users""#);
        assert!(binds.is_empty());
    }

    #[test]
    fn select_columns() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .from([UExpr::column(UsersCol::Name), UExpr::column(UsersCol::Age)]),
        );
        assert_eq!(sql, r#"SELECT "users".name, "users".age FROM "users""#);
    }

    #[test]
    fn select_column_with_alias() {
        let (sql, _) = build(
            SqlSelect::new::<Users>().from([UExpr::column(UsersCol::Name).alias("full_name")]),
        );
        assert_eq!(sql, r#"SELECT "users".name AS full_name FROM "users""#);
    }

    #[test]
    fn select_column_with_fn() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .from([UExpr::column(UsersCol::Id).col_fn(SqlFn::Count).alias("total")]),
        );
        assert_eq!(sql, r#"SELECT COUNT("users".id) AS total FROM "users""#);
    }

    #[test]
    fn select_with_single_filter() {
        let (sql, binds) =
            build(SqlSelect::new::<Users>().filter([UExpr::eq(UsersCol::Name, "alice")]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn select_with_multiple_filters() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = $1 AND "users".age = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn select_filter_is_null() {
        let (sql, binds) =
            build(SqlSelect::new::<Users>().filter([UExpr::is_null(UsersCol::Name)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name IS NULL"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn select_with_order_by() {
        let (sql, _) =
            build(SqlSelect::new::<Users>().order_by(UExpr::column(UsersCol::Name), SqlOrder::Asc));
        assert_eq!(sql, r#"SELECT * FROM "users" ORDER BY "users".name ASC"#);
    }

    #[test]
    fn select_with_multiple_order_by() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .order_by(UExpr::column(UsersCol::Name), SqlOrder::Asc)
                .order_by(UExpr::column(UsersCol::Age), SqlOrder::DescNullsFirst),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" ORDER BY "users".name ASC, "users".age DESC NULLS FIRST"#,
        );
    }

    #[test]
    fn select_with_group_by() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .from([
                    UExpr::column(UsersCol::Age),
                    UExpr::column(UsersCol::Id).col_fn(SqlFn::Count).alias("count"),
                ])
                .group_by([UExpr::column(UsersCol::Age)]),
        );
        assert_eq!(
            sql,
            r#"SELECT "users".age, COUNT("users".id) AS count FROM "users" GROUP BY "users".age"#,
        );
    }

    #[test]
    fn select_with_limit() {
        let (sql, binds) = build(SqlSelect::new::<Users>().limit(10));
        assert_eq!(sql, r#"SELECT * FROM "users" LIMIT $1"#);
        assert_eq!(binds, vec![SqlParam::I64(10)]);
    }

    #[test]
    fn select_with_offset() {
        let (sql, binds) = build(SqlSelect::new::<Users>().offset(20));
        assert_eq!(sql, r#"SELECT * FROM "users" OFFSET $1"#);
        assert_eq!(binds, vec![SqlParam::I64(20)]);
    }

    #[test]
    fn select_with_limit_and_offset_renumbered() {
        let (sql, binds) = build(SqlSelect::new::<Users>().limit(10).offset(20));
        assert_eq!(sql, r#"SELECT * FROM "users" LIMIT $1 OFFSET $2"#);
        assert_eq!(binds, vec![SqlParam::I64(10), SqlParam::I64(20)]);
    }

    #[test]
    fn select_distinct() {
        let (sql, _) = build(SqlSelect::new::<Users>().distinct());
        assert_eq!(sql, r#"SELECT DISTINCT * FROM "users""#);
    }

    #[test]
    fn select_distinct_with_columns() {
        let (sql, _) =
            build(SqlSelect::new::<Users>().distinct().from([UExpr::column(UsersCol::Name)]));
        assert_eq!(sql, r#"SELECT DISTINCT "users".name FROM "users""#);
    }

    #[test]
    fn select_with_join() {
        let (sql, _) = build(SqlSelect::new::<Users>().join::<Posts, Users>(
            SqlJoin::Left,
            PExpr::column(PostsCol::UserId),
            SqlOp::Eq,
            UExpr::column(UsersCol::Id),
        ));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" LEFT JOIN "posts" ON "posts".user_id = "users".id"#,
        );
    }

    #[test]
    fn select_filters_with_limit_offset_renumbered() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)])
                .limit(10)
                .offset(5),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = $1 AND "users".age = $2 LIMIT $3 OFFSET $4"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("alice".into()),
                SqlParam::I32(30),
                SqlParam::I64(10),
                SqlParam::I64(5),
            ],
        );
    }

    #[test]
    fn select_full_query() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .distinct()
                .from([UExpr::column(UsersCol::Name), UExpr::column(UsersCol::Age)])
                .filter([UExpr::eq(UsersCol::Age, 18i32)])
                .order_by(UExpr::column(UsersCol::Name), SqlOrder::AscNullsLast)
                .limit(50)
                .offset(10),
        );
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "users".name, "users".age FROM "users" WHERE 1=1 AND "users".age = $1 ORDER BY "users".name ASC NULLS LAST LIMIT $2 OFFSET $3"#,
        );
        assert_eq!(binds, vec![SqlParam::I32(18), SqlParam::I64(50), SqlParam::I64(10)],);
    }

    #[test]
    fn filter_with_val_fn_now_no_bind() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                .op(SqlOp::Eq)
                .val(SqlParam::String("ignored".into()))
                .val_fn(SqlFn::Now)]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = NOW()"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_with_val_fn_true_no_bind() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                .op(SqlOp::Eq)
                .val(SqlParam::Bool(true))
                .val_fn(SqlFn::True)]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = TRUE"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_with_val_fn_lower_keeps_bind() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                .op(SqlOp::Eq)
                .val(SqlParam::String("alice".into()))
                .val_fn(SqlFn::Lower)]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = LOWER($1)"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn select_with_having() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .from([
                    UExpr::column(UsersCol::Age),
                    UExpr::column(UsersCol::Id).col_fn(SqlFn::Count).alias("count"),
                ])
                .group_by([UExpr::column(UsersCol::Age)])
                .having([UExpr::column(UsersCol::Id)
                    .col_fn(SqlFn::Count)
                    .op(SqlOp::Eq)
                    .val(SqlParam::I32(5))]),
        );
        assert_eq!(
            sql,
            r#"SELECT "users".age, COUNT("users".id) AS count FROM "users" GROUP BY "users".age HAVING 1=1 AND COUNT("users".id) = $1"#,
        );
        assert_eq!(binds, vec![SqlParam::I32(5)]);
    }

    #[test]
    fn select_having_with_filters_renumbered() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .from([
                    UExpr::column(UsersCol::Age),
                    UExpr::column(UsersCol::Id).col_fn(SqlFn::Count).alias("count"),
                ])
                .filter([UExpr::eq(UsersCol::Name, "alice")])
                .group_by([UExpr::column(UsersCol::Age)])
                .having([UExpr::column(UsersCol::Id)
                    .col_fn(SqlFn::Count)
                    .op(SqlOp::Eq)
                    .val(SqlParam::I32(3))]),
        );
        assert_eq!(
            sql,
            r#"SELECT "users".age, COUNT("users".id) AS count FROM "users" WHERE 1=1 AND "users".name = $1 GROUP BY "users".age HAVING 1=1 AND COUNT("users".id) = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(3)]);
    }

    #[test]
    fn filter_with_subquery() {
        let sub = SqlSelect::new::<Posts>()
            .from([PExpr::column(PostsCol::UserId)])
            .filter([PExpr::eq(PostsCol::Title, "hello")]);

        let (sql, binds) = build(SqlSelect::new::<Users>().filter([
            UExpr::column(UsersCol::Id).op(SqlOp::In).select(sub),
            UExpr::eq(UsersCol::Name, "alice"),
        ]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts" WHERE 1=1 AND "posts".title = $1) AND "users".name = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("hello".into()), SqlParam::String("alice".into())],);
    }

    #[test]
    fn filter_with_subquery_no_binds() {
        let sub = SqlSelect::new::<Posts>().from([PExpr::column(PostsCol::UserId)]);

        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::column(UsersCol::Id).op(SqlOp::In).select(sub)]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts")"#,
        );
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_subquery_ignores_val_and_val_fn() {
        let sub = SqlSelect::new::<Posts>().from([PExpr::column(PostsCol::UserId)]);

        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Id)
                .op(SqlOp::In)
                .val(SqlParam::I32(999))
                .val_fn(SqlFn::Lower)
                .select(sub)]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts")"#,
        );
        assert!(binds.is_empty());
    }

    #[test]
    fn having_with_subquery() {
        let sub = SqlSelect::new::<Posts>()
            .from([PExpr::column(PostsCol::UserId)])
            .filter([PExpr::eq(PostsCol::Title, "test")]);

        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .from([
                    UExpr::column(UsersCol::Age),
                    UExpr::column(UsersCol::Id).col_fn(SqlFn::Count).alias("count"),
                ])
                .group_by([UExpr::column(UsersCol::Age)])
                .having([UExpr::column(UsersCol::Id).op(SqlOp::In).select(sub)]),
        );
        assert_eq!(
            sql,
            r#"SELECT "users".age, COUNT("users".id) AS count FROM "users" GROUP BY "users".age HAVING 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts" WHERE 1=1 AND "posts".title = $1)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("test".into())]);
    }

    #[test]
    fn filter_gt() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::column(UsersCol::Age).op(SqlOp::Gt).val(SqlParam::I32(18))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age > $1"#);
        assert_eq!(binds, vec![SqlParam::I32(18)]);
    }

    #[test]
    fn filter_gte() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::column(UsersCol::Age).op(SqlOp::Gte).val(SqlParam::I32(18))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age >= $1"#);
    }

    #[test]
    fn filter_lt() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::column(UsersCol::Age).op(SqlOp::Lt).val(SqlParam::I32(65))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age < $1"#);
    }

    #[test]
    fn filter_lte() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::column(UsersCol::Age).op(SqlOp::Lte).val(SqlParam::I32(65))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age <= $1"#);
    }

    #[test]
    fn filter_like() {
        let (sql, binds) =
            build(
                SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                    .op(SqlOp::Like)
                    .val(SqlParam::String("%alice%".into()))]),
            );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name LIKE $1"#);
        assert_eq!(binds, vec![SqlParam::String("%alice%".into())]);
    }

    #[test]
    fn filter_ilike() {
        let (sql, _) =
            build(
                SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                    .op(SqlOp::ILike)
                    .val(SqlParam::String("%alice%".into()))]),
            );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name ILIKE $1"#);
    }

    #[test]
    fn filter_between() {
        let (sql, binds) =
            build(SqlSelect::new::<Users>().filter([UExpr::between(UsersCol::Age, 18i32, 65i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age BETWEEN $1 AND $2"#);
        assert_eq!(binds, vec![SqlParam::I32(18), SqlParam::I32(65)]);
    }

    #[test]
    fn filter_exists() {
        let sub = SqlSelect::new::<Posts>().filter([PExpr::eq(PostsCol::Title, "hello")]);

        let (sql, binds) = build(SqlSelect::new::<Users>().filter([UExpr::exists(sub)]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND EXISTS (SELECT * FROM "posts" WHERE 1=1 AND "posts".title = $1)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("hello".into())]);
    }

    #[test]
    fn filter_not_exists() {
        let sub = SqlSelect::new::<Posts>().filter([PExpr::eq(PostsCol::Title, "hello")]);

        let (sql, _) = build(SqlSelect::new::<Users>().filter([UExpr::not_exists(sub)]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND NOT EXISTS (SELECT * FROM "posts" WHERE 1=1 AND "posts".title = $1)"#,
        );
    }

    #[test]
    fn filter_any() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([
            UExpr::column(UsersCol::Name).op(SqlOp::Any).val(SqlParam::String("alice".into())),
        ]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = ANY ($1)"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn filter_all() {
        let (sql, _) = build(SqlSelect::new::<Users>().filter([
            UExpr::column(UsersCol::Name).op(SqlOp::All).val(SqlParam::String("alice".into())),
        ]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = ALL ($1)"#);
    }

    #[test]
    fn select_with_sum() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .from([UExpr::column(UsersCol::Age).col_fn(SqlFn::Sum).alias("total_age")]),
        );
        assert_eq!(sql, r#"SELECT SUM("users".age) AS total_age FROM "users""#);
    }

    #[test]
    fn select_with_avg_min_max() {
        let (sql, _) = build(SqlSelect::new::<Users>().from([
            UExpr::column(UsersCol::Age).col_fn(SqlFn::Avg).alias("avg_age"),
            UExpr::column(UsersCol::Age).col_fn(SqlFn::Min).alias("min_age"),
            UExpr::column(UsersCol::Age).col_fn(SqlFn::Max).alias("max_age"),
        ]));
        assert_eq!(
            sql,
            r#"SELECT AVG("users".age) AS avg_age, MIN("users".age) AS min_age, MAX("users".age) AS max_age FROM "users""#,
        );
    }

    #[test]
    fn select_with_upper() {
        let (sql, _) = build(
            SqlSelect::new::<Users>().from([UExpr::column(UsersCol::Name).col_fn(SqlFn::Upper)]),
        );
        assert_eq!(sql, r#"SELECT UPPER("users".name) FROM "users""#);
    }

    #[test]
    fn filter_with_coalesce() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::column(UsersCol::Name)
                .op(SqlOp::Eq)
                .val(SqlParam::String("default".into()))
                .val_fn(SqlFn::Coalesce)]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = COALESCE($1)"#);
        assert_eq!(binds, vec![SqlParam::String("default".into())]);
    }

    #[test]
    fn filter_or() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::eq(UsersCol::Name, "alice").or(UExpr::eq(UsersCol::Name, "bob"))]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1 OR "users".name = $2)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::String("bob".into())],);
    }

    #[test]
    fn filter_and_grouped() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::eq(UsersCol::Name, "alice")
                .and(UExpr::column(UsersCol::Age).op(SqlOp::Gt).val(SqlParam::I32(18)))]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1 AND "users".age > $2)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(18)]);
    }

    #[test]
    fn filter_or_with_other_filters() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([
            UExpr::eq(UsersCol::Name, "alice").or(UExpr::eq(UsersCol::Name, "bob")),
            UExpr::column(UsersCol::Age).op(SqlOp::Gte).val(SqlParam::I32(18)),
        ]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1 OR "users".name = $2) AND "users".age >= $3"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("alice".into()),
                SqlParam::String("bob".into()),
                SqlParam::I32(18),
            ],
        );
    }

    #[test]
    fn filter_nested_or_and() {
        let (sql, binds) =
            build(
                SqlSelect::new::<Users>().filter([UExpr::eq(UsersCol::Name, "alice")
                    .or(UExpr::eq(UsersCol::Name, "bob")
                        .and(UExpr::column(UsersCol::Age).op(SqlOp::Gt).val(SqlParam::I32(30))))]),
            );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1 OR ("users".name = $2 AND "users".age > $3))"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("alice".into()),
                SqlParam::String("bob".into()),
                SqlParam::I32(30),
            ],
        );
    }
}
