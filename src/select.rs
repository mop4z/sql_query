use crate::{
    SqlBase,
    set_op::SqlSetOp,
    shared::{
        Cte, Table, UnbindedQuery,
        error::SqlQueryError,
        expr::{EvalExpr, Expr, SqlJoin, SqlOrder},
        prepend_ctes, push_conditions,
        value::SqlParam,
    },
};

/// Builder for SQL SELECT statements with optional joins, filters, grouping, and ordering.
pub struct SqlSelect {
    table: &'static str,
    pub(super) columns: Vec<String>,
    joined_tables: Vec<String>,
    join_binds: Vec<SqlParam>,
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    having: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    group_by: Vec<String>,
    order_by: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
    exists: bool,
    for_update: bool,
    ctes: Vec<Cte>,
}

impl SqlSelect {
    pub(super) fn new<T: Table>() -> Self {
        Self::new_with::<T>(vec![])
    }

    pub(super) fn new_with<T: Table>(ctes: Vec<Cte>) -> Self {
        Self {
            table: T::TABLE_NAME,
            columns: Vec::new(),
            joined_tables: Vec::new(),
            join_binds: Vec::new(),
            filters: Vec::new(),
            having: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
            exists: false,
            for_update: false,
            ctes,
        }
    }

    /// Set the SELECT column list. Omit to select `*`.
    ///
    /// Accepts column expressions — use `Col::col()`, `Col::count().alias("n")`,
    /// or `Expr::new().val(literal)` for computed columns.
    pub fn from(mut self, columns: impl IntoIterator<Item = impl EvalExpr>) -> Self {
        for c in columns {
            self.columns.push(c.eval().unwrap().0);
        }
        self
    }

    /// Adds a JOIN clause: `{join} JOIN "T1" ON t1_col = t2_col`.
    pub fn join<T1: Table, T2: Table>(
        mut self,
        sql_join: SqlJoin,
        t1_col: impl Into<Expr<T1>>,
        t2_col: impl Into<Expr<T2>>,
    ) -> Self {
        let mut s = String::with_capacity(64);
        s.push_str(sql_join.as_ref());
        s.push_str(" JOIN \"");
        s.push_str(T1::TABLE_NAME);
        s.push_str("\" ON ");
        let (t1_sql, t1_binds) = t1_col.into().eval().unwrap();
        s.push_str(&t1_sql);
        s.push_str(" = ");
        let (t2_sql, t2_binds) = t2_col.into().eval().unwrap();
        s.push_str(&t2_sql);
        self.joined_tables.push(s);
        self.join_binds.extend(t1_binds);
        self.join_binds.extend(t2_binds);
        self
    }

    /// Adds a `{join} JOIN LATERAL (subquery) alias ON TRUE` clause.
    pub fn join_lateral(mut self, sql_join: SqlJoin, alias: &str, subquery: impl SqlBase) -> Self {
        let uq = subquery.build().expect("join_lateral build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        let mut s = String::with_capacity(sub_sql.len() + 32);
        s.push_str(sql_join.as_ref());
        s.push_str(" JOIN LATERAL (");
        s.push_str(&sub_sql);
        s.push_str(") ");
        s.push_str(alias);
        s.push_str(" ON TRUE");
        self.joined_tables.push(s);
        self.join_binds.extend(sub_binds);
        self
    }

    /// Adds GROUP BY columns to the query.
    pub fn group_by(mut self, columns: impl IntoIterator<Item = impl EvalExpr>) -> Self {
        for c in columns {
            self.group_by.push(c.eval().unwrap().0);
        }
        self
    }

    /// Appends an ORDER BY clause for the given column and direction.
    pub fn order_by(mut self, column: impl EvalExpr, order: SqlOrder) -> Self {
        let mut s = column.eval().unwrap().0;
        s.push(' ');
        s.push_str(order.as_ref());
        self.order_by.push(s);
        self
    }

    /// Sets the maximum number of rows to return.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the number of rows to skip before returning results.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Enables SELECT DISTINCT to eliminate duplicate rows.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Wrap the entire query as `SELECT EXISTS (SELECT ...)`.
    /// The result is a single boolean — use `.bind_scalar::<bool>().fetch_one()`.
    pub fn exists(mut self) -> Self {
        self.exists = true;
        self
    }

    /// Append `FOR UPDATE` — acquire row-level exclusive locks on selected rows.
    pub fn for_update(mut self) -> Self {
        self.for_update = true;
        self
    }

    /// Adds WHERE conditions that are ANDed together.
    pub fn filter<E: EvalExpr>(mut self, filters: impl IntoIterator<Item = E>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    /// Adds HAVING conditions applied after GROUP BY.
    pub fn having<E: EvalExpr>(mut self, conditions: impl IntoIterator<Item = E>) -> Self {
        self.having.extend(conditions.into_iter().map(|x| x.eval()));
        self
    }

    /// Combine with another SELECT using `UNION` (deduplicates rows).
    pub fn union(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new(self, other)
    }

    /// Combine with another SELECT using `UNION ALL` (keeps duplicates).
    pub fn union_all(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new_all(self, other)
    }

    /// Combine with another SELECT using `INTERSECT`.
    pub fn intersect(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new_intersect(self, other)
    }

    /// Combine with another SELECT using `INTERSECT ALL`.
    pub fn intersect_all(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new_intersect_all(self, other)
    }

    /// Combine with another SELECT using `EXCEPT`.
    pub fn except(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new_except(self, other)
    }

    /// Combine with another SELECT using `EXCEPT ALL`.
    pub fn except_all(self, other: SqlSelect) -> SqlSetOp {
        SqlSetOp::new_except_all(self, other)
    }
}

impl SqlBase for SqlSelect {
    fn build(self) -> Result<UnbindedQuery, sqlx::Error> {
        let mut sql = String::with_capacity(128);
        if self.exists {
            sql.push_str("SELECT EXISTS (");
        }
        let select = if self.distinct { "SELECT DISTINCT " } else { "SELECT " };
        sql.push_str(select);
        if self.columns.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.columns.join(", "));
        }
        sql.push_str(" FROM \"");
        sql.push_str(self.table);
        sql.push('"');
        for join in &self.joined_tables {
            sql.push(' ');
            sql.push_str(join);
        }

        let mut binds = self.join_binds;
        prepend_ctes(self.ctes, &mut sql, &mut binds);
        push_conditions("WHERE", self.filters, &mut sql, &mut binds)?;

        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        push_conditions("HAVING", self.having, &mut sql, &mut binds)?;

        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT $#");
            binds.push(SqlParam::I64(limit as i64));
        }
        if let Some(offset) = self.offset {
            sql.push_str(" OFFSET $#");
            binds.push(SqlParam::I64(offset as i64));
        }

        if self.for_update {
            sql.push_str(" FOR UPDATE");
        }

        if self.exists {
            sql.push(')');
        }

        Ok(UnbindedQuery { sql, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqlCols, define_id};
    use sqlx::FromRow;

    define_id!(TestId);

    #[derive(Debug, FromRow, SqlCols)]
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

    type UExpr = Expr<Users>;
    type PExpr = Expr<Posts>;

    fn build(select: SqlSelect) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(select).unwrap();
        let bq = uq.bind();
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
                .from([UExpr::new().column(UsersCol::Name), UExpr::new().column(UsersCol::Age)]),
        );
        assert_eq!(sql, r#"SELECT "users".name, "users".age FROM "users""#);
    }

    #[test]
    fn select_column_with_alias() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .from([UExpr::new().column(UsersCol::Name).alias("full_name")]),
        );
        assert_eq!(sql, r#"SELECT "users".name AS full_name FROM "users""#);
    }

    #[test]
    fn select_column_with_fn() {
        let (sql, _) = build(SqlSelect::new::<Users>().from([UsersCol::Id.count().alias("total")]));
        assert_eq!(sql, r#"SELECT COUNT("users".id) AS total FROM "users""#);
    }

    #[test]
    fn select_with_single_filter() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice")]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn select_with_multiple_filters() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = $1 AND "users".age = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn select_filter_is_null() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([UsersCol::Name.is_null()]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name IS NULL"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn select_with_order_by() {
        let (sql, _) = build(
            SqlSelect::new::<Users>().order_by(UExpr::new().column(UsersCol::Name), SqlOrder::Asc),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" ORDER BY "users".name ASC"#);
    }

    #[test]
    fn select_with_multiple_order_by() {
        let (sql, _) = build(
            SqlSelect::new::<Users>()
                .order_by(UExpr::new().column(UsersCol::Name), SqlOrder::Asc)
                .order_by(UExpr::new().column(UsersCol::Age), SqlOrder::DescNullsFirst),
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
                    UExpr::from(UExpr::new().column(UsersCol::Age)),
                    UsersCol::Id.count().alias("count"),
                ])
                .group_by([UExpr::new().column(UsersCol::Age)]),
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
            build(SqlSelect::new::<Users>().distinct().from([UExpr::new().column(UsersCol::Name)]));
        assert_eq!(sql, r#"SELECT DISTINCT "users".name FROM "users""#);
    }

    #[test]
    fn select_with_cross_join_lateral() {
        let sub = SqlSelect::new::<Posts>()
            .from([PExpr::new().column(PostsCol::Title)])
            .filter([PostsCol::UserId.eq(1i32)])
            .limit(1);
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .join_lateral(SqlJoin::Cross, "lat", sub)
                .filter([UsersCol::Name.eq("alice")]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" CROSS JOIN LATERAL (SELECT "posts".title FROM "posts" WHERE 1=1 AND "posts".user_id = $1 LIMIT $2) lat ON TRUE WHERE 1=1 AND "users".name = $3"#,
        );
        assert_eq!(
            binds,
            vec![SqlParam::I32(1), SqlParam::I64(1), SqlParam::String("alice".into())],
        );
    }

    #[test]
    fn select_with_join() {
        let (sql, _) = build(SqlSelect::new::<Users>().join::<Posts, Users>(
            SqlJoin::Left,
            PostsCol::UserId.col(),
            UsersCol::Id.col(),
        ));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" LEFT JOIN "posts" ON "posts".user_id = "users".id"#,
        );
    }

    #[test]
    fn select_with_inner_join() {
        let (sql, _) = build(SqlSelect::new::<Users>().join::<Posts, Users>(
            SqlJoin::Inner,
            PostsCol::UserId.col(),
            UsersCol::Id.col(),
        ));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" INNER JOIN "posts" ON "posts".user_id = "users".id"#,
        );
    }

    #[test]
    fn select_filters_with_limit_offset_renumbered() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)])
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
                .from([UExpr::new().column(UsersCol::Name), UExpr::new().column(UsersCol::Age)])
                .filter([UsersCol::Age.eq(18i32)])
                .order_by(UExpr::new().column(UsersCol::Name), SqlOrder::AscNullsLast)
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
            SqlSelect::new::<Users>()
                .filter([UExpr::new().column(UsersCol::Name).eq(UExpr::new().now())]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = NOW()"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_with_val_fn_true_no_bind() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UExpr::new().column(UsersCol::Name).eq(UExpr::new().raw("TRUE"))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = TRUE"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_with_val_fn_lower_keeps_bind() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::new()
                .column(UsersCol::Name)
                .eq(SqlParam::String("alice".into()))
                .wrap_raw("LOWER")]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND LOWER("users".name = $1)"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn select_with_having() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .from([
                    UExpr::from(UExpr::new().column(UsersCol::Age)),
                    UsersCol::Id.count().alias("count"),
                ])
                .group_by([UExpr::new().column(UsersCol::Age)])
                .having([UsersCol::Id.count().eq(SqlParam::I32(5))]),
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
                    UExpr::from(UExpr::new().column(UsersCol::Age)),
                    UsersCol::Id.count().alias("count"),
                ])
                .filter([UsersCol::Name.eq("alice")])
                .group_by([UExpr::new().column(UsersCol::Age)])
                .having([UsersCol::Id.count().eq(SqlParam::I32(3))]),
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
            .from([PExpr::new().column(PostsCol::UserId)])
            .filter([PostsCol::Title.eq("hello")]);

        let (sql, binds) = build(SqlSelect::new::<Users>().filter([
            UExpr::new().column(UsersCol::Id).in_select(sub),
            UsersCol::Name.eq("alice"),
        ]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts" WHERE 1=1 AND "posts".title = $1) AND "users".name = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("hello".into()), SqlParam::String("alice".into())],);
    }

    #[test]
    fn filter_with_subquery_no_binds() {
        let sub = SqlSelect::new::<Posts>().from([PExpr::new().column(PostsCol::UserId)]);

        let (sql, binds) = build(
            SqlSelect::new::<Users>().filter([UExpr::new().column(UsersCol::Id).in_select(sub)]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND "users".id IN (SELECT "posts".user_id FROM "posts")"#,
        );
        assert!(binds.is_empty());
    }

    #[test]
    fn filter_gt() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([UsersCol::Age.gt(18i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age > $1"#);
        assert_eq!(binds, vec![SqlParam::I32(18)]);
    }

    #[test]
    fn filter_gte() {
        let (sql, _) = build(SqlSelect::new::<Users>().filter([UsersCol::Age.gte(18i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age >= $1"#);
    }

    #[test]
    fn filter_lt() {
        let (sql, _) = build(SqlSelect::new::<Users>().filter([UsersCol::Age.lt(65i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age < $1"#);
    }

    #[test]
    fn filter_lte() {
        let (sql, _) = build(SqlSelect::new::<Users>().filter([UsersCol::Age.lte(65i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age <= $1"#);
    }

    #[test]
    fn filter_like() {
        let (sql, binds) =
            build(SqlSelect::new::<Users>().filter([UsersCol::Name.like("%alice%")]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name LIKE $1"#);
        assert_eq!(binds, vec![SqlParam::String("%alice%".into())]);
    }

    #[test]
    fn filter_ilike() {
        let (sql, _) = build(SqlSelect::new::<Users>().filter([UsersCol::Name.ilike("%alice%")]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name ILIKE $1"#);
    }

    #[test]
    fn filter_between() {
        let (sql, binds) =
            build(SqlSelect::new::<Users>().filter([UsersCol::Age.between(18i32, 65i32)]));
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".age BETWEEN $1 AND $2"#);
        assert_eq!(binds, vec![SqlParam::I32(18), SqlParam::I32(65)]);
    }

    #[test]
    fn filter_exists() {
        let sub = SqlSelect::new::<Posts>().filter([PostsCol::Title.eq("hello")]);

        let (sql, binds) = build(SqlSelect::new::<Users>().filter([UExpr::new().exists(sub)]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND EXISTS (SELECT * FROM "posts" WHERE 1=1 AND "posts".title = $1)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("hello".into())]);
    }

    #[test]
    fn filter_not_exists() {
        let sub = SqlSelect::new::<Posts>().filter([PostsCol::Title.eq("hello")]);

        let (sql, _) = build(SqlSelect::new::<Users>().filter([UExpr::new().not_exists(sub)]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND NOT EXISTS (SELECT * FROM "posts" WHERE 1=1 AND "posts".title = $1)"#,
        );
    }

    #[test]
    fn filter_any() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UsersCol::Name.any(SqlParam::String("alice".into()))]),
        );
        assert_eq!(sql, r#"SELECT * FROM "users" WHERE 1=1 AND "users".name = ANY($1)"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn select_with_sum() {
        let (sql, _) =
            build(SqlSelect::new::<Users>().from([UsersCol::Age.sum().alias("total_age")]));
        assert_eq!(sql, r#"SELECT SUM("users".age) AS total_age FROM "users""#);
    }

    #[test]
    fn select_with_avg_min_max() {
        let (sql, _) = build(SqlSelect::new::<Users>().from([
            UsersCol::Age.avg().alias("avg_age"),
            UsersCol::Age.min().alias("min_age"),
            UsersCol::Age.max().alias("max_age"),
        ]));
        assert_eq!(
            sql,
            r#"SELECT AVG("users".age) AS avg_age, MIN("users".age) AS min_age, MAX("users".age) AS max_age FROM "users""#,
        );
    }

    #[test]
    fn select_with_upper() {
        let (sql, _) = build(SqlSelect::new::<Users>().from([UsersCol::Name.upper().alias("u")]));
        assert_eq!(sql, r#"SELECT UPPER("users".name) AS u FROM "users""#);
    }

    #[test]
    fn filter_or() {
        let (sql, binds) = build(
            SqlSelect::new::<Users>()
                .filter([UsersCol::Name.eq("alice").or(UsersCol::Name.eq("bob"))]),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) OR "users".name = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::String("bob".into())],);
    }

    #[test]
    fn filter_or_with_other_filters() {
        let (sql, binds) = build(SqlSelect::new::<Users>().filter([
            UsersCol::Name.eq("alice").or(UsersCol::Name.eq("bob")),
            UsersCol::Age.gte(18i32),
        ]));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) OR "users".name = $2 AND "users".age >= $3"#,
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
    fn select_with_single_cte() {
        let (sql, binds) = build(
            crate::SqlQ::with([(
                "active_users",
                SqlSelect::new::<Users>().filter([UsersCol::Age.eq(18i32)]),
            )])
            .select::<Users>(),
        );
        assert_eq!(
            sql,
            r#"WITH active_users AS (SELECT * FROM "users" WHERE 1=1 AND "users".age = $1) SELECT * FROM "users""#,
        );
        assert_eq!(binds, vec![SqlParam::I32(18)]);
    }

    #[test]
    fn select_with_multiple_ctes() {
        let (sql, binds) = build(
            crate::SqlQ::with([
                ("young", SqlSelect::new::<Users>().filter([UsersCol::Age.eq(18i32)])),
                ("old", SqlSelect::new::<Users>().filter([UsersCol::Age.eq(65i32)])),
            ])
            .select::<Users>(),
        );
        assert_eq!(
            sql,
            r#"WITH young AS (SELECT * FROM "users" WHERE 1=1 AND "users".age = $1), old AS (SELECT * FROM "users" WHERE 1=1 AND "users".age = $2) SELECT * FROM "users""#,
        );
        assert_eq!(binds, vec![SqlParam::I32(18), SqlParam::I32(65)]);
    }

    #[test]
    fn select_with_cte_and_filters() {
        let (sql, binds) = build(
            crate::SqlQ::with([(
                "active",
                SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice")]),
            )])
            .select::<Users>()
            .filter([UsersCol::Age.eq(30i32)]),
        );
        assert_eq!(
            sql,
            r#"WITH active AS (SELECT * FROM "users" WHERE 1=1 AND "users".name = $1) SELECT * FROM "users" WHERE 1=1 AND "users".age = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }
}
