use crate::{
    SqlBase,
    shared::{
        Cte, Returning, Table, UnbindedQuery,
        error::SqlQueryError,
        expr::EvalExpr,
        prepend_ctes, push_conditions, push_returning,
        value::SqlParam,
    },
};
#[cfg(test)]
use crate::shared::expr::Expr;

/// Builder for SQL UPDATE statements with SET, FROM, filters, and optional RETURNING clause.
pub struct SqlUpdate {
    table: &'static str,
    set_clauses: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    from_tables: Vec<String>,
    from_binds: Vec<SqlParam>,
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    returning: Returning,
    ctes: Vec<Cte>,
    include_nulls: bool,
}

impl SqlUpdate {
    pub(super) fn new<T: Table>() -> Self {
        Self::new_with::<T>(vec![])
    }

    pub(super) fn new_with<T: Table>(ctes: Vec<Cte>) -> Self {
        Self {
            table: T::TABLE_NAME,
            set_clauses: Vec::new(),
            from_tables: Vec::new(),
            from_binds: Vec::new(),
            filters: Vec::new(),
            returning: Returning::None,
            ctes,
            include_nulls: false,
        }
    }

    /// Add `SET col = val` clauses. Pass `Col::Name.eq(val)` expressions,
    /// or use `Expr::new().column(col).eq().now()` for computed values.
    pub fn set<E: EvalExpr>(mut self, exprs: impl IntoIterator<Item = E>) -> Self {
        self.set_clauses.extend(exprs.into_iter().map(|x| x.eval()));
        self
    }

    /// Add a `FROM "table"` clause for multi-table updates (Postgres-specific).
    /// Allows referencing columns from another table in SET and WHERE clauses.
    pub fn from<T: Table>(mut self) -> Self {
        let mut s = String::with_capacity(T::TABLE_NAME.len() + 2);
        s.push('"');
        s.push_str(T::TABLE_NAME);
        s.push('"');
        self.from_tables.push(s);
        self
    }

    /// Add a `FROM (subquery) alias` clause for referencing a subquery in SET/WHERE.
    pub fn from_subquery(mut self, alias: &str, query: impl SqlBase) -> Self {
        let uq = query.build().expect("from_subquery build failed");
        let (sub_sql, sub_binds) = uq.into_raw();
        let mut s = String::with_capacity(sub_sql.len() + alias.len() + 4);
        s.push('(');
        s.push_str(&sub_sql);
        s.push_str(") ");
        s.push_str(alias);
        self.from_tables.push(s);
        self.from_binds.extend(sub_binds);
        self
    }

    /// Adds WHERE conditions that are ANDed together.
    pub fn filter<E: EvalExpr>(mut self, filters: impl IntoIterator<Item = E>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    /// Adds a RETURNING clause for the specified columns.
    pub fn returning(mut self, columns: impl IntoIterator<Item = impl EvalExpr>) -> Self {
        let cols: Vec<String> = columns.into_iter().map(|c| c.eval().unwrap().0).collect();
        self.returning = Returning::Columns(cols);
        self
    }

    /// Adds a RETURNING * clause to return all columns of updated rows.
    pub fn returning_all(mut self) -> Self {
        self.returning = Returning::All;
        self
    }

    /// Explicitly opts out of a RETURNING clause (fire-and-forget update).
    pub fn no_returning(mut self) -> Self {
        self.returning = Returning::None;
        self
    }

    /// Forces SET clauses with NULL values to be included (normally skipped).
    pub fn include_nulls(mut self) -> Self {
        self.include_nulls = true;
        self
    }

    /// Returns true if at least one SET clause with a non-null value has been added.
    pub fn has_non_null_sets(&self) -> bool {
        self.set_clauses.iter().any(|r| {
            r.as_ref()
                .map(|(_, binds)| binds.iter().any(|b| !matches!(b, SqlParam::Null)))
                .unwrap_or(false)
        })
    }
}

impl SqlBase for SqlUpdate {
    fn build(self) -> Result<UnbindedQuery, sqlx::Error> {
        let mut sql = String::with_capacity(128);
        sql.push_str("UPDATE \"");
        sql.push_str(self.table);
        sql.push_str("\" SET ");
        let mut binds = vec![];
        prepend_ctes(self.ctes, &mut sql, &mut binds);

        let include_nulls = self.include_nulls;
        let target_prefix = format!("\"{}\".", self.table);
        let mut set_count = 0;
        for result in self.set_clauses {
            let (clause, params) = result.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            if !include_nulls
                && params.iter().all(|b| matches!(b, SqlParam::Null))
                && !params.is_empty()
            {
                continue;
            }
            if set_count > 0 {
                sql.push_str(", ");
            }
            // Postgres rejects qualified SET targets ("table".col = ...) —
            // strip the target-table prefix from the LHS if present.
            let clause = clause.strip_prefix(&target_prefix).unwrap_or(&clause);
            sql.push_str(clause);
            binds.extend(params);
            set_count += 1;
        }

        if !self.from_tables.is_empty() {
            sql.push_str(" FROM ");
            sql.push_str(&self.from_tables.join(", "));
            binds.extend(self.from_binds);
        }

        push_conditions("WHERE", self.filters, &mut sql, &mut binds)?;
        push_returning(self.returning, &mut sql);

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
    #[allow(dead_code)]
    type PExpr = Expr<Posts>;

    fn build(update: SqlUpdate) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(update).unwrap();
        let bq = uq.bind();
        (bq.sql, bq.binds)
    }

    #[test]
    fn update_single_set() {
        let (sql, binds) = build(SqlUpdate::new::<Users>().set([UsersCol::Name.eq("alice")]));
        assert_eq!(sql, r#"UPDATE "users" SET name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn update_multiple_sets() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>().set([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET name = $1, age = $2"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn update_with_filter() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("bob")])
                .filter([UsersCol::Id.eq(1i32)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET name = $1 WHERE 1=1 AND "users".id = $2"#,);
        assert_eq!(binds, vec![SqlParam::String("bob".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_with_filter_and_returning() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("bob")])
                .filter([UsersCol::Id.eq(1i32)])
                .returning_all(),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET name = $1 WHERE 1=1 AND "users".id = $2 RETURNING *"#,
        );
        assert_eq!(binds, vec![SqlParam::String("bob".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_with_val_fn_now() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::new().column(UsersCol::Name).eq(UExpr::new().now())]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET name = NOW()"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn update_bind_ordering_set_before_where() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("new_name"), UsersCol::Age.eq(25i32)])
                .filter([UsersCol::Name.eq("old_name"), UsersCol::Age.gt(18i32)]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET name = $1, age = $2 WHERE 1=1 AND "users".name = $3 AND "users".age > $4"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("new_name".into()),
                SqlParam::I32(25),
                SqlParam::String("old_name".into()),
                SqlParam::I32(18),
            ],
        );
    }

    #[test]
    fn update_with_or_filter() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>().set([UsersCol::Age.eq(0i32)]).filter([UsersCol::Name
                .eq("alice")
                .or(UsersCol::Name.eq("bob"))]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET age = $1 WHERE 1=1 AND ("users".name = $2) OR "users".name = $3"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::I32(0),
                SqlParam::String("alice".into()),
                SqlParam::String("bob".into())
            ],
        );
    }

    #[test]
    fn update_from_single_table() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("updated")])
                .from::<Posts>()
                .filter([UsersCol::Id.eq(1i32)]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET name = $1 FROM "posts" WHERE 1=1 AND "users".id = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("updated".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_from_with_join_condition() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("updated")])
                .from::<Posts>()
                .filter([UsersCol::Id.eq(1i32)])
                .filter([PostsCol::Title.eq("hello")]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET name = $1 FROM "posts" WHERE 1=1 AND "users".id = $2 AND "posts".title = $3"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("updated".into()),
                SqlParam::I32(1),
                SqlParam::String("hello".into())
            ],
        );
    }

    #[test]
    fn update_skips_null_sets_by_default() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("alice"), UsersCol::Age.eq(SqlParam::Null)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn update_include_nulls() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("alice"), UsersCol::Age.eq(SqlParam::Null)])
                .include_nulls(),
        );
        assert_eq!(sql, r#"UPDATE "users" SET name = $1, age = $2"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::Null]);
    }

    #[test]
    fn update_from_subquery() {
        let sub = crate::select::SqlSelect::new::<Posts>()
            .from([PExpr::new().column(PostsCol::UserId)])
            .filter([PostsCol::Title.eq("hello")]);
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UsersCol::Name.eq("updated")])
                .from_subquery("sub", sub)
                .filter([UExpr::new().raw("\"users\".id = sub.user_id")]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET name = $1 FROM (SELECT "posts".user_id FROM "posts" WHERE 1=1 AND "posts".title = $2) sub WHERE 1=1 AND "users".id = sub.user_id"#,
        );
        assert_eq!(
            binds,
            vec![SqlParam::String("updated".into()), SqlParam::String("hello".into())],
        );
    }
}
