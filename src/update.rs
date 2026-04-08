use sqlx::QueryBuilder;

use crate::{
    SqlBase,
    shared::{
        Cte, Returning, Table, UnbindedQuery, error::SqlQueryError, expr::SqlExpr, prepend_ctes,
        push_conditions, push_returning, value::SqlParam,
    },
};

/// Builder for SQL UPDATE statements with SET, FROM, filters, and optional RETURNING clause.
pub struct SqlUpdate {
    table: &'static str,
    set_clauses: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    from_tables: Vec<String>,
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    returning: Returning,
    ctes: Vec<Cte>,
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
            filters: Vec::new(),
            returning: Returning::None,
            ctes,
        }
    }

    /// Adds SET clauses for the columns to update.
    pub fn set<T: Table>(mut self, exprs: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        self.set_clauses.extend(exprs.into_iter().map(|x| x.eval()));
        self
    }

    /// Adds a FROM clause to reference another table in the update.
    pub fn from<T: Table>(mut self) -> Self {
        self.from_tables.push(format!("\"{}\"", T::TABLE_NAME));
        self
    }

    /// Adds WHERE conditions that are ANDed together.
    pub fn filter<T: Table>(mut self, filters: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    /// Adds a RETURNING clause for the specified columns.
    pub fn returning<T: Table>(mut self, columns: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        let cols: Vec<String> = columns.into_iter().map(|c| c.eval().unwrap().0).collect();
        self.returning = Returning::Columns(cols);
        self
    }

    /// Adds a RETURNING * clause to return all columns of updated rows.
    pub fn returning_all(mut self) -> Self {
        self.returning = Returning::All;
        self
    }
}

impl SqlBase for SqlUpdate {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error> {
        let mut sql = format!("UPDATE \"{}\" SET ", self.table);
        let mut binds = vec![];
        prepend_ctes(self.ctes, &mut sql, &mut binds);

        for (i, result) in self.set_clauses.into_iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let (clause, params) = result.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            sql.push_str(&clause);
            binds.extend(params);
        }

        if !self.from_tables.is_empty() {
            sql.push_str(" FROM ");
            sql.push_str(&self.from_tables.join(", "));
        }

        let mut qb = QueryBuilder::new(sql);
        push_conditions("WHERE", self.filters, &mut qb, &mut binds)?;
        push_returning(self.returning, &mut qb);

        Ok(UnbindedQuery { qb, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::expr::{SqlFn, SqlOp};
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

    fn build(update: SqlUpdate) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(update).unwrap();
        let bq = uq.build();
        (bq.sql, bq.binds)
    }

    #[test]
    fn update_single_set() {
        let (sql, binds) =
            build(SqlUpdate::new::<Users>().set([UExpr::eq(UsersCol::Name, "alice")]));
        assert_eq!(sql, r#"UPDATE "users" SET "users".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn update_multiple_sets() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET "users".name = $1, "users".age = $2"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn update_with_filter() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::eq(UsersCol::Name, "bob")])
                .filter([UExpr::eq(UsersCol::Id, 1i32)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET "users".name = $1 WHERE 1=1 AND "users".id = $2"#,);
        assert_eq!(binds, vec![SqlParam::String("bob".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_with_filter_and_returning() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::eq(UsersCol::Name, "bob")])
                .filter([UExpr::eq(UsersCol::Id, 1i32)])
                .returning_all(),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET "users".name = $1 WHERE 1=1 AND "users".id = $2 RETURNING *"#,
        );
        assert_eq!(binds, vec![SqlParam::String("bob".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_with_val_fn_now() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>().set([UExpr::column(UsersCol::Name)
                .op(SqlOp::Eq)
                .val(SqlParam::String("unused".into()))
                .val_fn(SqlFn::Now)]),
        );
        assert_eq!(sql, r#"UPDATE "users" SET "users".name = NOW()"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn update_bind_ordering_set_before_where() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::eq(UsersCol::Name, "new_name"), UExpr::eq(UsersCol::Age, 25i32)])
                .filter([
                    UExpr::eq(UsersCol::Name, "old_name"),
                    UExpr::column(UsersCol::Age).op(SqlOp::Gt).val(SqlParam::I32(18)),
                ]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET "users".name = $1, "users".age = $2 WHERE 1=1 AND "users".name = $3 AND "users".age > $4"#,
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
        let (sql, binds) =
            build(
                SqlUpdate::new::<Users>().set([UExpr::eq(UsersCol::Age, 0i32)]).filter([
                    UExpr::eq(UsersCol::Name, "alice").or(UExpr::eq(UsersCol::Name, "bob")),
                ]),
            );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET "users".age = $1 WHERE 1=1 AND ("users".name = $2 OR "users".name = $3)"#,
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
                .set([UExpr::eq(UsersCol::Name, "updated")])
                .from::<Posts>()
                .filter([UExpr::column(UsersCol::Id).op(SqlOp::Eq).val(SqlParam::I32(1))]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET "users".name = $1 FROM "posts" WHERE 1=1 AND "users".id = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("updated".into()), SqlParam::I32(1)]);
    }

    #[test]
    fn update_from_with_join_condition() {
        let (sql, binds) = build(
            SqlUpdate::new::<Users>()
                .set([UExpr::eq(UsersCol::Name, "updated")])
                .from::<Posts>()
                .filter([UExpr::column(UsersCol::Id).op(SqlOp::Eq).val(SqlParam::I32(1))])
                .filter([PExpr::eq(PostsCol::Title, "hello")]),
        );
        assert_eq!(
            sql,
            r#"UPDATE "users" SET "users".name = $1 FROM "posts" WHERE 1=1 AND "users".id = $2 AND "posts".title = $3"#,
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
}
