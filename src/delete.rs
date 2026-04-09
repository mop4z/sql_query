use std::marker::PhantomData;

use sqlx::QueryBuilder;

use crate::{
    SqlBase,
    shared::{
        Cte, Returning, Table, UnbindedQuery, error::SqlQueryError, expr::Expr, prepend_ctes,
        push_conditions, push_returning, value::SqlParam,
    },
};

/// Builder for SQL DELETE statements with filters and optional RETURNING clause.
pub struct SqlDelete<T: Table> {
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    returning: Returning,
    delete_all: bool,
    ctes: Vec<Cte>,
    _t: PhantomData<T>,
}

impl<T: Table> SqlDelete<T> {
    pub(super) fn new() -> Self {
        Self::new_with(vec![])
    }

    pub(super) fn new_with(ctes: Vec<Cte>) -> Self {
        Self {
            filters: Vec::new(),
            returning: Returning::None,
            delete_all: false,
            ctes,
            _t: PhantomData,
        }
    }

    /// Allows deleting all rows without requiring any filter conditions.
    pub fn delete_all(mut self) -> Self {
        self.delete_all = true;
        self
    }

    /// Adds WHERE conditions that are ANDed together.
    pub fn filter(mut self, filters: impl IntoIterator<Item = Expr<T>>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    /// Adds a RETURNING clause for the specified columns.
    pub fn returning(mut self, columns: impl IntoIterator<Item = Expr<T>>) -> Self {
        let cols: Vec<String> = columns.into_iter().map(|c| c.eval().unwrap().0).collect();
        self.returning = Returning::Columns(cols);
        self
    }

    /// Adds a RETURNING * clause to return all columns of deleted rows.
    pub fn returning_all(mut self) -> Self {
        self.returning = Returning::All;
        self
    }

    /// Explicitly opts out of a RETURNING clause (fire-and-forget delete).
    pub fn no_returning(mut self) -> Self {
        self.returning = Returning::None;
        self
    }
}

impl<T: Table> SqlBase for SqlDelete<T> {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error> {
        if self.filters.is_empty() && !self.delete_all {
            return Err(sqlx::Error::Protocol(
                SqlQueryError::DeleteRequiresFilterOrDeleteAll.to_string(),
            ));
        }

        let mut sql = format!("DELETE FROM \"{}\"", T::TABLE_NAME);
        let mut binds = vec![];
        prepend_ctes(self.ctes, &mut sql, &mut binds);

        let mut qb = QueryBuilder::new(sql);
        push_conditions("WHERE", self.filters, &mut qb, &mut binds)?;
        push_returning(self.returning, &mut qb);

        Ok(UnbindedQuery { qb, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    type UExpr = Expr<Users>;

    fn build(delete: SqlDelete<Users>) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(delete).unwrap();
        let bq = uq.bind();
        (bq.sql, bq.binds)
    }

    #[test]
    fn delete_all() {
        let (sql, binds) = build(SqlDelete::<Users>::new().delete_all());
        assert_eq!(sql, r#"DELETE FROM "users""#);
        assert!(binds.is_empty());
    }

    #[test]
    fn delete_without_filter_or_delete_all_fails() {
        let result = SqlBase::build(SqlDelete::<Users>::new());
        assert!(result.is_err());
    }

    #[test]
    fn delete_with_filter() {
        let (sql, binds) = build(SqlDelete::<Users>::new().filter([UsersCol::Name.eq("alice")]));
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn delete_with_multiple_filters() {
        let (sql, binds) = build(
            SqlDelete::<Users>::new().filter([UsersCol::Name.eq("alice"), UsersCol::Age.gt(18i32)]),
        );
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1 AND "users".age > $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(18)]);
    }

    #[test]
    fn delete_with_returning() {
        let (sql, _) =
            build(SqlDelete::<Users>::new().filter([UsersCol::Name.eq("alice")]).returning_all());
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1 RETURNING *"#,);
    }

    #[test]
    fn delete_with_or_filter() {
        let (sql, binds) = build(
            SqlDelete::<Users>::new().filter([UsersCol::Name
                .eq("alice")
                .or()
                .column(UsersCol::Name)
                .eq()
                .val("bob")]),
        );
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND ("users".name = $1) OR "users".name = $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::String("bob".into())],);
    }

    #[test]
    fn delete_with_is_null() {
        let (sql, binds) = build(SqlDelete::<Users>::new().filter([UsersCol::Name.is_null()]));
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name IS NULL"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn delete_with_subquery() {
        let sub = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Id).into()])
            .filter([UsersCol::Name.eq("alice")]);

        let (sql, binds) = build(
            SqlDelete::<Users>::new().filter([UExpr::new().column(UsersCol::Id).in_select(sub)]),
        );
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND "users".id IN (SELECT "users".id FROM "users" WHERE 1=1 AND "users".name = $1)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }
}
