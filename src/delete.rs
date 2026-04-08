use std::marker::PhantomData;

use sqlx::QueryBuilder;

use crate::{
    SqlBase,
    shared::{
        Table, UnbindedQuery, error::SqlQueryError, expr::SqlExpr, push_conditions, value::SqlParam,
    },
};

pub struct SqlDelete<T: Table> {
    filters: Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>,
    returning: bool,
    _t: PhantomData<T>,
}

impl<T: Table> SqlDelete<T> {
    pub(super) fn new() -> Self {
        Self { filters: Vec::new(), returning: false, _t: PhantomData }
    }

    pub fn filter(mut self, filters: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        self.filters.extend(filters.into_iter().map(|x| x.eval()));
        self
    }

    pub fn returning(mut self) -> Self {
        self.returning = true;
        self
    }
}

impl<T: Table> SqlBase for SqlDelete<T> {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error> {
        let mut qb = QueryBuilder::new(format!("DELETE FROM \"{}\"", T::TABLE_NAME));
        let mut binds = vec![];

        push_conditions("WHERE", self.filters, &mut qb, &mut binds)?;

        if self.returning {
            qb.push(" RETURNING *");
        }

        Ok(UnbindedQuery { qb, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::expr::SqlOp;
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

    type UExpr = SqlExpr<Users>;

    fn build(delete: SqlDelete<Users>) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(delete).unwrap();
        let bq = uq.build();
        (bq.sql, bq.binds)
    }

    #[test]
    fn delete_all() {
        let (sql, binds) = build(SqlDelete::<Users>::new());
        assert_eq!(sql, r#"DELETE FROM "users""#);
        assert!(binds.is_empty());
    }

    #[test]
    fn delete_with_filter() {
        let (sql, binds) =
            build(SqlDelete::<Users>::new().filter([UExpr::eq(UsersCol::Name, "alice")]));
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }

    #[test]
    fn delete_with_multiple_filters() {
        let (sql, binds) = build(SqlDelete::<Users>::new().filter([
            UExpr::eq(UsersCol::Name, "alice"),
            UExpr::column(UsersCol::Age).op(SqlOp::Gt).val(SqlParam::I32(18)),
        ]));
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1 AND "users".age > $2"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(18)]);
    }

    #[test]
    fn delete_with_returning() {
        let (sql, _) = build(
            SqlDelete::<Users>::new().filter([UExpr::eq(UsersCol::Name, "alice")]).returning(),
        );
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name = $1 RETURNING *"#,);
    }

    #[test]
    fn delete_with_or_filter() {
        let (sql, binds) = build(
            SqlDelete::<Users>::new()
                .filter([UExpr::eq(UsersCol::Name, "alice").or(UExpr::eq(UsersCol::Name, "bob"))]),
        );
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND ("users".name = $1 OR "users".name = $2)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::String("bob".into())],);
    }

    #[test]
    fn delete_with_is_null() {
        let (sql, binds) =
            build(SqlDelete::<Users>::new().filter([UExpr::is_null(UsersCol::Name)]));
        assert_eq!(sql, r#"DELETE FROM "users" WHERE 1=1 AND "users".name IS NULL"#);
        assert!(binds.is_empty());
    }

    #[test]
    fn delete_with_subquery() {
        let sub = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::column(UsersCol::Id)])
            .filter([UExpr::eq(UsersCol::Name, "alice")]);

        let (sql, binds) = build(
            SqlDelete::<Users>::new()
                .filter([UExpr::column(UsersCol::Id).op(SqlOp::In).select(sub)]),
        );
        assert_eq!(
            sql,
            r#"DELETE FROM "users" WHERE 1=1 AND "users".id IN (SELECT "users".id FROM "users" WHERE 1=1 AND "users".name = $1)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into())]);
    }
}
