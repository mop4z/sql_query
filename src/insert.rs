use std::fmt::Write;

use sqlx::QueryBuilder;

use crate::{
    SqlBase,
    shared::{
        Cte, Returning, SqlConflict, Table, UnbindedQuery, error::SqlQueryError, expr::SqlExpr,
        prepend_ctes, push_returning, value::SqlParam,
    },
};

pub struct SqlInsert<T: Table> {
    columns: Vec<String>,
    rows: Vec<Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>>,
    on_conflict: Option<SqlConflict<T::Col>>,
    returning: Returning,
    ctes: Vec<Cte>,
    _t: std::marker::PhantomData<T>,
}

impl<T: Table> SqlInsert<T> {
    pub(super) fn new() -> Self {
        Self::new_with(vec![])
    }

    pub(super) fn new_with(ctes: Vec<Cte>) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            on_conflict: None,
            returning: Returning::None,
            ctes,
            _t: std::marker::PhantomData,
        }
    }

    pub fn values(
        mut self,
        exprs: impl IntoIterator<Item = SqlExpr<T>>,
    ) -> Result<Self, SqlQueryError> {
        if !self.columns.is_empty() || !self.rows.is_empty() {
            return Err(SqlQueryError::InsertValuesAlreadySet);
        }
        let (cols, row) = Self::extract_row(exprs);
        self.columns = cols;
        self.rows.push(row);
        Ok(self)
    }

    pub fn values_nested(
        mut self,
        rows: impl IntoIterator<Item = impl IntoIterator<Item = SqlExpr<T>>>,
    ) -> Result<Self, SqlQueryError> {
        if !self.columns.is_empty() || !self.rows.is_empty() {
            return Err(SqlQueryError::InsertValuesAlreadySet);
        }
        let mut first = true;
        for exprs in rows {
            let (cols, row) = Self::extract_row(exprs);
            if first {
                self.columns = cols;
                first = false;
            }
            self.rows.push(row);
        }
        Ok(self)
    }

    fn extract_row(
        exprs: impl IntoIterator<Item = SqlExpr<T>>,
    ) -> (Vec<String>, Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>) {
        let mut cols = vec![];
        let mut row = vec![];
        for expr in exprs {
            let (col, val_expr) = expr.into_col_and_val();
            if let Some(col) = col {
                cols.push(col);
            }
            row.push(val_expr.eval());
        }
        (cols, row)
    }

    pub fn on_conflict(mut self, conflict: SqlConflict<T::Col>) -> Self {
        self.on_conflict = Some(conflict);
        self
    }

    pub fn returning(mut self, columns: impl IntoIterator<Item = SqlExpr<T>>) -> Self {
        let cols: Vec<String> = columns.into_iter().map(|c| c.eval().unwrap().0).collect();
        self.returning = Returning::Columns(cols);
        self
    }

    pub fn returning_all(mut self) -> Self {
        self.returning = Returning::All;
        self
    }
}

impl<T: Table> SqlBase for SqlInsert<T> {
    fn build<'a>(self) -> Result<UnbindedQuery<'a>, sqlx::Error> {
        let col_list = self.columns.join(", ");
        let mut sql = format!("INSERT INTO \"{}\" ({col_list})", T::TABLE_NAME);
        let mut binds = vec![];
        prepend_ctes(self.ctes, &mut sql, &mut binds);

        for (i, row) in self.rows.into_iter().enumerate() {
            if i == 0 {
                sql.push_str(" VALUES ");
            } else {
                sql.push_str(", ");
            }
            sql.push('(');
            for (j, result) in row.into_iter().enumerate() {
                if j > 0 {
                    sql.push_str(", ");
                }
                let (val_sql, val_binds) =
                    result.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
                sql.push_str(&val_sql);
                binds.extend(val_binds);
            }
            sql.push(')');
        }

        if let Some(conflict) = self.on_conflict {
            match conflict {
                SqlConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                SqlConflict::DoUpdate { conflict_cols, update_cols } => {
                    let cols: Vec<&str> = conflict_cols.iter().map(|c| c.as_ref()).collect();
                    write!(sql, " ON CONFLICT ({}) DO UPDATE SET ", cols.join(", ")).unwrap();
                    push_excluded_sets(&mut sql, &update_cols);
                }
                SqlConflict::OnConstraint { name, update_cols } => {
                    write!(sql, " ON CONFLICT ON CONSTRAINT {name} DO UPDATE SET ").unwrap();
                    push_excluded_sets(&mut sql, &update_cols);
                }
            }
        }

        let mut qb = QueryBuilder::new(sql);
        push_returning(self.returning, &mut qb);
        Ok(UnbindedQuery { qb, binds })
    }
}

fn push_excluded_sets<C: AsRef<str>>(sql: &mut String, cols: &[C]) {
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let c = c.as_ref();
        write!(sql, "{c} = EXCLUDED.{c}").unwrap();
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

    type UExpr = SqlExpr<Users>;

    fn build(insert: SqlInsert<Users>) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(insert).unwrap();
        let bq = uq.build();
        (bq.sql, bq.binds)
    }

    #[test]
    fn insert_single_row() {
        let (sql, binds) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)])
                .unwrap(),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name, age) VALUES ($1, $2)"#);
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn insert_multiple_rows() {
        let (sql, binds) = build(
            SqlInsert::<Users>::new()
                .values_nested([
                    vec![UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)],
                    vec![UExpr::eq(UsersCol::Name, "bob"), UExpr::eq(UsersCol::Age, 25i32)],
                ])
                .unwrap(),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name, age) VALUES ($1, $2), ($3, $4)"#);
        assert_eq!(
            binds,
            vec![
                SqlParam::String("alice".into()),
                SqlParam::I32(30),
                SqlParam::String("bob".into()),
                SqlParam::I32(25),
            ],
        );
    }

    #[test]
    fn insert_with_returning() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice")])
                .unwrap()
                .returning_all(),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name) VALUES ($1) RETURNING *"#);
    }

    #[test]
    fn insert_on_conflict_do_nothing() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice")])
                .unwrap()
                .on_conflict(SqlConflict::DoNothing),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name) VALUES ($1) ON CONFLICT DO NOTHING"#);
    }

    #[test]
    fn insert_on_conflict_do_update() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)])
                .unwrap()
                .on_conflict(SqlConflict::DoUpdate {
                    conflict_cols: vec![UsersCol::Name],
                    update_cols: vec![UsersCol::Age],
                }),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET age = EXCLUDED.age"#,
        );
    }

    #[test]
    fn insert_on_conflict_on_constraint() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)])
                .unwrap()
                .on_conflict(SqlConflict::OnConstraint {
                    name: "users_name_key",
                    update_cols: vec![UsersCol::Name, UsersCol::Age],
                }),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT users_name_key DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age"#,
        );
    }

    #[test]
    fn insert_on_conflict_with_returning() {
        let (sql, binds) = build(
            SqlInsert::<Users>::new()
                .values([UExpr::eq(UsersCol::Name, "alice"), UExpr::eq(UsersCol::Age, 30i32)])
                .unwrap()
                .on_conflict(SqlConflict::DoUpdate {
                    conflict_cols: vec![UsersCol::Name],
                    update_cols: vec![UsersCol::Age],
                })
                .returning_all(),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET age = EXCLUDED.age RETURNING *"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::I32(30)]);
    }

    #[test]
    fn err_values_called_twice() {
        let result = SqlInsert::<Users>::new()
            .values([UExpr::eq(UsersCol::Name, "alice")])
            .unwrap()
            .values([UExpr::eq(UsersCol::Name, "bob")]);
        assert!(matches!(result, Err(SqlQueryError::InsertValuesAlreadySet)));
    }

    #[test]
    fn err_values_nested_after_values() {
        let result = SqlInsert::<Users>::new()
            .values([UExpr::eq(UsersCol::Name, "alice")])
            .unwrap()
            .values_nested([vec![UExpr::eq(UsersCol::Name, "bob")]]);
        assert!(matches!(result, Err(SqlQueryError::InsertValuesAlreadySet)));
    }
}
