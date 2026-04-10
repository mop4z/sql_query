use crate::{
    SqlBase,
    select::SqlSelect,
    shared::{
        Cte, Returning, SqlConflict, Table, UnbindedQuery,
        error::SqlQueryError,
        expr::{EvalExpr, Expr},
        prepend_ctes, push_returning,
        value::SqlParam,
    },
};

/// Builder for SQL INSERT statements with conflict handling and optional RETURNING clause.
pub struct SqlInsert<T: Table> {
    columns: Vec<String>,
    rows: Vec<Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>>,
    select_source: Option<(String, Vec<SqlParam>)>,
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
            select_source: None,
            on_conflict: None,
            returning: Returning::None,
            ctes,
            _t: std::marker::PhantomData,
        }
    }

    fn has_source(&self) -> bool {
        !self.columns.is_empty() || !self.rows.is_empty() || self.select_source.is_some()
    }

    /// Sets column-value pairs for a single-row insert.
    pub fn values(
        mut self,
        exprs: impl IntoIterator<Item = Expr<T>>,
    ) -> Result<Self, SqlQueryError> {
        if self.has_source() {
            return Err(SqlQueryError::InsertValuesAlreadySet);
        }
        let (cols, row) = Self::extract_row(exprs);
        self.columns = cols;
        self.rows.push(row);
        Ok(self)
    }

    /// Sets column-value pairs for a multi-row insert.
    pub fn values_nested(
        mut self,
        rows: impl IntoIterator<Item = impl IntoIterator<Item = Expr<T>>>,
    ) -> Result<Self, SqlQueryError> {
        if self.has_source() {
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

    /// Sets the data source to a SELECT query instead of literal VALUES.
    pub fn from_select(
        mut self,
        columns: impl IntoIterator<Item = T::Col>,
        select: SqlSelect,
    ) -> Result<Self, SqlQueryError> {
        if self.has_source() {
            return Err(SqlQueryError::InsertSourceAlreadySet);
        }
        self.columns = columns.into_iter().map(|c| c.as_ref().to_string()).collect();
        let uq = SqlBase::build(select).expect("select build failed");
        let (sql, binds) = uq.into_raw();
        self.select_source = Some((sql, binds));
        Ok(self)
    }

    fn extract_row(
        exprs: impl IntoIterator<Item = Expr<T>>,
    ) -> (Vec<String>, Vec<Result<(String, Vec<SqlParam>), SqlQueryError>>) {
        let mut cols = vec![];
        let mut row = vec![];
        for expr in exprs {
            let (col, val_sql, binds) = expr.into_col_and_val();
            if let Some(col) = col {
                cols.push(col);
            }
            row.push(Ok((val_sql, binds)));
        }
        (cols, row)
    }

    /// Sets the ON CONFLICT resolution strategy for the insert.
    pub fn on_conflict(mut self, conflict: SqlConflict<T::Col>) -> Self {
        self.on_conflict = Some(conflict);
        self
    }

    /// Adds a RETURNING clause for the specified columns.
    pub fn returning(mut self, columns: impl IntoIterator<Item = impl EvalExpr>) -> Self {
        let cols: Vec<String> = columns.into_iter().map(|c| c.eval().unwrap().0).collect();
        self.returning = Returning::Columns(cols);
        self
    }

    /// Adds a RETURNING * clause to return all columns of inserted rows.
    pub fn returning_all(mut self) -> Self {
        self.returning = Returning::All;
        self
    }

    /// Explicitly opts out of a RETURNING clause (fire-and-forget insert).
    pub fn no_returning(mut self) -> Self {
        self.returning = Returning::None;
        self
    }
}

impl<T: Table> SqlBase for SqlInsert<T> {
    fn build(self) -> Result<UnbindedQuery, sqlx::Error> {
        let mut sql = String::with_capacity(128);
        sql.push_str("INSERT INTO \"");
        sql.push_str(T::TABLE_NAME);
        sql.push_str("\" (");
        sql.push_str(&self.columns.join(", "));
        sql.push(')');
        let mut binds = vec![];
        prepend_ctes(self.ctes, &mut sql, &mut binds);

        if let Some((select_sql, select_binds)) = self.select_source {
            sql.push(' ');
            sql.push_str(&select_sql);
            binds.extend(select_binds);
        } else {
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
        }

        if let Some(conflict) = self.on_conflict {
            match conflict {
                SqlConflict::DoNothing => {
                    sql.push_str(" ON CONFLICT DO NOTHING");
                }
                SqlConflict::DoUpdate { conflict_cols, update_cols } => {
                    sql.push_str(" ON CONFLICT (");
                    for (i, c) in conflict_cols.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(c.as_ref());
                    }
                    sql.push_str(") DO UPDATE SET ");
                    push_excluded_sets(&mut sql, &update_cols);
                }
                SqlConflict::OnConstraint { name, update_cols } => {
                    sql.push_str(" ON CONFLICT ON CONSTRAINT ");
                    sql.push_str(name);
                    sql.push_str(" DO UPDATE SET ");
                    push_excluded_sets(&mut sql, &update_cols);
                }
            }
        }

        push_returning(self.returning, &mut sql);
        Ok(UnbindedQuery { sql, binds })
    }
}

fn push_excluded_sets<C: AsRef<str>>(sql: &mut String, cols: &[C]) {
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let c = c.as_ref();
        sql.push_str(c);
        sql.push_str(" = EXCLUDED.");
        sql.push_str(c);
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

    fn build(insert: SqlInsert<Users>) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(insert).unwrap();
        let bq = uq.bind();
        (bq.sql, bq.binds)
    }

    #[test]
    fn insert_single_row() {
        let (sql, binds) = build(
            SqlInsert::<Users>::new()
                .values([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)])
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
                    vec![UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)],
                    vec![UsersCol::Name.eq("bob"), UsersCol::Age.eq(25i32)],
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
            SqlInsert::<Users>::new().values([UsersCol::Name.eq("alice")]).unwrap().returning_all(),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name) VALUES ($1) RETURNING *"#);
    }

    #[test]
    fn insert_on_conflict_do_nothing() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UsersCol::Name.eq("alice")])
                .unwrap()
                .on_conflict(SqlConflict::DoNothing),
        );
        assert_eq!(sql, r#"INSERT INTO "users" (name) VALUES ($1) ON CONFLICT DO NOTHING"#);
    }

    #[test]
    fn insert_on_conflict_do_update() {
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .values([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)])
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
                .values([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)])
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
                .values([UsersCol::Name.eq("alice"), UsersCol::Age.eq(30i32)])
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
            .values([UsersCol::Name.eq("alice")])
            .unwrap()
            .values([UsersCol::Name.eq("bob")]);
        assert!(matches!(result, Err(SqlQueryError::InsertValuesAlreadySet)));
    }

    #[test]
    fn err_values_nested_after_values() {
        let result = SqlInsert::<Users>::new()
            .values([UsersCol::Name.eq("alice")])
            .unwrap()
            .values_nested([vec![UsersCol::Name.eq("bob")]]);
        assert!(matches!(result, Err(SqlQueryError::InsertValuesAlreadySet)));
    }

    type UExpr = Expr<Users>;

    #[test]
    fn insert_from_select() {
        let select = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Name), UExpr::new().column(UsersCol::Age)])
            .filter([UsersCol::Age.gt(18i32)]);
        let (sql, binds) = build(
            SqlInsert::<Users>::new()
                .from_select([UsersCol::Name, UsersCol::Age], select)
                .unwrap(),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) SELECT "users".name, "users".age FROM "users" WHERE 1=1 AND "users".age > $1"#,
        );
        assert_eq!(binds, vec![SqlParam::I32(18)]);
    }

    #[test]
    fn insert_from_select_on_conflict() {
        let select = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Name), UExpr::new().column(UsersCol::Age)])
            .filter([UsersCol::Age.gt(18i32)]);
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .from_select([UsersCol::Name, UsersCol::Age], select)
                .unwrap()
                .on_conflict(SqlConflict::DoNothing),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) SELECT "users".name, "users".age FROM "users" WHERE 1=1 AND "users".age > $1 ON CONFLICT DO NOTHING"#,
        );
    }

    #[test]
    fn insert_from_select_returning() {
        let select = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Name), UExpr::new().column(UsersCol::Age)]);
        let (sql, _) = build(
            SqlInsert::<Users>::new()
                .from_select([UsersCol::Name, UsersCol::Age], select)
                .unwrap()
                .returning_all(),
        );
        assert_eq!(
            sql,
            r#"INSERT INTO "users" (name, age) SELECT "users".name, "users".age FROM "users" RETURNING *"#,
        );
    }

    #[test]
    fn err_from_select_after_values() {
        let select = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Name)]);
        let result = SqlInsert::<Users>::new()
            .values([UsersCol::Name.eq("alice")])
            .unwrap()
            .from_select([UsersCol::Name], select);
        assert!(matches!(result, Err(SqlQueryError::InsertSourceAlreadySet)));
    }

    #[test]
    fn err_values_after_from_select() {
        let select = crate::select::SqlSelect::new::<Users>()
            .from([UExpr::new().column(UsersCol::Name)]);
        let result = SqlInsert::<Users>::new()
            .from_select([UsersCol::Name], select)
            .unwrap()
            .values([UsersCol::Name.eq("bob")]);
        assert!(matches!(result, Err(SqlQueryError::InsertValuesAlreadySet)));
    }
}
