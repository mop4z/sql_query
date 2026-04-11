use crate::{
    SqlBase,
    select::SqlSelect,
    shared::{
        UnbindedQuery,
        expr::{EvalExpr, SqlOrder},
        value::SqlParam,
    },
};

#[derive(strum::AsRefStr)]
enum SetOpKind {
    #[strum(serialize = "UNION")]
    Union,
    #[strum(serialize = "UNION ALL")]
    UnionAll,
    #[strum(serialize = "INTERSECT")]
    Intersect,
    #[strum(serialize = "INTERSECT ALL")]
    IntersectAll,
    #[strum(serialize = "EXCEPT")]
    Except,
    #[strum(serialize = "EXCEPT ALL")]
    ExceptAll,
}

/// Builder for compound SELECT statements joined by UNION / INTERSECT / EXCEPT.
pub struct SqlSetOp {
    first_sql: String,
    first_binds: Vec<SqlParam>,
    rest: Vec<(SetOpKind, String, Vec<SqlParam>)>,
    order_by: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
}

impl SqlSetOp {
    pub(crate) fn new(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::Union, second)
    }

    pub(crate) fn new_all(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::UnionAll, second)
    }

    pub(crate) fn new_intersect(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::Intersect, second)
    }

    pub(crate) fn new_intersect_all(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::IntersectAll, second)
    }

    pub(crate) fn new_except(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::Except, second)
    }

    pub(crate) fn new_except_all(first: SqlSelect, second: SqlSelect) -> Self {
        Self::with_kind(first, SetOpKind::ExceptAll, second)
    }

    fn with_kind(first: SqlSelect, kind: SetOpKind, second: SqlSelect) -> Self {
        let (first_sql, first_binds) =
            SqlBase::build(first).expect("set op: first query build failed").into_raw();
        let (second_sql, second_binds) =
            SqlBase::build(second).expect("set op: second query build failed").into_raw();
        Self {
            first_sql,
            first_binds,
            rest: vec![(kind, second_sql, second_binds)],
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    fn push(mut self, kind: SetOpKind, other: SqlSelect) -> Self {
        let (sql, binds) = SqlBase::build(other).expect("set op: query build failed").into_raw();
        self.rest.push((kind, sql, binds));
        self
    }

    pub fn union(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::Union, other)
    }

    pub fn union_all(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::UnionAll, other)
    }

    pub fn intersect(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::Intersect, other)
    }

    pub fn intersect_all(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::IntersectAll, other)
    }

    pub fn except(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::Except, other)
    }

    pub fn except_all(self, other: SqlSelect) -> Self {
        self.push(SetOpKind::ExceptAll, other)
    }

    /// Appends an ORDER BY clause on the combined result.
    pub fn order_by(mut self, column: impl EvalExpr, order: SqlOrder) -> Self {
        let mut s = column.eval().unwrap().0;
        s.push(' ');
        s.push_str(order.as_ref());
        self.order_by.push(s);
        self
    }

    /// Sets the maximum number of rows to return from the combined result.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the number of rows to skip in the combined result.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }
}

impl SqlBase for SqlSetOp {
    fn build(self) -> Result<UnbindedQuery, sqlx::Error> {
        let mut sql = self.first_sql;
        let mut binds = self.first_binds;

        for (kind, part_sql, part_binds) in self.rest {
            sql.push(' ');
            sql.push_str(kind.as_ref());
            sql.push(' ');
            sql.push_str(&part_sql);
            binds.extend(part_binds);
        }

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

        Ok(UnbindedQuery { sql, binds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::Table;
    use crate::shared::expr::Expr;
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

    fn build(set_op: SqlSetOp) -> (String, Vec<SqlParam>) {
        let uq = SqlBase::build(set_op).unwrap();
        let bq = uq.bind();
        (bq.sql, bq.binds)
    }

    #[test]
    fn union_two_selects() {
        let q1 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice")]);
        let q2 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("bob")]);
        let (sql, binds) = build(q1.union(q2));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) UNION SELECT * FROM "users" WHERE 1=1 AND ("users".name = $2)"#,
        );
        assert_eq!(binds, vec![SqlParam::String("alice".into()), SqlParam::String("bob".into())]);
    }

    #[test]
    fn union_all() {
        let q1 = SqlSelect::new::<Users>();
        let q2 = SqlSelect::new::<Users>();
        let (sql, _) = build(q1.union_all(q2));
        assert_eq!(sql, r#"SELECT * FROM "users" UNION ALL SELECT * FROM "users""#,);
    }

    #[test]
    fn intersect() {
        let q1 = SqlSelect::new::<Users>();
        let q2 = SqlSelect::new::<Users>();
        let (sql, _) = build(q1.intersect(q2));
        assert_eq!(sql, r#"SELECT * FROM "users" INTERSECT SELECT * FROM "users""#,);
    }

    #[test]
    fn except() {
        let q1 = SqlSelect::new::<Users>();
        let q2 = SqlSelect::new::<Users>();
        let (sql, _) = build(q1.except(q2));
        assert_eq!(sql, r#"SELECT * FROM "users" EXCEPT SELECT * FROM "users""#,);
    }

    #[test]
    fn union_different_bind_counts() {
        let q1 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice")]);
        let q2 =
            SqlSelect::new::<Users>().filter([UsersCol::Name.eq("bob"), UsersCol::Age.gt(18i32)]);
        let (sql, binds) = build(q1.union(q2));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) UNION SELECT * FROM "users" WHERE 1=1 AND ("users".name = $2) AND ("users".age > $3)"#,
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
    fn three_way_union() {
        let q1 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("a")]);
        let q2 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("b")]);
        let q3 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("c")]);
        let (sql, binds) = build(q1.union(q2).union(q3));
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) UNION SELECT * FROM "users" WHERE 1=1 AND ("users".name = $2) UNION SELECT * FROM "users" WHERE 1=1 AND ("users".name = $3)"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("a".into()),
                SqlParam::String("b".into()),
                SqlParam::String("c".into()),
            ],
        );
    }

    #[test]
    fn union_with_order_limit_offset() {
        let q1 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("alice")]);
        let q2 = SqlSelect::new::<Users>().filter([UsersCol::Name.eq("bob")]);
        let (sql, binds) = build(
            q1.union(q2)
                .order_by(Expr::<Users>::new().raw("name"), SqlOrder::Asc)
                .limit(10)
                .offset(5),
        );
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE 1=1 AND ("users".name = $1) UNION SELECT * FROM "users" WHERE 1=1 AND ("users".name = $2) ORDER BY name ASC LIMIT $3 OFFSET $4"#,
        );
        assert_eq!(
            binds,
            vec![
                SqlParam::String("alice".into()),
                SqlParam::String("bob".into()),
                SqlParam::I64(10),
                SqlParam::I64(5),
            ],
        );
    }

    #[test]
    fn union_no_binds() {
        let q1 = SqlSelect::new::<Users>();
        let q2 = SqlSelect::new::<Users>();
        let (sql, binds) = build(q1.union(q2));
        assert_eq!(sql, r#"SELECT * FROM "users" UNION SELECT * FROM "users""#,);
        assert!(binds.is_empty());
    }
}
