//! Rule-based rewrites that are sound before cost-based planning.

use crabka_pgcatalog::Table;
use crabka_pgparser::ast::{BinaryOp, Expr, ValuesStmt};

use crate::scope::Scope;

/// Whether a WHERE clause is the literal boolean `false`.
///
/// This is the first Phase-1 constant-qual rewrite. It does not evaluate an
/// expression, so volatile calls and errors remain on their ordinary paths.
pub(crate) fn is_literal_false(filter: Option<&Expr>) -> bool {
    matches!(filter, Some(Expr::BoolLiteral(false)))
}

/// A one-row `VALUES` relation needs no scan node.
pub(crate) fn is_single_row_values(values: &ValuesStmt) -> bool {
    values.rows.len() == 1
}

/// Reduce a root null test on a stored `NOT NULL` column to its known truth.
///
/// The caller supplies the only legal qualifier for this one-relation scope,
/// preventing a malformed reference from being hidden by the reduction.
pub(crate) fn reduce_not_null_test(
    filter: Option<&Expr>,
    table: &Table,
    qualifier: &str,
) -> Option<Expr> {
    filter.map(|filter| match filter {
        Expr::IsNull { expr, negated }
            if matches!(expr.as_ref(), Expr::Column { table: column_table, name }
                if column_table.as_deref().is_none_or(|written| written == qualifier)
                    && table.column_index(name).is_some_and(|index| table.columns[index].not_null)) =>
        {
            Expr::BoolLiteral(*negated)
        }
        _ => filter.clone(),
    })
}

/// Rewrite a typed `column = column` qual to `column IS NOT NULL`.
///
/// The equality-operator check is essential: `IS NOT NULL` is available for
/// every type, while `=` is not. Keeping the original expression in that case
/// preserves the analysis error the query owes.
pub(crate) fn rewrite_self_equality(filter: Option<&Expr>, scope: &Scope) -> Option<Expr> {
    filter.map(|filter| rewrite_self_equality_expr(filter, scope))
}

fn rewrite_self_equality_expr(filter: &Expr, scope: &Scope) -> Expr {
    match filter {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } if left == right => {
            let Expr::Column { table, name } = left.as_ref() else {
                return filter.clone();
            };
            let Ok(index) = scope.resolve(table.as_deref(), name) else {
                return filter.clone();
            };
            if crate::eval::require_equality_operator(scope.ty_at(index)).is_err() {
                return filter.clone();
            }
            Expr::IsNull {
                expr: left.clone(),
                negated: true,
            }
        }
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite_self_equality_expr(left, scope)),
            right: Box::new(rewrite_self_equality_expr(right, scope)),
        },
        _ => filter.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_pgparser::ast::BinaryOp;
    use crabka_pgtypes::ColumnType;

    use crate::scope::{ColumnBinding, Exposure};

    #[test]
    fn only_the_false_literal_is_a_constant_false_qual() {
        assert!(is_literal_false(Some(&Expr::BoolLiteral(false))));
        assert!(!is_literal_false(Some(&Expr::BoolLiteral(true))));
        assert!(!is_literal_false(None));
    }

    #[test]
    fn distinguishes_single_and_multi_row_values() {
        assert!(is_single_row_values(&ValuesStmt {
            rows: vec![vec![Expr::IntLiteral("1".into())]],
        }));
        assert!(!is_single_row_values(&ValuesStmt {
            rows: vec![
                vec![Expr::IntLiteral("1".into())],
                vec![Expr::IntLiteral("2".into())],
            ],
        }));
    }

    #[test]
    fn reduces_only_a_not_null_column_test_in_its_own_scope() {
        let mut column_metadata = crabka_pgcatalog::Column::new("a", ColumnType::Int4);
        column_metadata.not_null = true;
        let table = Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: crabka_pgcatalog::RelationName::public("t"),
            columns: vec![column_metadata],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        };
        let column = Expr::Column {
            table: Some("t".into()),
            name: "a".into(),
        };
        assert!(
            reduce_not_null_test(
                Some(&Expr::IsNull {
                    expr: Box::new(column.clone()),
                    negated: false,
                }),
                &table,
                "t",
            ) == Some(Expr::BoolLiteral(false))
        );
        assert!(
            reduce_not_null_test(
                Some(&Expr::IsNull {
                    expr: Box::new(column),
                    negated: true,
                }),
                &table,
                "other",
            )
            .is_some_and(|expr| !matches!(expr, Expr::BoolLiteral(_)))
        );
    }

    #[test]
    fn rewrites_a_typed_self_equality_but_not_a_type_without_equality() {
        let column = Expr::Column {
            table: Some("t".into()),
            name: "a".into(),
        };
        let equality = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(column.clone()),
            right: Box::new(column.clone()),
        };
        let mut scope = Scope::empty();
        scope.columns.push(ColumnBinding {
            qualifier: Some("t".into()),
            name: "a".into(),
            ty: ColumnType::Int4,
            exposure: Exposure::Output,
        });
        assert!(
            rewrite_self_equality(Some(&equality), &scope)
                == Some(Expr::IsNull {
                    expr: Box::new(column.clone()),
                    negated: true,
                })
        );
        scope.columns[0].ty = ColumnType::Json;
        assert!(rewrite_self_equality(Some(&equality), &scope) == Some(equality));
    }

    #[test]
    fn rewrites_self_equality_inside_boolean_qual_trees() {
        let column = Expr::Column {
            table: Some("t".into()),
            name: "a".into(),
        };
        let equality = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(column.clone()),
            right: Box::new(column.clone()),
        };
        let filter = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::BoolLiteral(true)),
            right: Box::new(equality),
        };
        let mut scope = Scope::empty();
        scope.columns.push(ColumnBinding {
            qualifier: Some("t".into()),
            name: "a".into(),
            ty: ColumnType::Int4,
            exposure: Exposure::Output,
        });
        assert!(
            rewrite_self_equality(Some(&filter), &scope)
                == Some(Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(Expr::BoolLiteral(true)),
                    right: Box::new(Expr::IsNull {
                        expr: Box::new(column),
                        negated: true,
                    }),
                })
        );
    }
}
