//! Rule-based rewrites that are sound before cost-based planning.

use crabka_pgparser::ast::Expr;

use crate::scope::Scope;

/// Whether a WHERE clause is the literal boolean `false`.
///
/// This is the first Phase-1 constant-qual rewrite. It does not evaluate an
/// expression, so volatile calls and errors remain on their ordinary paths.
pub(crate) fn is_literal_false(filter: Option<&Expr>) -> bool {
    matches!(filter, Some(Expr::BoolLiteral(false)))
}

/// Rewrite a typed `column = column` qual to `column IS NOT NULL`.
///
/// The equality-operator check is essential: `IS NOT NULL` is available for
/// every type, while `=` is not. Keeping the original expression in that case
/// preserves the analysis error the query owes.
pub(crate) fn rewrite_self_equality(filter: Option<&Expr>, scope: &Scope) -> Option<Expr> {
    filter.map(|filter| match filter {
        Expr::Binary {
            op: crabka_pgparser::ast::BinaryOp::Eq,
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
        _ => filter.clone(),
    })
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
}
