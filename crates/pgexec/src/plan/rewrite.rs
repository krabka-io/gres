//! Rule-based rewrites that are sound before cost-based planning.

use crabka_pgparser::ast::Expr;

/// Whether a WHERE clause is the literal boolean `false`.
///
/// This is the first Phase-1 constant-qual rewrite. It does not evaluate an
/// expression, so volatile calls and errors remain on their ordinary paths.
pub(crate) fn is_literal_false(filter: Option<&Expr>) -> bool {
    matches!(filter, Some(Expr::BoolLiteral(false)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_false_literal_is_a_constant_false_qual() {
        assert!(is_literal_false(Some(&Expr::BoolLiteral(false))));
        assert!(!is_literal_false(Some(&Expr::BoolLiteral(true))));
        assert!(!is_literal_false(None));
    }
}
