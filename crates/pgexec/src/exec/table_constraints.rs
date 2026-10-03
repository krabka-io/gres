use super::*;

pub(super) fn resolve_relation_tablespace_oid(kv: &dyn Kv, name: &str) -> Result<u32, ExecError> {
    if name == "pg_global" {
        return Err(ExecError::Remote(krabka_pgwire::error::PgError::error(
            "0A000",
            "only shared relations can be placed in pg_global tablespace",
        )));
    }
    krabka_pgcatalog::tablespace_oid(kv, name).map_err(|error| match error {
        krabka_pgcatalog::CatalogError::UndefinedObject(_) => {
            ExecError::Remote(krabka_pgwire::error::PgError::error(
                "42704",
                format!("tablespace \"{name}\" does not exist"),
            ))
        }
        other => other.into(),
    })
}

pub(super) fn constraint_deferral(
    attributes: krabka_pgparser::ast::ConstraintAttributes,
) -> krabka_pgcatalog::ConstraintDeferral {
    krabka_pgcatalog::ConstraintDeferral::of(attributes.deferrable, attributes.initially_deferred)
}

pub(super) fn create_table_constraint_index(
    table_name: &krabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
    without_overlaps: bool,
    deferral: krabka_pgcatalog::ConstraintDeferral,
) -> krabka_pgcatalog::NewIndex {
    let suffix = if primary_key { "pkey" } else { "key" };
    let table = &table_name.name;
    let name = if primary_key {
        format!("{table}_pkey")
    } else {
        format!("{table}_{}_{suffix}", columns.join("_"))
    };
    krabka_pgcatalog::NewIndex {
        name,
        columns: columns.to_vec(),
        key_options: krabka_pgcatalog::default_index_key_options(columns.len()),
        include: Vec::new(),
        predicate: None,
        nulls_not_distinct: false,
        unique: true,
        placement: krabka_pgcatalog::IndexPlacement::Local,
        method: if without_overlaps {
            krabka_pgcatalog::IndexMethod::Gist
        } else {
            krabka_pgcatalog::IndexMethod::Btree
        },
        constraint: Some(if primary_key {
            krabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            krabka_pgcatalog::IndexConstraint::Unique
        }),
        without_overlaps,
        deferral,
    }
}

pub(super) fn validate_without_overlaps_key(
    columns: &[String],
    table_columns: &[Column],
) -> Result<(), ExecError> {
    let Some((temporal, leading)) = columns.split_last() else {
        return Err(ExecError::WithoutOverlapsNeedsTwoColumns);
    };
    if leading.is_empty() {
        return Err(ExecError::WithoutOverlapsNeedsTwoColumns);
    }
    let column = table_columns
        .iter()
        .find(|column| column.name == *temporal)
        .ok_or_else(|| ExecError::UndefinedIndexColumn(temporal.clone()))?;
    if !matches!(
        column.ty.storage_type(),
        krabka_pgtypes::ColumnType::Range(_) | krabka_pgtypes::ColumnType::Multirange(_)
    ) {
        return Err(ExecError::WithoutOverlapsNotRange(temporal.clone()));
    }
    Ok(())
}

pub(super) fn create_table_primary_key_columns<'a>(
    columns: &'a [krabka_pgparser::ast::ColumnDef],
    constraints: &'a [krabka_pgparser::ast::TableConstraint],
) -> HashSet<&'a str> {
    let mut primary_key_columns = HashSet::new();
    for column in columns {
        if column.constraints.iter().any(|constraint| {
            matches!(
                constraint.kind,
                krabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
            )
        }) {
            primary_key_columns.insert(column.name.as_str());
        }
    }
    for constraint in constraints {
        if let krabka_pgparser::ast::TableConstraintKind::PrimaryKey { columns, .. } =
            &constraint.kind
        {
            primary_key_columns.extend(columns.iter().map(String::as_str));
        }
    }
    primary_key_columns
}
