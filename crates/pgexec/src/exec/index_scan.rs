//! Local-index probes used by read and write execution.

use super::*;

pub(super) fn lookup_local_index_equal(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    index: &krabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<Vec<ScannedRow>, ExecError> {
    let prefix = krabka_pgkv::key::secondary_index_entry_prefix(table.id, index.id, values);
    let entries = mvcc.kv.scan_prefix(&prefix)?;
    let mut rowids = BTreeSet::new();
    for (key, _) in entries {
        rowids.insert(krabka_pgkv::key::secondary_index_rowid_of(
            table.id, index.id, &key,
        )?);
    }

    let mut exact = Vec::new();
    for candidate in visible_rows_for_rowids(mvcc, table, rowids)? {
        if indexed_values(table, index, &candidate.row)? == values {
            exact.push(candidate);
        }
    }
    Ok(exact)
}

/// Read one local B-tree in physical key order. The ordered stream is separate
/// from equality entries so it cannot alter uniqueness or foreign-key probes.
pub(super) fn lookup_local_index_ordered(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    index: &krabka_pgcatalog::Index,
) -> Result<Option<Vec<ScannedRow>>, ExecError> {
    if !local_index_supports_ordered_scan(table, index) {
        return Ok(None);
    }
    let prefix = krabka_pgkv::key::secondary_index_ordered_prefix(table.id, index.id);
    let mut rows = Vec::new();
    for (key, _) in mvcc.kv.scan_prefix(&prefix)? {
        let rowid = krabka_pgkv::key::secondary_index_rowid_of(table.id, index.id, &key)?;
        rows.extend(visible_rows_for_rowids(
            mvcc,
            table,
            BTreeSet::from([rowid]),
        )?);
    }
    Ok(Some(rows))
}

pub(super) fn local_index_supports_ordered_scan(
    table: &Table,
    index: &krabka_pgcatalog::Index,
) -> bool {
    index.placement == krabka_pgcatalog::IndexPlacement::Local
        && index.method == krabka_pgcatalog::IndexMethod::Btree
        && index.predicate.is_none()
        && index.columns.len() == index.key_options.len()
        && index
            .columns
            .iter()
            .zip(&index.key_options)
            .all(|(column, option)| {
                option
                    .collation
                    .as_deref()
                    .is_none_or(|collation| matches!(collation, "C" | "POSIX"))
                    && table
                        .column_index(column)
                        .is_some_and(|column| ordered_column_type(table.columns[column].ty))
            })
}

fn ordered_column_type(ty: krabka_pgtypes::ColumnType) -> bool {
    matches!(
        ty,
        krabka_pgtypes::ColumnType::Bool
            | krabka_pgtypes::ColumnType::Int2
            | krabka_pgtypes::ColumnType::Int4
            | krabka_pgtypes::ColumnType::Int8
            | krabka_pgtypes::ColumnType::Text
            | krabka_pgtypes::ColumnType::Varchar(_)
            | krabka_pgtypes::ColumnType::InternalChar
            | krabka_pgtypes::ColumnType::Float4
            | krabka_pgtypes::ColumnType::Float8
            | krabka_pgtypes::ColumnType::Bytea
            | krabka_pgtypes::ColumnType::Oid
            | krabka_pgtypes::ColumnType::Xid
            | krabka_pgtypes::ColumnType::Xid8
            | krabka_pgtypes::ColumnType::Cid
            | krabka_pgtypes::ColumnType::PgLsn
            | krabka_pgtypes::ColumnType::Money
            | krabka_pgtypes::ColumnType::JsonPath
            | krabka_pgtypes::ColumnType::Array(
                krabka_pgtypes::ElemType::Bool
                    | krabka_pgtypes::ElemType::Int2
                    | krabka_pgtypes::ElemType::Int4
                    | krabka_pgtypes::ElemType::Int8
                    | krabka_pgtypes::ElemType::Text
                    | krabka_pgtypes::ElemType::Float4
                    | krabka_pgtypes::ElemType::Float8
                    | krabka_pgtypes::ElemType::Bytea
                    | krabka_pgtypes::ElemType::JsonPath
                    | krabka_pgtypes::ElemType::Varchar(_)
            )
    )
}

pub(super) fn lookup_local_gin(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    index: &krabka_pgcatalog::Index,
    query: &krabka_pgtypes::TsQuery,
) -> Result<Option<Vec<ScannedRow>>, ExecError> {
    let Some(rowids) = gin_candidate_rowids(mvcc.kv, table, index, query)? else {
        return Ok(None);
    };
    let column = table
        .column_index(&index.columns[0])
        .ok_or_else(|| ExecError::UndefinedColumn(index.columns[0].clone()))?;
    Ok(Some(
        visible_rows_for_rowids(mvcc, table, rowids)?
            .into_iter()
            .filter(|candidate| {
                matches!(&candidate.row[column], Datum::TsVector(vector) if vector.matches(query))
            })
            .collect(),
    ))
}

pub(super) fn gin_candidate_rowids(
    kv: &dyn Kv,
    table: &Table,
    index: &krabka_pgcatalog::Index,
    query: &krabka_pgtypes::TsQuery,
) -> Result<Option<BTreeSet<u64>>, ExecError> {
    use krabka_pgtypes::TsQuery;

    match query {
        TsQuery::Empty => Ok(Some(BTreeSet::new())),
        TsQuery::Term(term) if term.prefix => Ok(None),
        TsQuery::Term(term) => {
            let prefix = krabka_pgkv::key::secondary_index_entry_prefix(
                table.id,
                index.id,
                &[Datum::Text(term.text.clone())],
            );
            let mut rowids = BTreeSet::new();
            for (key, _) in kv.scan_prefix(&prefix)? {
                rowids.insert(krabka_pgkv::key::secondary_index_rowid_of(
                    table.id, index.id, &key,
                )?);
            }
            Ok(Some(rowids))
        }
        TsQuery::Not(_) => Ok(None),
        TsQuery::And(left, right) | TsQuery::Phrase(left, right, _) => {
            let left = gin_candidate_rowids(kv, table, index, left)?;
            let right = gin_candidate_rowids(kv, table, index, right)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(&left & &right),
                (Some(candidates), None) | (None, Some(candidates)) => Some(candidates),
                (None, None) => None,
            })
        }
        TsQuery::Or(left, right) => {
            let left = gin_candidate_rowids(kv, table, index, left)?;
            let right = gin_candidate_rowids(kv, table, index, right)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(&left | &right),
                _ => None,
            })
        }
    }
}

pub(super) fn visible_rows_for_rowids(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    rowids: BTreeSet<u64>,
) -> Result<Vec<ScannedRow>, ExecError> {
    let mut rows = Vec::new();
    for rowid in rowids {
        let row_prefix = krabka_pgkv::key::row_key(table.id, rowid);
        let versions = mvcc
            .kv
            .scan_prefix(&row_prefix)?
            .iter()
            .map(|(_, value)| {
                let (xmin, xmax, cmin, cmax, row) =
                    krabka_pgmvcc::version::decode_tuple_with_command_ids(value)?;
                Ok((xmin, xmax, cmin, cmax, row))
            })
            .collect::<Result<Vec<_>, krabka_pgkv::KvError>>()?;
        let Some((xmin, cmin, cmax, row)) = find_visible_one_with_command_ids(
            mvcc.kv,
            mvcc.global,
            mvcc.global_snapshot,
            mvcc.snapshot,
            mvcc.own,
            mvcc.command_id,
            &versions,
        )?
        else {
            continue;
        };
        rows.push(ScannedRow {
            rowid,
            xmin,
            cmin,
            cmax,
            row,
        });
    }
    Ok(rows)
}

/// Choose the index probe for an UPDATE/DELETE filter: a top-level
/// `column = literal` conjunct matching a single-column local index. Returns
/// `None`, which means a full scan, for sharded tables, for filters outside the
/// pushdown subset, and when no index matches. It reuses the SELECT path's
/// extraction, so only exact-type literals on supported column types qualify.
pub(super) fn choose_write_index_probe(
    catalog_kv: &dyn Kv,
    table: &Table,
    filter: Option<&Expr>,
) -> Result<Option<(krabka_pgcatalog::Index, Datum)>, ExecError> {
    if table.sharded {
        return Ok(None);
    }
    let predicate = crate::plan_dist::predicate_for_filter(table, filter);
    choose_local_index_equality(catalog_kv, table, &predicate)
}

/// Candidate `(rowid, xmin, row)` source for UPDATE/DELETE: probe a matching
/// local index instead of scanning the whole table when the filter pins an
/// indexed column to a literal, else fall back to `scan_live`. Both paths read
/// under the statement's snapshot/gsnap/own visibility and return rows sorted
/// by rowid; the caller still applies the FULL residual filter and the
/// under-lock EvalPlanQual re-check to every candidate, so the affected rows,
/// RETURNING output, and lock order are identical to the full scan.
/// The stored rows an `UPDATE`/`DELETE`/`MERGE` may act on, after privileges
/// and row security.
///
/// This is the write side's counterpart to the read gate: an `UPDATE` that
/// could change a row a `SELECT` would not have shown is the exact hazard the
/// programme exists to avoid, so both the privilege test and the `USING` qual
/// are applied here, where every row-selecting write path already passes.
///
/// The privilege check runs before the rows are gathered, and the two decisions
/// are driven by one [`crate::privilege::WriteAction`] rather than by a
/// `PolicyCommand` beside a privilege — a caller handed both can pair them
/// wrongly, and `TRUNCATE` (a `DELETE` policy command needing the `TRUNCATE`
/// privilege) is exactly the pairing that would be got wrong.
pub(super) fn write_candidate_rows(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    action: crate::privilege::WriteAction,
    filter: Option<&Expr>,
    reads_target_columns: bool,
    reads: crate::scope::GeneratedReads<'_>,
) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
    let governor = write_ctx.governor(table);
    crate::privilege::require_write(
        &write_ctx.privileges(),
        governor,
        action,
        reads_target_columns,
    )?;
    let using = crate::rls::RowSecurityUsing::compile(
        &write_ctx.policy_read_ctx(),
        governor,
        action.policy_command(),
    )?;
    let mut rows: Vec<(u64, u64, Vec<Datum>)> = if let Some((index, value)) =
        choose_write_index_probe(write_ctx.catalog_kv, table, filter)?
    {
        lookup_local_index_equal(&write_ctx.mvcc_read(), table, &index, &[value])?
            .into_iter()
            .map(|row| (row.rowid, row.xmin, row.row))
            .collect()
    } else {
        scan_live(
            write_ctx.kv,
            write_ctx.global,
            write_ctx.global_snapshot,
            write_ctx.snapshot,
            Some(write_ctx.xid),
            table,
        )?
    };
    // The `USING` qual, the statement's own WHERE and any `RETURNING old.*` all
    // read these rows, so a virtual generated column each of them can reach has
    // to hold its value before any of them run — and one none of them can reach
    // must NOT be computed, or a row whose expression raises can never be
    // deleted. What "can reach" means is settled by the caller, in
    // [`read_generated_reads`], because the row this scan produces is re-read
    // under its lock by `eval_plan_qual` and the two must agree.
    for (_, _, row) in &mut rows {
        expand_virtual_generated_row(table, row, write_ctx.eval_ctx, reads)?;
    }
    using.retain_visible(table, rows, write_ctx.eval_ctx)
}
