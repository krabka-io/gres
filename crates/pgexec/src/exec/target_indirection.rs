//! INSERT and UPDATE target indirection assignment.

use super::*;

/// The array element type beneath any domain wrappers.
fn array_assignment_element(ty: ColumnType) -> Option<crabka_pgtypes::ElemType> {
    match ty {
        ColumnType::Domain(domain) => array_assignment_element(*domain.base),
        ty => ty.array_element(),
    }
}

fn assign_point_subscript(
    base: &Datum,
    args: &[crate::array_fn::SubscriptArg],
    value: &Datum,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let [crate::array_fn::SubscriptArg::Index(index)] = args else {
        return Err(ExecError::Unsupported(
            "slices of fixed-length arrays not implemented".into(),
        ));
    };
    let Some(index) = crate::array_fn::subscript_int(index)? else {
        return Ok(base.clone());
    };
    let &Datum::Point(mut point) = base else {
        return Ok(Datum::Null);
    };
    if value.is_null() {
        return Ok(base.clone());
    }
    let Datum::Float8(value) = coerce(value.clone(), ColumnType::Float8, ctx)? else {
        unreachable!("float8 coercion changes the datum variant")
    };
    match index {
        0 => point.x = value,
        1 => point.y = value,
        _ => {
            return Err(ExecError::Type(crabka_pgtypes::TypeError::array_subscript(
                "array subscript out of range",
            )));
        }
    }
    Ok(Datum::Point(point))
}

/// The type at the end of an INSERT/UPDATE target's indirection path.
pub(super) fn target_indirection_type(
    ty: ColumnType,
    indirections: &[TargetIndirection],
) -> Result<ColumnType, ExecError> {
    let Some((first, rest)) = indirections.split_first() else {
        return Ok(ty);
    };
    match first {
        TargetIndirection::Field(field) => {
            let ColumnType::Record(Some(named)) = ty.storage_type() else {
                return Err(ExecError::TypeMismatch(format!(
                    "column notation .{field} applied to type {}, which is not a composite type",
                    ty.name()
                )));
            };
            let definition = crabka_pgtypes::usertype::lookup_oid(named.oid).ok_or_else(|| {
                ExecError::UndefinedObject(format!("type \"{}\" does not exist", named.name))
            })?;
            let fields = definition.fields().unwrap_or(&[]);
            let field = fields
                .iter()
                .find(|candidate| candidate.name == *field)
                .ok_or_else(|| {
                    ExecError::UndefinedColumn(format!(
                        "column \"{field}\" not found in data type {}",
                        named.name
                    ))
                })?;
            target_indirection_type(field.ty, rest)
        }
        TargetIndirection::Subscript(_) => {
            let count = indirections
                .iter()
                .take_while(|step| matches!(step, TargetIndirection::Subscript(_)))
                .count();
            if let Some(elem) = array_assignment_element(ty) {
                let subscripts = &indirections[..count];
                let has_slice = subscripts.iter().any(|step| {
                    matches!(step, TargetIndirection::Subscript(subscript) if subscript.is_slice())
                });
                if has_slice && count == indirections.len() {
                    Ok(ty)
                } else {
                    target_indirection_type(elem.column_type(), &indirections[count..])
                }
            } else if count == indirections.len() {
                Ok(ty)
            } else {
                Err(ExecError::TypeMismatch(format!(
                    "cannot subscript type {} because it does not support subscripting",
                    ty.name()
                )))
            }
        }
    }
}

/// Replace a value below an INSERT/UPDATE target's ordered indirection path.
pub(super) fn assign_target_indirections(
    base: &Datum,
    ty: ColumnType,
    indirections: &[TargetIndirection],
    value: &Datum,
    scope: &Scope,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let Some((first, rest)) = indirections.split_first() else {
        return coerce(value.clone(), ty, ctx);
    };
    match first {
        TargetIndirection::Field(field) => {
            let ColumnType::Record(Some(named)) = ty.storage_type() else {
                return Err(ExecError::TypeMismatch(format!(
                    "column notation .{field} applied to type {}, which is not a composite type",
                    ty.name()
                )));
            };
            let definition = crabka_pgtypes::usertype::lookup_oid(named.oid).ok_or_else(|| {
                ExecError::UndefinedObject(format!("type \"{}\" does not exist", named.name))
            })?;
            let fields = definition.fields().unwrap_or(&[]);
            let index = fields
                .iter()
                .position(|candidate| candidate.name == *field)
                .ok_or_else(|| {
                    ExecError::UndefinedColumn(format!(
                        "column \"{field}\" not found in data type {}",
                        named.name
                    ))
                })?;
            let mut record = match base {
                Datum::Null => crabka_pgtypes::RecordValue::named(
                    Some(named),
                    Arc::from(
                        fields
                            .iter()
                            .map(|field| field.name.clone())
                            .collect::<Vec<_>>(),
                    ),
                    vec![Datum::Null; fields.len()],
                ),
                Datum::Record(record) if record.values.len() == fields.len() => record.clone(),
                // An ALTER TYPE ... ADD ATTRIBUTE leaves old stored values
                // narrower than the current composite definition. Align those
                // values by field name and supply NULL for the new attributes.
                Datum::Record(record) => crabka_pgtypes::RecordValue::named(
                    Some(named),
                    Arc::from(
                        fields
                            .iter()
                            .map(|field| field.name.clone())
                            .collect::<Vec<_>>(),
                    ),
                    fields
                        .iter()
                        .map(|field| record.field(&field.name).cloned().unwrap_or(Datum::Null))
                        .collect(),
                ),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "column notation .{field} applied to type {}, which is not a composite type",
                        other.column_type().map_or("unknown", ColumnType::name)
                    )));
                }
            };
            record.values[index] = assign_target_indirections(
                &record.values[index],
                fields[index].ty,
                rest,
                value,
                scope,
                row,
                ctx,
            )?;
            Ok(Datum::Record(record))
        }
        TargetIndirection::Subscript(_) => {
            let count = indirections
                .iter()
                .take_while(|step| matches!(step, TargetIndirection::Subscript(_)))
                .count();
            let subscripts = indirections[..count]
                .iter()
                .map(|step| match step {
                    TargetIndirection::Subscript(subscript) => subscript.clone(),
                    TargetIndirection::Field(_) => unreachable!("subscript prefix"),
                })
                .collect::<Vec<_>>();
            let args = crate::eval::eval_assignment_subscripts(&subscripts, scope, row, ctx)?;
            if let Some(elem) = array_assignment_element(ty) {
                if args.iter().any(crate::array_fn::SubscriptArg::is_slice)
                    && count == indirections.len()
                {
                    return crate::array_fn::array_assign(base, &args, value, elem, ctx);
                }
                let current = crate::array_fn::array_ref(base, &args)?;
                let replacement = assign_target_indirections(
                    &current,
                    elem.column_type(),
                    &indirections[count..],
                    value,
                    scope,
                    row,
                    ctx,
                )?;
                crate::array_fn::array_assign(base, &args, &replacement, elem, ctx)
            } else {
                if count != indirections.len() {
                    return Err(ExecError::TypeMismatch(format!(
                        "cannot subscript type {} because it does not support subscripting",
                        ty.name()
                    )));
                }
                if ty.storage_type() == ColumnType::Point {
                    return assign_point_subscript(base, &args, value, ctx);
                }
                if ty.storage_type() != ColumnType::Jsonb {
                    return Err(ExecError::TypeMismatch(format!(
                        "cannot subscript type {} because it does not support subscripting",
                        ty.name()
                    )));
                }
                let indexes = args
                    .iter()
                    .map(|arg| match arg {
                        crate::array_fn::SubscriptArg::Index(value) => Ok(value.clone()),
                        crate::array_fn::SubscriptArg::Slice { .. } => {
                            Err(ExecError::TypeMismatch(
                                "jsonb subscript does not support slices".into(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;
                crate::json_fn::jsonb_subscript_assign(base, &indexes, value)
            }
        }
    }
}
