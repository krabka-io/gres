//! Helpers shared by the `Gres` and `GresTenant` reconcilers.
//!
//! The module holds the reconcile error surface, the requeue and time
//! conversions, the server-side-apply and owner-reference helpers, the
//! condition builder, the PEM expiry reader, and the ACL diff that the tenant
//! reconciler applies to its Kafka principal.

use std::{collections::BTreeSet, fmt::Debug, future::Future, pin::Pin, sync::Arc};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use krabka_client_admin::{AclEntry, AclEntryFilter};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use kube::{
    Resource,
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use serde::{Serialize, de::DeserializeOwned};
use time::OffsetDateTime;

use crate::{context::Context, crd::Condition};

/// Server-side-apply field manager of every object this operator writes.
pub(crate) const FIELD_MANAGER: &str = "krabka-gres-operator";

pub(super) fn error_requeue(ctx: Arc<Context>) -> Action {
    let delay = ctx.config.controller_error_requeue;
    drop(ctx);
    requeue(delay)
}

pub(crate) fn requeue(delay: Time) -> Action {
    Action::requeue(delay.to_std())
}

/// A millisecond count held as `u64` — a `refined_type` newtype such as
/// `krabka_gres_control`'s `PositiveMillis` — as a time extent.
/// [`TimeExt::from_millis`] takes an `i64`, so a value past `i64::MAX`
/// milliseconds saturates rather than wrapping negative.
pub(crate) fn time_from_millis_u64(millis: u64) -> Time {
    Time::from_millis(i64::try_from(millis).unwrap_or(i64::MAX))
}

/// A time extent back in whole milliseconds as `u64`, for arithmetic against
/// the epoch-millisecond instants the Gres status fields carry. A negative
/// extent clamps to zero.
pub(crate) fn millis_u64(extent: Time) -> u64 {
    u64::try_from(extent.millis_i64()).unwrap_or_default()
}

/// Reconcile-error surface shared by both reconcilers.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("resource missing uid (not yet admitted)")]
    MissingUid,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("secret malformed: {0}")]
    MalformedSecret(String),
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("CA: {0}")]
    Ca(#[from] krabka_security::ca::CaError),
    #[error("cert parse: {0}")]
    CertParse(String),
    /// `Gres.spec.tracing` failed shape validation. Concrete cases:
    /// `type = "Otlp"` without an `otlp` block, an empty `otlp.endpoint`, a
    /// `sampleRatio` outside `[0.0, 1.0]`, or a non-positive timeout. The
    /// reconciler returns this before it renders any pod template, so a
    /// compute pod never boots with broken OTLP env vars.
    #[error("tracing: {0}")]
    TracingInvalid(String),
    #[error("gres control: {0}")]
    GresControl(#[from] krabka_gres_control::ControlError),
    #[error("producer error: {0}")]
    Producer(#[from] krabka_client_producer::ProducerError),
    #[error("gres control write: {0}")]
    GresControlWrite(#[from] crate::context::GresControlWriteError),
    #[error("admin error: {0}")]
    Admin(#[from] krabka_client_admin::AdminError),
    #[error("pgdog admin error: {0}")]
    PgdogAdmin(#[from] crate::context::PgdogAdminError),
}

/// Build a Kubernetes-style condition with `lastTransitionTime` set to
/// now (RFC3339, second precision, with `Z`).
pub(crate) fn condition(type_: &str, status: &str, reason: &str, message: &str) -> Condition {
    Condition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    }
}

/// Time a reconcile future and record the shared reconcile metric.
pub(crate) async fn record_reconcile<E, F>(
    ctx: &Context,
    kind: &'static str,
    reconcile: Pin<Box<F>>,
) -> Result<Action, E>
where
    F: Future<Output = Result<Action, E>> + ?Sized,
{
    let started = std::time::Instant::now();
    let result = reconcile.await;
    let outcome = if result.is_ok() {
        crate::telemetry::ReconcileResult::Ok
    } else {
        crate::telemetry::ReconcileResult::Error
    };
    ctx.metrics
        .record_reconcile(kind, outcome, started.elapsed().as_time());
    result
}

/// Server-side apply a typed object. The field manager is
/// [`FIELD_MANAGER`]. Force-takeover is on, so the operator wrests fields
/// back from any previous manager. The object shape is stable across
/// reconciles because the renderers are pure functions of the owner.
#[tracing::instrument(level = "debug", skip_all, fields(name = %name), err)]
pub(crate) async fn apply_object<K>(api: &Api<K>, name: &str, obj: &K) -> Result<(), ReconcileError>
where
    K: Resource + Clone + Serialize + DeserializeOwned + Debug,
{
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Apply(obj)).await?;
    Ok(())
}

/// Generic owner-reference builder. It works for any CR whose
/// `DynamicType = ()`. It reads `apiVersion` and `kind` from the trait and
/// the name from the metadata.
pub(crate) fn owner_ref<T>(obj: &T) -> Result<OwnerReference, ReconcileError>
where
    T: Resource<DynamicType = ()>,
{
    let uid = obj
        .meta()
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    Ok(OwnerReference {
        api_version: T::api_version(&()).to_string(),
        kind: T::kind(&()).to_string(),
        name: obj.meta().name.clone().unwrap_or_default(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

/// Reads the `notAfter` instant of the first certificate in `pem`.
pub(crate) fn cert_not_after(pem: &str) -> Result<OffsetDateTime, ReconcileError> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    use x509_parser::prelude::{FromDer, X509Certificate};
    let der = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or_else(|| ReconcileError::CertParse("no PEM block".into()))?
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    OffsetDateTime::from_unix_timestamp(cert.validity().not_after.timestamp())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))
}

/// Builds an `AclEntryFilter` that matches exactly one `AclEntry`.
///
/// This limits `DeleteAcls` to one tuple at a time.
pub(crate) fn entry_to_exact_filter(e: &AclEntry) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(e.resource_type),
        resource_name: Some(e.resource_name.clone()),
        pattern_type: Some(e.pattern_type),
        principal: Some(e.principal.clone()),
        host: Some(e.host.clone()),
        operation: Some(e.operation),
        permission_type: Some(e.permission_type),
    }
}

/// Splits the ACL change into `(additions, deletions)`.
///
/// `additions` holds the entries that are in `desired` and not in
/// `current`. `deletions` holds the entries that are in `current` and not
/// in `desired`.
pub(crate) fn diff_acls(
    current: &BTreeSet<AclEntry>,
    desired: &BTreeSet<AclEntry>,
) -> (Vec<AclEntry>, Vec<AclEntry>) {
    let additions: Vec<AclEntry> = desired.difference(current).cloned().collect();
    let deletions: Vec<AclEntry> = current.difference(desired).cloned().collect();
    (additions, deletions)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_client_admin::{AclOperation, PatternType, PermissionType, ResourceType};

    use super::*;

    fn entry(resource_type: ResourceType, name: &str) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    #[test]
    fn diff_acls_additions_and_deletions() {
        let keep = entry(ResourceType::Topic, "keep");
        let drop = entry(ResourceType::Topic, "drop");
        let add = entry(ResourceType::Group, "g");
        let current = BTreeSet::from([keep.clone(), drop.clone()]);
        let desired = BTreeSet::from([keep, add.clone()]);

        let (additions, deletions) = diff_acls(&current, &desired);

        assert!(additions == vec![add]);
        assert!(deletions == vec![drop]);
    }

    #[test]
    fn diff_acls_noop_when_matching() {
        let set = BTreeSet::from([entry(ResourceType::Topic, "x")]);
        let (additions, deletions) = diff_acls(&set, &set);
        assert!(additions.is_empty());
        assert!(deletions.is_empty());
    }

    #[test]
    fn entry_to_exact_filter_populates_every_axis() {
        let filter = entry_to_exact_filter(&entry(ResourceType::Topic, "orders"));
        assert!(
            filter
                == AclEntryFilter {
                    resource_type: Some(ResourceType::Topic),
                    resource_name: Some("orders".into()),
                    pattern_type: Some(PatternType::Literal),
                    principal: Some("User:alice".into()),
                    host: Some("*".into()),
                    operation: Some(AclOperation::Read),
                    permission_type: Some(PermissionType::Allow),
                }
        );
    }

    #[test]
    fn cert_not_after_rejects_input_without_pem_block() {
        assert!(matches!(
            cert_not_after("not a certificate"),
            Err(ReconcileError::CertParse(_))
        ));
    }

    #[test]
    fn time_conversions_saturate_and_clamp() {
        assert!(time_from_millis_u64(u64::MAX) == Time::from_millis(i64::MAX));
        assert!(millis_u64(Time::from_millis(-5)) == 0);
        assert!(millis_u64(Time::from_millis(1_500)) == 1_500);
    }
}
