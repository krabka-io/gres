//! CRD type definitions.
//!
//! Each kind lives in its own submodule. That submodule is the single
//! source of truth for the runtime types and for the generated CRD YAML
//! manifest. See `gen_crds`.

pub mod condition;
pub mod gres;
pub mod gres_tenant;
pub mod tracing;

pub use condition::Condition;
pub use gres::{
    Gres, GresActivatorSpec, GresBalancerGoal, GresBalancerGoals, GresBalancerOperationKind,
    GresBalancerPlanSnapshot, GresBalancerRegistryLayout, GresBalancerSpec, GresBalancerStatus,
    GresBalancerThresholds, GresKafkaSpec, GresRegistrySpec, GresSpec, GresStatus,
    KafkaCredentialsSecretRef, PgdogPoolerModeSpec, PgdogSpec, SecretKeyRef, SecretRef,
    TenantDefaults,
};
pub use gres_tenant::{
    GresTenant, GresTenantRangeKey, GresTenantRangeSpec, GresTenantSpec, GresTenantStatus,
};
pub use tracing::{OtlpProtocol, OtlpTracing, Tracing, TracingType};
