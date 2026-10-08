//! Controllers, also called reconcilers, for the Gres CRDs.
//!
//! Each kind is in its own submodule. The submodules share the SSA,
//! owner-reference, condition, and requeue helpers from `common`.

pub mod common;
pub mod gres;
pub mod gres_split_operation;
pub mod gres_tenant;
