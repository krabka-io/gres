//! Kubernetes operator for Crabka Gres fleets.
//!
//! The binary entry point is `src/main.rs`. This library exposes the reusable
//! parts: the `Gres` and `GresTenant` CRD types, their controllers,
//! telemetry, and leader election. You can unit-test these parts without the
//! binary.
//!
//! ## Runtime config scope
//!
//! ```rust
//! use assert2::assert;
//! use krabka_gres_operator::config::OperatorConfig;
//!
//! # fn example(mut config: OperatorConfig) {
//! config.watch_namespaces = vec!["gres-a".into(), "gres-b".into()];
//! assert!(config.watched().unwrap().len() == 2);
//!
//! config.watch_namespaces.clear();
//! assert!(config.watched().is_none());
//! # }
//! ```

pub mod config;
pub mod context;
pub mod controller;
pub mod crd;
pub mod gen_crds;
pub mod health;
pub mod leader_election;
pub mod run;
pub mod telemetry;
