//! Test-helper facade for `krabka-broker`.
//!
//! This nested crate is unpublished on purpose. Broker integration tests
//! depend on it to activate `krabka-broker/test-helpers`, so that
//! `krabka-broker` does not have to dev-depend on itself.

pub use krabka_broker::*;
