//! Shared test harness for the operator's integration tests.
//!
//! Each integration test file (`reconcile_gres.rs`,
//! `reconcile_gres_tenant.rs`) includes this module via
//! `#[path = "shared/mod.rs"] mod shared;`.
//!
//! The harness wires a `tower::Service` mock that:
//!   - matches incoming requests against an ordered list of `MockRule`s. The
//!     order is FIFO: the first rule that matches wins and is consumed;
//!   - captures every observed request, so the test body can assert on
//!     methods, URIs, and bodies;
//!   - falls through to a 404 when no rule matches. That 404 fails the test
//!     and shows the unexpected request.

#![allow(dead_code)]

pub mod fake_admin;

use std::sync::{Arc, Mutex};

use http::{Method, Request, Response};
use http_body_util::BodyExt as _;
use hyper::body::Bytes;
use krabka_gres_operator::{
    config::OperatorConfig, context::Context, telemetry::new_registry_with_metrics,
};
use kube::Client;
use tokio::sync::Mutex as AsyncMutex;
use tower::{ServiceBuilder, service_fn};

/// One preloaded mock response. The mock matches on `(method, path_substr)`
/// against the incoming request URI. A substring match is enough, because
/// kube's generated paths are deterministic and unambiguous.
pub struct MockRule {
    pub method: Method,
    pub path_substr: String,
    pub response: Response<Vec<u8>>,
}

/// Shared mock state: an ordered queue of rules with FIFO consumption, and
/// the list of every observed request. The list holds a request whether or not
/// a rule matched it.
pub struct MockState {
    pub rules: Mutex<Vec<MockRule>>,
    pub observed: Mutex<Vec<Request<Bytes>>>,
}

impl MockState {
    pub fn new(rules: Vec<MockRule>) -> Arc<Self> {
        Arc::new(Self {
            rules: Mutex::new(rules),
            observed: Mutex::new(Vec::new()),
        })
    }

    pub fn take_observed(&self) -> Vec<Request<Bytes>> {
        std::mem::take(&mut *self.observed.lock().unwrap())
    }

    pub fn remaining_rules(&self) -> usize {
        self.rules.lock().unwrap().len()
    }
}

/// Build a kube `Client` whose underlying transport is the FIFO rule matcher
/// described above. Each call records the request bytes before it returns the
/// canned response.
pub fn mock_client(state: &Arc<MockState>, default_ns: &str) -> Client {
    let state_for_svc = state.clone();
    let svc = ServiceBuilder::new().service(service_fn(move |req: Request<kube::client::Body>| {
        let state = state_for_svc.clone();
        async move {
            let (parts, body) = req.into_parts();
            let bytes = body.collect().await.unwrap().to_bytes();
            let captured = Request::from_parts(parts.clone(), bytes);
            state.observed.lock().unwrap().push(captured);

            // FIFO: walk the rule list, take the first match.
            let response = {
                let mut rules = state.rules.lock().unwrap();
                let uri_str = parts.uri.to_string();
                let pos = rules
                    .iter()
                    .position(|r| r.method == parts.method && uri_str.contains(&r.path_substr));
                pos.map(|i| rules.remove(i)).map(|r| r.response)
            };

            let response = response.unwrap_or_else(|| {
                Response::builder()
                    .status(404)
                    .header("content-type", "application/json")
                    .body(not_found_body("unexpected"))
                    .expect("404 response builds")
            });

            let (rp, rb) = response.into_parts();
            Ok::<_, kube::Error>(Response::from_parts(rp, kube::client::Body::from(rb)))
        }
    }));
    Client::new(svc, default_ns)
}

/// Build the apimachinery `Status` body that kube-rs parses to recognize a
/// 404. kube uses the `code` field to construct `kube::Error::Api`.
pub fn not_found_body(message: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "code": 404,
        "reason": "NotFound",
        "message": message,
    }))
    .expect("status body serializes")
}

pub fn json_response(status: u16, body: &serde_json::Value) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(body).expect("body serializes"))
        .expect("response builds")
}

/// Build an `OperatorConfig` with the fixture defaults.
pub fn op_config(namespace: &str) -> OperatorConfig {
    OperatorConfig {
        watch_namespaces: vec![],
        operator_namespace: namespace.into(),
        lease_name: "l".into(),
        pod_name: "p".into(),
        health_addr: "0.0.0.0:0".parse().unwrap(),
        client_dispatch_queue_capacity:
            krabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        client_frame_max: krabka_units::mebibytes(100),
        log_filter: "info".into(),
        default_gres_image: None,
        default_pgdog_image: None,
        default_gres_activator_image: None,
        pgdog_reload_attempts: "3".parse().unwrap(),
        pgdog_reload_backoff: krabka_units::millis(100),
        pgdog_reload_requeue: krabka_units::secs(15),
        pgdog_admin_timeout: krabka_units::secs(20),
        pgdog_transition_poll: krabka_units::minutes(1),
        controller_error_requeue: krabka_units::secs(15),
        controller_dependency_requeue: krabka_units::secs(30),
        controller_invalid_requeue: krabka_units::minutes(5),
        leader_lease_duration: krabka_units::secs(15),
        leader_retry_interval: krabka_units::secs(2),
        topic_mutation_timeout: krabka_units::secs(30),
        gres_checkpoint_store: None,
        gres_checkpoint_bucket: None,
        gres_checkpoint_region: None,
        gres_checkpoint_endpoint: None,
        gres_checkpoint_allow_http: false,
        gres_checkpoint_access_key_id: None,
        gres_checkpoint_secret_access_key: None,
        gres_checkpoint_gcs_service_account_path: None,
        gres_checkpoint_gcs_application_credentials_path: None,
    }
}

/// Build a `Context` wired to the supplied mock client.
pub fn fixture_ctx(client: kube::Client, namespace: &str) -> Context {
    let (registry, metrics) = new_registry_with_metrics();
    Context::new(
        client,
        op_config(namespace),
        Arc::new(AsyncMutex::new(registry)),
        metrics,
    )
}
