use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::Arc,
};

use krabka_client_admin::{AdminClient, AdminClientLike};
use krabka_gres_substrate::checkpoint::{Manifest, ManifestValidation};
use krabka_object_store::{
    GcsConfig, ObjectStoreConfig, S3Config, build_object_store, read_capped,
};
use kube::Client;
use object_store::path::Path;
use tokio::sync::Mutex;

use crate::{
    config::{GresCheckpointStoreKind, OperatorConfig},
    telemetry::{ControllerMetrics, SharedRegistry},
};

/// Boxed-dyn admin client handle.
///
/// Tests substitute a fake here and open no TCP connection. Production
/// code wraps a real `AdminClient`.
pub type AdminClientHandle = Arc<Mutex<dyn AdminClientLike + Send>>;

/// Write seam for the Gres control plane.
///
/// Production writes Kafka records with the idempotent producer. Tests
/// install an in-memory recorder.
pub type GresControlHandle = Arc<dyn GresControlLike>;

/// Narrow seam that verifies a durable checkpoint.
///
/// An implementation gets the referenced object and decodes it. It then
/// compares the tenant metadata and the checkpoint metadata of that object
/// with the registry record.
pub type CheckpointManifestVerifierHandle = Arc<dyn CheckpointManifestVerifier>;

/// Boxed `PgDog` admin seam.
///
/// Production uses the `PgDog` admin `PostgreSQL` endpoint. Tests install
/// a deterministic fake that can report stale views.
pub type PgdogAdminHandle = Arc<dyn PgdogAdminLike>;

#[async_trait::async_trait]
pub trait CheckpointManifestVerifier: Send + Sync {
    /// Verifies that the durable manifest matches `record` exactly.
    async fn validate(
        &self,
        record: &krabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError>;
}

/// The reason why the operator could not verify the durable checkpoint
/// that WAL parking needs.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointManifestError {
    /// The operator was not configured with a durable checkpoint object store.
    #[error(
        "Gres checkpoint verifier is not configured: set GRES_CHECKPOINT_STORE, GRES_CHECKPOINT_BUCKET, and provider settings"
    )]
    Unconfigured,
    /// A necessary provider-specific setting was absent.
    #[error("Gres checkpoint verifier configuration is invalid: {0}")]
    InvalidConfiguration(String),
    /// The operator could not construct the object store client.
    #[error("Gres checkpoint object store configuration: {0}")]
    ObjectStoreConfiguration(#[from] krabka_object_store::ObjectStoreError),
    /// The referenced checkpoint is absent, is corrupt, or does not match
    /// its registry record.
    #[error("Gres checkpoint manifest verification failed: {0}")]
    Verification(String),
}

#[derive(Debug)]
struct UnavailableCheckpointManifestVerifier {
    reason: String,
    unconfigured: bool,
}

#[async_trait::async_trait]
impl CheckpointManifestVerifier for UnavailableCheckpointManifestVerifier {
    async fn validate(
        &self,
        _record: &krabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError> {
        if self.unconfigured {
            return Err(CheckpointManifestError::Unconfigured);
        }
        Err(CheckpointManifestError::InvalidConfiguration(
            self.reason.clone(),
        ))
    }
}

struct ObjectStoreCheckpointManifestVerifier {
    store: Arc<dyn object_store::ObjectStore>,
}

impl ObjectStoreCheckpointManifestVerifier {
    fn from_config(config: &OperatorConfig) -> Result<Self, CheckpointManifestError> {
        let Some(kind) = config.gres_checkpoint_store else {
            return Err(CheckpointManifestError::Unconfigured);
        };
        let bucket = required_config(
            config.gres_checkpoint_bucket.as_ref(),
            "GRES_CHECKPOINT_BUCKET",
        )?;
        let store_config = match kind {
            GresCheckpointStoreKind::S3 => {
                let (access_key_id, secret_access_key) = s3_credentials(config)?;
                ObjectStoreConfig::S3(S3Config {
                    bucket,
                    region: required_config(
                        config.gres_checkpoint_region.as_ref(),
                        "GRES_CHECKPOINT_REGION",
                    )?,
                    endpoint: config.gres_checkpoint_endpoint.clone(),
                    access_key_id,
                    secret_access_key,
                    allow_http: config.gres_checkpoint_allow_http,
                    ..Default::default()
                })
            }
            GresCheckpointStoreKind::Gcs => {
                let (service_account_path, application_credentials_path) = gcs_credentials(config)?;
                ObjectStoreConfig::Gcs(GcsConfig {
                    bucket,
                    service_account_path,
                    application_credentials_path,
                    endpoint: config.gres_checkpoint_endpoint.clone(),
                    allow_http: config.gres_checkpoint_allow_http,
                    ..Default::default()
                })
            }
        };
        Ok(Self {
            store: build_object_store(&store_config)?,
        })
    }
}

fn optional_config(value: Option<&String>) -> Option<String> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn s3_credentials(
    config: &OperatorConfig,
) -> Result<(Option<String>, Option<String>), CheckpointManifestError> {
    let access_key_id = optional_config(config.gres_checkpoint_access_key_id.as_ref());
    let secret_access_key = optional_config(config.gres_checkpoint_secret_access_key.as_ref());
    if access_key_id.is_some() != secret_access_key.is_some() {
        return Err(CheckpointManifestError::InvalidConfiguration(
            "GRES_CHECKPOINT_ACCESS_KEY_ID and GRES_CHECKPOINT_SECRET_ACCESS_KEY must be set together"
                .into(),
        ));
    }
    Ok((access_key_id, secret_access_key))
}

fn gcs_credentials(
    config: &OperatorConfig,
) -> Result<(Option<String>, Option<String>), CheckpointManifestError> {
    let service_account_path =
        optional_config(config.gres_checkpoint_gcs_service_account_path.as_ref());
    let application_credentials_path = optional_config(
        config
            .gres_checkpoint_gcs_application_credentials_path
            .as_ref(),
    );
    if service_account_path.is_some() && application_credentials_path.is_some() {
        return Err(CheckpointManifestError::InvalidConfiguration(
            "GRES_CHECKPOINT_GCS_SERVICE_ACCOUNT_PATH conflicts with GRES_CHECKPOINT_GCS_APPLICATION_CREDENTIALS_PATH"
                .into(),
        ));
    }
    Ok((service_account_path, application_credentials_path))
}

fn required_config(value: Option<&String>, name: &str) -> Result<String, CheckpointManifestError> {
    let Some(value) = value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(CheckpointManifestError::InvalidConfiguration(format!(
            "{name} is required"
        )));
    };
    Ok(value.to_owned())
}

#[async_trait::async_trait]
impl CheckpointManifestVerifier for ObjectStoreCheckpointManifestVerifier {
    async fn validate(
        &self,
        record: &krabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError> {
        let checkpoint = record.final_checkpoint.as_ref().ok_or_else(|| {
            CheckpointManifestError::Verification("registry record has no final checkpoint".into())
        })?;
        let manifest_path = Path::from(checkpoint.manifest_key.as_str());
        let manifest_bytes = read_capped(&self.store, &manifest_path, checkpoint.total_bytes)
            .await
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        let manifest = Manifest::decode(&manifest_bytes)
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        let expected_tenant = checkpoint_manifest_tenant(record);
        if manifest.tenant != expected_tenant
            || manifest.wal_generation != checkpoint.wal_generation
            || manifest.covered_offset != checkpoint.covered_offset
        {
            return Err(CheckpointManifestError::Verification(
                "manifest tenant, WAL generation, or covered offset does not match the registry checkpoint".into(),
            ));
        }

        let mut parts = BTreeMap::new();
        let mut actual_bytes = u64::try_from(manifest_bytes.len()).map_err(|_| {
            CheckpointManifestError::Verification("manifest byte length overflow".into())
        })?;
        for part in &manifest.parts {
            let part_bytes = read_capped(
                &self.store,
                &Path::from(part.name.as_str()),
                part.encoded_bytes,
            )
            .await
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
            actual_bytes = actual_bytes
                .checked_add(u64::try_from(part_bytes.len()).map_err(|_| {
                    CheckpointManifestError::Verification(
                        "checkpoint part byte length overflow".into(),
                    )
                })?)
                .ok_or_else(|| {
                    CheckpointManifestError::Verification("checkpoint byte size overflow".into())
                })?;
            parts.insert(part.name.clone(), part_bytes.to_vec());
        }
        if actual_bytes != checkpoint.total_bytes {
            return Err(CheckpointManifestError::Verification(
                "checkpoint byte total does not match the registry checkpoint".into(),
            ));
        }
        manifest
            .validate(&ManifestValidation {
                tenant: &expected_tenant,
                wal_generation: checkpoint.wal_generation,
                log_start: None,
                parts_by_name: &parts,
            })
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        Ok(())
    }
}

fn checkpoint_manifest_tenant(record: &krabka_gres_control::TenantRecord) -> String {
    match record.ranges.as_slice() {
        [range] => format!("{}/r{}", record.name, range.range_id),
        _ => record.name.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgdogExpectedRoute {
    pub database: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgdogReloadRequest {
    /// DNS name for the `PostgreSQL` host identity and for TLS
    /// verification.
    pub host: String,
    /// Optional TCP destination for one replica. `host` stays the SNI
    /// name.
    pub connect_addr: Option<std::net::IpAddr>,
    pub port: u16,
    pub password: String,
    pub expected_routes: Vec<PgdogExpectedRoute>,
    pub maintenance_mode: bool,
    pub tls_ca_pem: Option<Vec<u8>>,
    pub tls_client_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, thiserror::Error)]
pub enum PgdogAdminError {
    #[error("pgdog admin connection: {0}")]
    Connect(#[from] tokio_postgres::Error),
    #[error("pgdog admin TLS: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("pgdog fleet admin: {0}")]
    Fleet(String),
}

#[async_trait::async_trait]
pub trait PgdogAdminLike: Send + Sync {
    async fn reload_and_database_views_match(
        &self,
        requests: &[PgdogReloadRequest],
    ) -> Result<bool, PgdogAdminError>;
}

#[derive(Debug, Default)]
struct TokioPostgresPgdogAdmin;

#[async_trait::async_trait]
impl PgdogAdminLike for TokioPostgresPgdogAdmin {
    async fn reload_and_database_views_match(
        &self,
        requests: &[PgdogReloadRequest],
    ) -> Result<bool, PgdogAdminError> {
        if requests.is_empty() {
            return Err(PgdogAdminError::Fleet(
                "reload request must address at least one PgDog replica".into(),
            ));
        }
        let mut clients = Vec::with_capacity(requests.len());
        for request in requests {
            let mut config = tokio_postgres::Config::new();
            config
                .host(&request.host)
                .port(request.port)
                .user("admin")
                .password(&request.password)
                .dbname("admin");
            if let Some(connect_addr) = request.connect_addr {
                config.hostaddr(connect_addr);
            }
            clients.push(
                connect_pgdog_admin(
                    config,
                    request.tls_ca_pem.as_deref(),
                    request
                        .tls_client_identity_pem
                        .as_ref()
                        .map(|(cert, key)| (cert.as_slice(), key.as_slice())),
                )
                .await?,
            );
        }
        let connections = clients
            .iter()
            .map(|client| client as &dyn PgdogAdminConnectionLike)
            .collect::<Vec<_>>();
        reload_and_match_connections(&connections, requests).await
    }
}

#[async_trait::async_trait]
trait PgdogAdminConnectionLike: Send + Sync {
    async fn execute(&self, command: &str) -> Result<(), PgdogAdminError>;
    async fn routes(
        &self,
    ) -> Result<std::collections::BTreeSet<PgdogExpectedRoute>, PgdogAdminError>;
}

#[async_trait::async_trait]
impl PgdogAdminConnectionLike for tokio_postgres::Client {
    async fn execute(&self, command: &str) -> Result<(), PgdogAdminError> {
        self.simple_query(command).await?;
        Ok(())
    }

    async fn routes(
        &self,
    ) -> Result<std::collections::BTreeSet<PgdogExpectedRoute>, PgdogAdminError> {
        // PgDog 0.1.47 exposes effective routes through SHOW POOLS. Columns
        // 1, 3, and 4 are database, configured address, and port. This view
        // contains configured database pools, not the admin pseudo-database.
        let messages = self.simple_query("SHOW POOLS").await?;
        let mut routes = std::collections::BTreeSet::new();
        for (index, row) in messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .enumerate()
        {
            let field = |column: usize, name: &str| {
                row.get(column).ok_or_else(|| {
                    PgdogAdminError::Fleet(format!(
                        "SHOW POOLS row {index} is missing {name} column {column}"
                    ))
                })
            };
            let database = field(1, "database")?.to_string();
            let host = field(3, "address")?.to_string();
            let port_text = field(4, "port")?;
            let port = port_text.parse::<i32>().map_err(|error| {
                PgdogAdminError::Fleet(format!(
                    "SHOW POOLS row {index} has invalid port {port_text}: {error}"
                ))
            })?;
            routes.insert(pgdog_route_from_fields(index, database, host, port)?);
        }
        Ok(routes)
    }
}

fn pgdog_route_from_fields(
    row_index: usize,
    database: String,
    host: String,
    port: i32,
) -> Result<PgdogExpectedRoute, PgdogAdminError> {
    let port = u16::try_from(port).map_err(|error| {
        PgdogAdminError::Fleet(format!(
            "SHOW POOLS row {row_index} has invalid port {port}: {error}"
        ))
    })?;
    if database.is_empty() || host.is_empty() {
        return Err(PgdogAdminError::Fleet(format!(
            "SHOW POOLS row {row_index} has an empty database or address"
        )));
    }
    Ok(PgdogExpectedRoute {
        database,
        host,
        port,
    })
}

async fn reload_and_match_connections(
    clients: &[&dyn PgdogAdminConnectionLike],
    requests: &[PgdogReloadRequest],
) -> Result<bool, PgdogAdminError> {
    let maintenance = requests.iter().any(|request| request.maintenance_mode);
    let mut maintenance_clients = 0;
    if maintenance {
        for client in clients {
            if let Err(error) = client.execute("MAINTENANCE ON").await {
                let mut cleanup_errors = Vec::new();
                // The failing command may have reached PgDog before its
                // response failed. OFF is idempotent, so clean every
                // connected replica, including this and later clients.
                for entered in clients {
                    if let Err(cleanup_error) = entered.execute("MAINTENANCE OFF").await {
                        cleanup_errors.push(cleanup_error.to_string());
                    }
                }
                if !cleanup_errors.is_empty() {
                    return Err(PgdogAdminError::Fleet(format!(
                        "{error}; maintenance rollback failed: {}",
                        cleanup_errors.join("; ")
                    )));
                }
                return Err(error);
            }
            maintenance_clients += 1;
        }
    }
    let operation = reload_and_match_all(clients, requests).await;
    let mut cleanup_errors = Vec::new();
    if maintenance {
        for client in &clients[..maintenance_clients] {
            if let Err(error) = client.execute("MAINTENANCE OFF").await {
                cleanup_errors.push(error.to_string());
            }
        }
    }
    match (operation, cleanup_errors.is_empty()) {
        (Ok(matches), true) => Ok(matches),
        (Ok(_), false) => Err(PgdogAdminError::Fleet(format!(
            "maintenance cleanup failed: {}",
            cleanup_errors.join("; ")
        ))),
        (Err(operation), true) => Err(operation),
        (Err(operation), false) => Err(PgdogAdminError::Fleet(format!(
            "{operation}; maintenance cleanup failed: {}",
            cleanup_errors.join("; ")
        ))),
    }
}

async fn reload_and_match_all(
    clients: &[&dyn PgdogAdminConnectionLike],
    requests: &[PgdogReloadRequest],
) -> Result<bool, PgdogAdminError> {
    for (client, request) in clients.iter().zip(requests) {
        client.execute("RELOAD").await?;
        let observed = client.routes().await?;
        if !route_view_matches(&request.expected_routes, &observed) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn route_view_matches(
    expected: &[PgdogExpectedRoute],
    observed: &std::collections::BTreeSet<PgdogExpectedRoute>,
) -> bool {
    expected
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        == *observed
}

#[cfg(test)]
mod pgdog_reload_tests {
    use std::{collections::BTreeSet, sync::Mutex};

    use assert2::assert;

    use super::{
        PgdogAdminConnectionLike, PgdogAdminError, PgdogExpectedRoute, PgdogReloadRequest,
        pgdog_route_from_fields, reload_and_match_connections, route_view_matches,
    };

    struct FakeConnection {
        fail_execute: Option<&'static str>,
        fail_routes: bool,
        calls: Mutex<Vec<String>>,
        routes: BTreeSet<PgdogExpectedRoute>,
    }

    impl FakeConnection {
        fn new(fail_execute: Option<&'static str>, fail_routes: bool) -> Self {
            Self {
                fail_execute,
                fail_routes,
                calls: Mutex::new(Vec::new()),
                routes: BTreeSet::from([expected_route()]),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl PgdogAdminConnectionLike for FakeConnection {
        async fn execute(&self, command: &str) -> Result<(), PgdogAdminError> {
            self.calls.lock().unwrap().push(command.into());
            if self.fail_execute == Some(command) {
                return Err(PgdogAdminError::Fleet(format!("{command} failed")));
            }
            Ok(())
        }

        async fn routes(&self) -> Result<BTreeSet<PgdogExpectedRoute>, PgdogAdminError> {
            self.calls.lock().unwrap().push("SHOW POOLS".into());
            if self.fail_routes {
                return Err(PgdogAdminError::Fleet("SHOW POOLS failed".into()));
            }
            Ok(self.routes.clone())
        }
    }

    fn expected_route() -> PgdogExpectedRoute {
        PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }
    }

    #[test]
    fn malformed_show_pools_route_is_rejected_instead_of_discarded() {
        assert!(pgdog_route_from_fields(7, "tenant-a".into(), "host".into(), -1).is_err());
        assert!(pgdog_route_from_fields(8, String::new(), "host".into(), 5_432).is_err());
    }

    fn requests() -> Vec<PgdogReloadRequest> {
        ["10.0.0.10", "10.0.0.11"]
            .into_iter()
            .map(|ip| PgdogReloadRequest {
                host: "fleet-pgdog.ns.svc.cluster.local".into(),
                connect_addr: Some(ip.parse().unwrap()),
                port: 6_432,
                password: "pw".into(),
                expected_routes: vec![expected_route()],
                maintenance_mode: true,
                tls_ca_pem: Some(b"ca".to_vec()),
                tls_client_identity_pem: Some((b"cert".to_vec(), b"key".to_vec())),
            })
            .collect()
    }

    #[tokio::test]
    async fn maintenance_on_failure_rolls_back_every_connected_replica() {
        let first = FakeConnection::new(None, false);
        let second = FakeConnection::new(Some("MAINTENANCE ON"), false);

        let error = reload_and_match_connections(&[&first, &second], &requests())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("MAINTENANCE ON failed"));
        assert!(first.calls().contains(&"MAINTENANCE OFF".into()));
        assert!(second.calls().contains(&"MAINTENANCE OFF".into()));
    }

    #[tokio::test]
    async fn reload_and_show_failures_still_exit_maintenance_on_every_replica() {
        for (fail_execute, fail_routes) in [(Some("RELOAD"), false), (None, true)] {
            let first = FakeConnection::new(fail_execute, fail_routes);
            let second = FakeConnection::new(None, false);

            assert!(
                reload_and_match_connections(&[&first, &second], &requests())
                    .await
                    .is_err()
            );
            assert!(first.calls().contains(&"MAINTENANCE OFF".into()));
            assert!(second.calls().contains(&"MAINTENANCE OFF".into()));
        }
    }

    #[tokio::test]
    async fn reload_never_reconnects_other_tenant_pools() {
        let first = FakeConnection::new(None, false);
        let second = FakeConnection::new(None, false);

        assert!(
            reload_and_match_connections(&[&first, &second], &requests())
                .await
                .unwrap()
        );
        for calls in [first.calls(), second.calls()] {
            assert!(calls == ["MAINTENANCE ON", "RELOAD", "SHOW POOLS", "MAINTENANCE OFF"]);
            assert!(!calls.iter().any(|command| command == "RECONNECT"));
        }
    }

    #[tokio::test]
    async fn operation_and_maintenance_off_errors_are_both_preserved() {
        let first = FakeConnection::new(Some("RELOAD"), false);
        let second = FakeConnection::new(Some("MAINTENANCE OFF"), false);

        let error = reload_and_match_connections(&[&first, &second], &requests())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("RELOAD failed"));
        assert!(error.contains("MAINTENANCE OFF failed"));
    }

    #[test]
    fn route_confirmation_rejects_same_database_on_wrong_endpoint() {
        let expected = vec![PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }];
        let observed = BTreeSet::from([PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "stale-tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }]);

        assert!(!route_view_matches(&expected, &observed));
    }

    #[test]
    fn route_confirmation_rejects_stale_extra_database() {
        let expected_route = PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        };
        let stale_route = PgdogExpectedRoute {
            database: "deleted-tenant".into(),
            host: "deleted-tenant-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        };
        let observed = BTreeSet::from([expected_route.clone(), stale_route]);

        assert!(!route_view_matches(&[expected_route], &observed));
    }
}

async fn connect_pgdog_admin(
    config: tokio_postgres::Config,
    tls_ca_pem: Option<&[u8]>,
    tls_client_identity_pem: Option<(&[u8], &[u8])>,
) -> Result<tokio_postgres::Client, PgdogAdminError> {
    if let Some(tls_ca_pem) = tls_ca_pem {
        let certificate = native_tls::Certificate::from_pem(tls_ca_pem)?;
        let mut builder = native_tls::TlsConnector::builder();
        builder.add_root_certificate(certificate);
        if let Some((certificate, private_key)) = tls_client_identity_pem {
            builder.identity(native_tls::Identity::from_pkcs8(certificate, private_key)?);
        }
        let connector = postgres_native_tls::MakeTlsConnector::new(builder.build()?);
        let (client, connection) = config.connect(connector).await?;
        drop(tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "pgdog TLS admin connection task ended");
            }
        }));
        return Ok(client);
    }
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    drop(tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "pgdog plaintext admin connection task ended");
        }
    }));
    Ok(client)
}

#[derive(Debug, thiserror::Error)]
pub enum GresControlWriteError {
    #[error("control record: {0}")]
    Control(#[from] krabka_gres_control::ControlError),
    #[error("producer: {0}")]
    Producer(#[from] krabka_client_producer::ProducerError),
    #[error("producer completion channel closed: {0}")]
    Completion(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("durable checkpoint manifest: {0}")]
    CheckpointManifest(#[from] CheckpointManifestError),
}

#[async_trait::async_trait]
pub trait GresControlLike: Send + Sync {
    async fn get_tenant(
        &self,
        tenant: &krabka_gres_control::TenantName,
    ) -> Result<Option<krabka_gres_control::TenantRecord>, GresControlWriteError>;
    /// Creates a record, or replaces the exact version that the
    /// reconciler read.
    async fn replace_tenant_if_version(
        &self,
        record: &krabka_gres_control::TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<krabka_gres_control::TenantRecord, GresControlWriteError>;
    async fn delete_tenant(
        &self,
        tenant: &krabka_gres_control::TenantName,
    ) -> Result<(), GresControlWriteError>;
    /// Gets and validates the durable final checkpoint manifest that
    /// `record` refers to.
    ///
    /// An implementation must verify the manifest identity and the
    /// checkpoint metadata. The registry metadata alone is not proof that
    /// the checkpoint is durable.
    async fn validate_final_checkpoint_manifest(
        &self,
        record: &krabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError>;
    async fn list_split_operations(
        &self,
        _tenant: &krabka_gres_control::TenantName,
    ) -> Result<Vec<krabka_gres_control::SplitOperationRecord>, GresControlWriteError> {
        Ok(Vec::new())
    }
    async fn compare_and_swap_split_operation(
        &self,
        _expected_revision: u64,
        _operation: &krabka_gres_control::SplitOperationRecord,
    ) -> Result<krabka_gres_control::SplitOperationRecord, GresControlWriteError> {
        Err(
            krabka_gres_control::ControlError::UnsupportedRegistryMutation {
                mutation: "compare_and_swap_split_operation",
                reason: "control backend does not expose the split journal",
            }
            .into(),
        )
    }
}

struct KafkaGresControl {
    registry: Mutex<krabka_gres_control::Registry>,
    checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
}

#[derive(Clone)]
struct CachedGresControl {
    bootstrap: String,
    policy: krabka_gres_control::RegistryPolicy,
    credentials: Option<KafkaCredentials>,
    control: GresControlHandle,
}

/// The SCRAM-SHA-512 credentials that the operator uses on a SASL Kafka
/// listener, read from `Gres.spec.kafka.credentialsSecretRef`.
#[derive(Clone, PartialEq, Eq)]
pub struct KafkaCredentials {
    username: String,
    password: String,
}

impl KafkaCredentials {
    /// Credentials for `username` and `password`.
    #[must_use]
    pub const fn new(username: String, password: String) -> Self {
        Self { username, password }
    }

    /// The client security of a `SASL_PLAINTEXT` connection with these
    /// credentials.
    #[must_use]
    pub fn security(&self) -> krabka_client_core::security::ClientSecurity {
        krabka_gres_control::scram_sha512_security(self.username.clone(), self.password.clone())
    }

    /// A cache-key component that changes when either credential changes and
    /// does not reveal the password.
    fn fingerprint(&self) -> String {
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(self.password.as_bytes());
        format!("{}\0{}", self.username, hex::encode(digest))
    }
}

impl std::fmt::Debug for KafkaCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaCredentials")
            .field("username", &self.username)
            .field("password", &"[hidden]")
            .finish()
    }
}

#[async_trait::async_trait]
impl GresControlLike for KafkaGresControl {
    async fn get_tenant(
        &self,
        tenant: &krabka_gres_control::TenantName,
    ) -> Result<Option<krabka_gres_control::TenantRecord>, GresControlWriteError> {
        Ok(self.registry.lock().await.get(tenant.as_str()).await?)
    }

    async fn replace_tenant_if_version(
        &self,
        record: &krabka_gres_control::TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<krabka_gres_control::TenantRecord, GresControlWriteError> {
        let mut registry = self.registry.lock().await;
        registry.ensure_topic().await?;
        let stored_record = registry
            .replace_if_version(record, expected_record_version)
            .await?;
        registry
            .upsert_tenant_config(&stored_record, stored_record.wal_replication)
            .await?;
        Ok(stored_record)
    }

    async fn delete_tenant(
        &self,
        tenant: &krabka_gres_control::TenantName,
    ) -> Result<(), GresControlWriteError> {
        self.registry.lock().await.delete(tenant.as_str()).await?;
        Ok(())
    }

    async fn validate_final_checkpoint_manifest(
        &self,
        record: &krabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError> {
        self.checkpoint_manifest_verifier
            .validate(record)
            .await
            .map_err(GresControlWriteError::CheckpointManifest)
    }

    async fn list_split_operations(
        &self,
        tenant: &krabka_gres_control::TenantName,
    ) -> Result<Vec<krabka_gres_control::SplitOperationRecord>, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .list_split_operations(tenant.as_str())
            .await?)
    }

    async fn compare_and_swap_split_operation(
        &self,
        expected_revision: u64,
        operation: &krabka_gres_control::SplitOperationRecord,
    ) -> Result<krabka_gres_control::SplitOperationRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .compare_and_swap_split_operation(Some(expected_revision), operation)
            .await?)
    }
}

/// Shared context for each reconciler.
///
/// A clone is cheap. Every field is an `Arc` or is shared with interior
/// mutability.
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Controller metrics for the whole operator: the reconcile counters,
    /// histograms, and gauges. A clone is cheap. The handles are
    /// registered against `registry`.
    pub metrics: ControllerMetrics,
    /// Per-fleet-and-endpoint admin-client cache.
    /// The cache replaces a broken connection at the next use.
    pub admin_clients: Arc<Mutex<HashMap<String, AdminClientHandle>>>,
    gres_controls: Arc<Mutex<HashMap<(String, String), CachedGresControl>>>,
    pub checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
    pub pgdog_admin: PgdogAdminHandle,
}

impl Context {
    #[must_use]
    pub fn new(
        client: Client,
        config: OperatorConfig,
        registry: SharedRegistry,
        metrics: ControllerMetrics,
    ) -> Self {
        let config = Arc::new(config);
        Self {
            client,
            checkpoint_manifest_verifier: checkpoint_manifest_verifier(&config),
            config,
            registry,
            metrics,
            admin_clients: Arc::new(Mutex::new(HashMap::new())),
            gres_controls: Arc::new(Mutex::new(HashMap::new())),
            pgdog_admin: Arc::new(TokioPostgresPgdogAdmin),
        }
    }

    #[must_use]
    pub fn with_pgdog_admin_for_test(mut self, pgdog_admin: PgdogAdminHandle) -> Self {
        self.pgdog_admin = pgdog_admin;
        self
    }

    #[must_use]
    pub fn with_checkpoint_manifest_verifier_for_test(
        mut self,
        checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
    ) -> Self {
        self.checkpoint_manifest_verifier = checkpoint_manifest_verifier;
        self
    }

    /// Looks up an `AdminClient` for the named `Gres` fleet, or opens one.
    ///
    /// `bootstrap` is `Gres.spec.kafka.bootstrapServers`, for example
    /// `demo-kafka-bootstrap.default.svc.cluster.local:9092`.
    ///
    /// # Errors
    ///
    /// Returns an error when the admin client cannot connect to `bootstrap`.
    pub async fn admin_client_for(
        &self,
        fleet: &str,
        bootstrap: &str,
        credentials: Option<&KafkaCredentials>,
    ) -> Result<AdminClientHandle, krabka_client_admin::AdminError> {
        let mut map = self.admin_clients.lock().await;
        let key = format!(
            "{fleet}\0{bootstrap}\0{}",
            credentials
                .map(KafkaCredentials::fingerprint)
                .unwrap_or_default()
        );
        if let Some(client) = map.get(&key).or_else(|| map.get(fleet)) {
            return Ok(client.clone());
        }
        let admin = AdminClient::connect_with_options(
            &bootstrap_addrs(bootstrap),
            krabka_client_core::ConnectionOptions {
                dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
                    self.config.client_dispatch_queue_capacity,
                )
                .map_err(krabka_client_admin::AdminError::Protocol)?,
                frame_max: krabka_client_core::ClientFrameMax::try_from(
                    self.config.client_frame_max,
                )
                .map_err(krabka_client_admin::AdminError::Protocol)?,
                security: credentials.map(|credentials| Box::new(credentials.security())),
                ..krabka_client_core::ConnectionOptions::default()
            },
        )
        .await?;
        let entry: AdminClientHandle = Arc::new(Mutex::new(admin));
        map.insert(key, entry.clone());
        Ok(entry)
    }

    /// Drops the cached admin client for `fleet`.
    ///
    /// Reconcile calls this when a Transport error shows that the
    /// connection died. The next call opens a new connection.
    pub async fn drop_admin_client(&self, fleet: &str) {
        let mut clients = self.admin_clients.lock().await;
        clients.retain(|key, _| key != fleet && !key.starts_with(&format!("{fleet}\0")));
    }

    /// Fills the admin-client cache with a handle from the caller. This is
    /// for tests only.
    ///
    /// The `AdminClientLike` trait covers both the real client and the
    /// fakes of each test, so reconcile tests can call the trait methods
    /// and open no TCP connection.
    ///
    /// There is no `cfg` gate on this function. It stays in the public
    /// API. In production it does no damage and nothing calls it. Without
    /// the gate, the build needs no parallel test-only profile.
    pub async fn insert_admin_client_for_test(&self, fleet: &str, admin: AdminClientHandle) {
        self.admin_clients
            .lock()
            .await
            .insert(fleet.to_string(), admin);
    }

    /// Looks up the Gres control-plane handle of a fleet, or connects one.
    ///
    /// The cache is keyed by namespace and `Gres` name. A changed bootstrap
    /// list, registry policy, or credential replaces the cached handle.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot connect or cannot ensure its
    /// topic.
    pub async fn gres_control_for(
        &self,
        namespace: &str,
        fleet: &str,
        bootstrap: &str,
        policy: &krabka_gres_control::RegistryPolicy,
        credentials: Option<&KafkaCredentials>,
    ) -> Result<GresControlHandle, GresControlWriteError> {
        let bootstrap_owned = bootstrap.to_owned();
        let policy_owned = policy.clone();
        let security = credentials.map(KafkaCredentials::security);
        let checkpoint_manifest_verifier = Arc::clone(&self.checkpoint_manifest_verifier);
        let target = ControlTarget {
            bootstrap,
            policy,
            credentials,
        };
        self.gres_control_for_with(namespace, fleet, &target, async move {
            let mut registry = Box::pin(krabka_gres_control::Registry::connect_with_policy(
                &bootstrap_owned,
                policy_owned,
                security,
            ))
            .await?;
            Box::pin(registry.ensure_topic()).await?;
            Ok(Arc::new(KafkaGresControl {
                registry: Mutex::new(registry),
                checkpoint_manifest_verifier,
            }) as GresControlHandle)
        })
        .await
    }

    async fn gres_control_for_with<F>(
        &self,
        namespace: &str,
        fleet: &str,
        target: &ControlTarget<'_>,
        build: F,
    ) -> Result<GresControlHandle, GresControlWriteError>
    where
        F: Future<Output = Result<GresControlHandle, GresControlWriteError>>,
    {
        let key = (namespace.to_owned(), fleet.to_owned());
        if let Some(entry) = self.gres_controls.lock().await.get(&key)
            && target.matches(entry)
        {
            return Ok(Arc::clone(&entry.control));
        }

        let control = build.await?;
        let mut map = self.gres_controls.lock().await;
        if let Some(entry) = map.get(&key)
            && target.matches(entry)
        {
            return Ok(Arc::clone(&entry.control));
        }
        map.insert(
            key,
            CachedGresControl {
                bootstrap: target.bootstrap.to_owned(),
                policy: target.policy.clone(),
                credentials: target.credentials.cloned(),
                control: Arc::clone(&control),
            },
        );
        Ok(control)
    }

    /// Fills the Gres control cache of `fleet` for `bootstrap` and the
    /// default registry policy. This is for tests only.
    pub async fn insert_gres_control_for_test(
        &self,
        namespace: &str,
        fleet: &str,
        bootstrap: &str,
        control: GresControlHandle,
    ) {
        self.insert_gres_control_for_test_with_policy(
            namespace,
            fleet,
            bootstrap,
            krabka_gres_control::RegistryPolicy::default(),
            control,
        )
        .await;
    }

    /// Fills the Gres control cache of `fleet` for `bootstrap` and
    /// `policy`. This is for tests only.
    pub async fn insert_gres_control_for_test_with_policy(
        &self,
        namespace: &str,
        fleet: &str,
        bootstrap: &str,
        policy: krabka_gres_control::RegistryPolicy,
        control: GresControlHandle,
    ) {
        self.gres_controls.lock().await.insert(
            (namespace.to_owned(), fleet.to_owned()),
            CachedGresControl {
                bootstrap: bootstrap.to_owned(),
                policy,
                credentials: None,
                control,
            },
        );
    }
}

/// What a cached Gres control handle must have connected with.
struct ControlTarget<'a> {
    bootstrap: &'a str,
    policy: &'a krabka_gres_control::RegistryPolicy,
    credentials: Option<&'a KafkaCredentials>,
}

impl ControlTarget<'_> {
    fn matches(&self, entry: &CachedGresControl) -> bool {
        entry.bootstrap == self.bootstrap
            && entry.policy == *self.policy
            && entry.credentials.as_ref() == self.credentials
    }
}

fn checkpoint_manifest_verifier(config: &OperatorConfig) -> CheckpointManifestVerifierHandle {
    match ObjectStoreCheckpointManifestVerifier::from_config(config) {
        Ok(verifier) => Arc::new(verifier),
        Err(error) => {
            tracing::error!(error = %error, "Gres tenant WAL parking is unavailable until durable checkpoint verification is configured");
            Arc::new(UnavailableCheckpointManifestVerifier {
                reason: error.to_string(),
                unconfigured: matches!(error, CheckpointManifestError::Unconfigured),
            })
        }
    }
}

/// Splits a comma-separated `bootstrapServers` value into its trimmed,
/// non-empty `host:port` entries.
fn bootstrap_addrs(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;
    use tower::service_fn;

    use super::*;

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    #[test]
    fn bootstrap_addrs_splits_a_comma_separated_list() {
        assert!(
            bootstrap_addrs(" broker-a:9092, broker-b:9092 ,,")
                == ["broker-a:9092", "broker-b:9092"]
        );
        assert!(bootstrap_addrs("broker-a:9092") == ["broker-a:9092"]);
    }

    #[derive(Parser)]
    struct ConfigArgs {
        #[command(flatten)]
        config: OperatorConfig,
    }

    struct TestGresControl;

    #[async_trait::async_trait]
    impl GresControlLike for TestGresControl {
        async fn get_tenant(
            &self,
            _tenant: &krabka_gres_control::TenantName,
        ) -> Result<Option<krabka_gres_control::TenantRecord>, GresControlWriteError> {
            unreachable!("cache test does not read tenants")
        }

        async fn replace_tenant_if_version(
            &self,
            _record: &krabka_gres_control::TenantRecord,
            _expected_record_version: Option<u64>,
        ) -> Result<krabka_gres_control::TenantRecord, GresControlWriteError> {
            unreachable!("cache test does not write tenants")
        }

        async fn delete_tenant(
            &self,
            _tenant: &krabka_gres_control::TenantName,
        ) -> Result<(), GresControlWriteError> {
            unreachable!("cache test does not delete tenants")
        }

        async fn validate_final_checkpoint_manifest(
            &self,
            _record: &krabka_gres_control::TenantRecord,
        ) -> Result<(), GresControlWriteError> {
            unreachable!("cache test does not verify checkpoints")
        }
    }

    fn test_context() -> Context {
        let client = Client::new(
            service_fn(|_| async {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(500)
                        .body(kube::client::Body::from(Vec::new()))
                        .expect("response"),
                )
            }),
            "default",
        );
        let (registry, metrics) = crate::telemetry::new_registry_with_metrics();
        Context::new(
            client,
            ConfigArgs::parse_from(["operator"]).config,
            Arc::new(Mutex::new(registry)),
            metrics,
        )
    }

    #[tokio::test]
    async fn gres_control_cache_tracks_inputs_without_locking_during_build() {
        let ctx = test_context();
        let defaults = krabka_gres_control::RegistryPolicy::default();
        let first: GresControlHandle = Arc::new(TestGresControl);
        let observed = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "a:9092",
                    policy: &defaults,
                    credentials: None,
                },
                async {
                    assert!(ctx.gres_controls.try_lock().is_ok());
                    Ok(Arc::clone(&first))
                },
            )
            .await
            .expect("first control");
        assert!(Arc::ptr_eq(&observed, &first));

        let reused = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "a:9092",
                    policy: &defaults,
                    credentials: None,
                },
                async { unreachable!("equal cache inputs must not rebuild") },
            )
            .await
            .expect("reused control");
        assert!(Arc::ptr_eq(&reused, &first));

        let changed_reader_admin_dns = defaults
            .clone()
            .with_reader_admin_dns_timeout(krabka_units::millis(37))
            .expect("reader/admin DNS timeout");
        let changed_reader_admin_dns_control: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "a:9092",
                    policy: &changed_reader_admin_dns,
                    credentials: None,
                },
                async { Ok(Arc::clone(&changed_reader_admin_dns_control)) },
            )
            .await
            .expect("reader/admin DNS policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_reader_admin_dns_control));

        let changed_dns = changed_reader_admin_dns
            .clone()
            .with_producer_dns_timeout(krabka_units::millis(37))
            .expect("DNS timeout");
        let changed_dns_control: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "a:9092",
                    policy: &changed_dns,
                    credentials: None,
                },
                async { Ok(Arc::clone(&changed_dns_control)) },
            )
            .await
            .expect("DNS policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_dns_control));

        let custom = krabka_gres_control::RegistryPolicy::new(
            2,
            krabka_units::millis(15_001),
            krabka_units::millis(251),
            krabka_units::millis(501),
            krabka_units::bytes(1_048_577),
        )
        .expect("policy");
        let changed_policy: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "a:9092",
                    policy: &custom,
                    credentials: None,
                },
                async { Ok(Arc::clone(&changed_policy)) },
            )
            .await
            .expect("policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_policy));

        let changed_bootstrap: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with(
                "ns-a",
                "demo",
                &ControlTarget {
                    bootstrap: "b:9092",
                    policy: &custom,
                    credentials: None,
                },
                async { Ok(Arc::clone(&changed_bootstrap)) },
            )
            .await
            .expect("bootstrap replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_bootstrap));

        let other_namespace: GresControlHandle = Arc::new(TestGresControl);
        let isolated = ctx
            .gres_control_for_with(
                "ns-b",
                "demo",
                &ControlTarget {
                    bootstrap: "b:9092",
                    policy: &custom,
                    credentials: None,
                },
                async { Ok(Arc::clone(&other_namespace)) },
            )
            .await
            .expect("namespace-isolated control");
        assert!(Arc::ptr_eq(&isolated, &other_namespace));
        assert!(ctx.gres_controls.lock().await.len() == 2);
    }

    #[tokio::test]
    async fn gres_control_cache_reconnects_when_kafka_credentials_change() {
        let ctx = test_context();
        let policy = krabka_gres_control::RegistryPolicy::default();
        let original = KafkaCredentials::new("gres-operator".into(), "first".into());
        let rotated = KafkaCredentials::new("gres-operator".into(), "second".into());
        let target = |credentials| ControlTarget {
            bootstrap: "a:9092",
            policy: &policy,
            credentials,
        };

        let plaintext: GresControlHandle = Arc::new(TestGresControl);
        ctx.gres_control_for_with("ns", "demo", &target(None), async {
            Ok(Arc::clone(&plaintext))
        })
        .await
        .expect("plaintext control");
        let authenticated: GresControlHandle = Arc::new(TestGresControl);
        let observed = ctx
            .gres_control_for_with("ns", "demo", &target(Some(&original)), async {
                Ok(Arc::clone(&authenticated))
            })
            .await
            .expect("authenticated control");
        assert!(Arc::ptr_eq(&observed, &authenticated));
        let reused = ctx
            .gres_control_for_with("ns", "demo", &target(Some(&original)), async {
                unreachable!("equal credentials must not reconnect")
            })
            .await
            .expect("reused control");
        assert!(Arc::ptr_eq(&reused, &authenticated));
        let reconnected: GresControlHandle = Arc::new(TestGresControl);
        let observed = ctx
            .gres_control_for_with("ns", "demo", &target(Some(&rotated)), async {
                Ok(Arc::clone(&reconnected))
            })
            .await
            .expect("rotated control");
        assert!(Arc::ptr_eq(&observed, &reconnected));
    }

    #[test]
    fn kafka_credentials_hide_the_password() {
        let credentials = KafkaCredentials::new("gres-operator".into(), "hunter2".into());
        let debug = format!("{credentials:?}");
        assert!(debug.contains("gres-operator"));
        assert!(!debug.contains("hunter2"));
        assert!(!credentials.fingerprint().contains("hunter2"));
        assert!(
            credentials.fingerprint()
                != KafkaCredentials::new("gres-operator".into(), "other".into()).fingerprint()
        );
    }

    fn checkpoint_config(kind: GresCheckpointStoreKind) -> OperatorConfig {
        let mut config = ConfigArgs::parse_from(["operator"]).config;
        config.gres_checkpoint_store = Some(kind);
        config.gres_checkpoint_bucket = Some("checkpoints".into());
        config.gres_checkpoint_region = Some("us-east-1".into());
        config
    }

    #[test]
    fn single_range_checkpoint_manifest_identity_is_generation_namespace() {
        let record = krabka_gres_control::TenantRecord::new(
            1,
            krabka_gres_control::TenantId::try_from("tenant-a").unwrap(),
            krabka_gres_control::TenantName::try_from("tenant-a").unwrap(),
            krabka_gres_control::TenantState::Active,
            krabka_gres_control::SqlUser::try_from("alice").unwrap(),
            krabka_security::scram::PgScramVerifier::generate_with_salt(
                &fixture_password(),
                4096,
                vec![1; 16],
            )
            .unwrap()
            .to_string(),
            1,
        )
        .unwrap()
        .with_range_layout(vec![krabka_gres_control::RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "tenant-a-gres.default.svc:5432".into(),
            wal_generation: 0,
            lifecycle: krabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }])
        .unwrap();

        assert!(checkpoint_manifest_tenant(&record) == "tenant-a/r0");
    }

    #[tokio::test]
    async fn checkpoint_verifier_preserves_unconfigured_error_category() {
        let config = ConfigArgs::parse_from(["operator"]).config;
        let verifier = checkpoint_manifest_verifier(&config);
        let record = krabka_gres_control::TenantRecord::new(
            1,
            krabka_gres_control::TenantId::try_from("tenant-a").unwrap(),
            krabka_gres_control::TenantName::try_from("tenant-a").unwrap(),
            krabka_gres_control::TenantState::Suspended,
            krabka_gres_control::SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            1,
        )
        .unwrap();

        let result = verifier.validate(&record).await;
        assert!(matches!(result, Err(CheckpointManifestError::Unconfigured)));
    }

    #[test]
    fn checkpoint_provider_credentials_reject_ambiguous_or_partial_configuration() {
        let mut s3 = checkpoint_config(GresCheckpointStoreKind::S3);
        s3.gres_checkpoint_access_key_id = Some("access".into());
        assert!(matches!(
            ObjectStoreCheckpointManifestVerifier::from_config(&s3),
            Err(CheckpointManifestError::InvalidConfiguration(_))
        ));

        let mut gcs = checkpoint_config(GresCheckpointStoreKind::Gcs);
        gcs.gres_checkpoint_gcs_service_account_path = Some("service.json".into());
        gcs.gres_checkpoint_gcs_application_credentials_path = Some("adc.json".into());
        assert!(matches!(
            ObjectStoreCheckpointManifestVerifier::from_config(&gcs),
            Err(CheckpointManifestError::InvalidConfiguration(_))
        ));
    }
}
