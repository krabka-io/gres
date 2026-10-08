//! In-memory `AdminClientLike` for reconcile tests.
//!
//! Records every call against an internal log and serves canned
//! responses from a `HashMap<topic_name, TopicState>` the test
//! pre-populates. Mirrors enough of the JVM-broker semantics for the
//! `KafkaTopic` reconcile to exercise its happy / partition-change /
//! immutable / config-diff / delete branches without a live TCP
//! connection.

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Mutex as StdMutex,
};

use krabka_client_admin::{
    AclEntry, AclEntryFilter, AdminClientLike, AdminError, AlterConfigOp, AlterConfigOpType,
    AlterConfigsResults, Config, ConfigEntry, ConfigResource, ConfigSource, ConfigType,
    CreateAclOutcome, CreateDelegationTokenOptions, CreatePartitionsOp, CreatePartitionsOutcome,
    CreateTopicOutcome, CreateTopicSpec, DelegationToken, DeleteAclFilterOutcome, DeleteRecordsOp,
    DeleteRecordsOutcome, DeleteTopicOutcome, DescribeConfigsOptions, DescribeConfigsResults,
    DescribeDelegationTokenOptions, ExpireDelegationTokenOptions, IncrementalAlterConfigsOptions,
    KafkaError, MetadataQuorum, MetadataVersionUpdate, PartitionAssignment, QuotaOp,
    RenewDelegationTokenOptions, ScramDeletion, ScramUpsertion, ScramUserOutcome, TopicMetadata,
    TopicMetadataEntry, TopicMutationOptions, TopicReplicationStatus, UpgradeType, UserQuotaConfig,
};
use krabka_client_core::ClientError;
use krabka_security::KafkaPrincipal;
use krabka_units::{Time, convert::TimeExt as _, days, secs};

/// Per-RPC error to inject. `Broker` surfaces as a per-outcome error
/// (matches how Kafka reports per-topic errors); `Transport` surfaces as
/// `AdminError::Transport(_)` (the variant the reconcile T3-fix evicts
/// the cached admin client on); `BrokerToplevel` surfaces as
/// `AdminError::Broker { .. }` (the variant `describe_configs` returns
/// when any result carries a non-zero error code).
#[derive(Debug, Clone)]
pub enum InjectableError {
    Broker {
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    BrokerToplevel {
        api: &'static str,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    Transport,
}

#[derive(Debug, Default)]
pub struct InjectedErrors {
    pub metadata_version_update: Option<InjectableError>,
    pub create_topics: Option<InjectableError>,
    pub delete_topics: Option<InjectableError>,
    pub create_partitions: Option<InjectableError>,
    pub describe_configs: Option<InjectableError>,
    pub incremental_alter_configs: Option<InjectableError>,
    pub metadata: Option<InjectableError>,
    pub create_acls: Option<InjectableError>,
}

/// A single recorded admin call. Tests assert against the captured
/// sequence to verify which RPCs were issued (and in what order).
#[derive(Debug, Clone)]
pub enum RecordedCall {
    UpdateMetadataVersion {
        level: i16,
        safe_downgrade: bool,
        timeout: Time,
    },
    DescribeMetadataQuorum,
    RemoveRaftVoter {
        cluster_id: uuid::Uuid,
        node_id: i32,
        directory_id: uuid::Uuid,
    },
    DescribePartitionAssignments(Vec<String>),
    ListPartitionReassignments(BTreeMap<String, Vec<i32>>),
    UnregisterBroker(i32),
    Metadata(Vec<String>),
    ReconcileTopicReplicationFactor {
        topic: String,
        replication_factor: i32,
        timeout: Time,
    },
    CreateTopics(Vec<CreateTopicSpec>),
    DeleteTopics(Vec<String>),
    DeleteRecords(Vec<DeleteRecordsOp>),
    CreatePartitions(Vec<CreatePartitionsOp>),
    DescribeConfigs(Vec<String>),
    IncrementalAlterConfigs(BTreeMap<ConfigResource, Vec<AlterConfigOp>>),
    AlterUserScramCredentials {
        upsertions: Vec<ScramUpsertion>,
        deletions: Vec<ScramDeletion>,
    },
    DescribeAcls(AclEntryFilter),
    CreateAcls(Vec<AclEntry>),
    DeleteAcls(Vec<AclEntryFilter>),
    DescribeUserQuotas(String),
    AlterUserQuotas {
        username: String,
        ops: Vec<QuotaOp>,
        validate_only: bool,
    },
    // ── delegation-token RPCs ─────────────────────────────────────────
    CreateDelegationToken {
        owner_principal_name: String,
        renewers: Vec<String>,
        max_lifetime: Option<Time>,
    },
    RenewDelegationToken {
        hmac: Vec<u8>,
    },
    ExpireDelegationToken {
        hmac: Vec<u8>,
    },
    DescribeDelegationTokensOwnedBy {
        owner_principal: String,
    },
}

/// Per-topic state held by the fake. Mirrors `TopicMetadataEntry` +
/// dynamic-topic config overrides.
#[derive(Debug, Clone, Default)]
pub struct TopicState {
    pub partitions: i32,
    pub replicas: i32,
    pub topic_id: Option<uuid::Uuid>,
    pub config_overrides: BTreeMap<String, String>,
}

/// Test fake. `recorded_calls` and `topics` use `std::sync::Mutex`
/// (rather than `tokio::sync::Mutex`) because both are accessed only
/// while the fake's `async` methods hold the outer per-cluster
/// `tokio::sync::Mutex` lock — there's no contention or await across
/// these mutations.
#[derive(Default)]
pub struct FakeAdminClient {
    pub recorded_calls: StdMutex<Vec<RecordedCall>>,
    pub create_topic_timeouts: StdMutex<Vec<Time>>,
    pub delete_topic_timeouts: StdMutex<Vec<Time>>,
    pub topics: StdMutex<HashMap<String, TopicState>>,
    pub injected: StdMutex<InjectedErrors>,
    pub metadata_quorum: StdMutex<Option<MetadataQuorum>>,
    pub partition_assignments: StdMutex<Vec<PartitionAssignment>>,
    pub active_reassignments: StdMutex<Vec<PartitionAssignment>>,
    /// In-memory ACL store, keyed on the full tuple. Reconcile
    /// tests pre-seed this when verifying convergence; the trait
    /// implementations below diff against the live set.
    pub acls: StdMutex<BTreeSet<AclEntry>>,
    /// SCRAM users that have been upserted at least once. The reconcile
    /// happy-path only inspects the recorded-call log; this set lets
    /// future deletion-path tests check eviction.
    pub scram_users: StdMutex<BTreeSet<String>>,
    /// In-memory client-quota store, keyed by username. Reconcile
    /// tests seed this when verifying convergence.
    pub user_quotas: StdMutex<BTreeMap<String, UserQuotaConfig>>,
    /// In-memory delegation-token store. The fake mirrors the
    /// broker's KIP-48 semantics enough for the reconciler's
    /// Describe → decide → Create/Renew/Expire loop to exercise its
    /// branches: `create_delegation_token_as_owner` mints a fresh token
    /// keyed by a sequential id with `expiry_timestamp_ms = now + 7d`
    /// and `max_timestamp_ms = now + 30d`; `renew_delegation_token`
    /// extends `expiry_timestamp_ms` to `min(now + 7d, max_timestamp_ms)`;
    /// `expire_delegation_token` removes the matching entry.
    pub delegation_tokens: StdMutex<Vec<DelegationToken>>,
    /// Monotonic id counter for minted tokens.
    pub next_token_id: StdMutex<u64>,
    /// Makes successful `DeleteTopics` acknowledgements leave topic metadata
    /// visible, modelling Kafka's asynchronous topic deletion.
    pub retain_topics_after_delete_ack: StdMutex<bool>,
}

impl FakeAdminClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_topic(&self, name: &str, state: TopicState) {
        self.topics.lock().unwrap().insert(name.into(), state);
    }

    pub fn set_metadata_quorum(&self, quorum: MetadataQuorum) {
        *self.metadata_quorum.lock().unwrap() = Some(quorum);
    }

    pub fn set_partition_assignments(&self, assignments: Vec<PartitionAssignment>) {
        *self.partition_assignments.lock().unwrap() = assignments;
    }

    pub fn set_active_reassignments(&self, assignments: Vec<PartitionAssignment>) {
        *self.active_reassignments.lock().unwrap() = assignments;
    }

    pub fn retain_topics_after_delete_ack(&self) {
        *self.retain_topics_after_delete_ack.lock().unwrap() = true;
    }

    pub fn remove_topic(&self, name: &str) {
        self.topics.lock().unwrap().remove(name);
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.recorded_calls.lock().unwrap().clone()
    }

    pub fn create_topic_timeouts(&self) -> Vec<Time> {
        self.create_topic_timeouts.lock().unwrap().clone()
    }

    pub fn delete_topic_timeouts(&self) -> Vec<Time> {
        self.delete_topic_timeouts.lock().unwrap().clone()
    }

    pub fn inject_create_topics_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().create_topics = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_create_partitions_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().create_partitions = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_incremental_alter_configs_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().incremental_alter_configs = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_delete_topics_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().delete_topics = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    /// Inject a top-level `AdminError::Broker { .. }` for `describe_configs`.
    /// Matches the path the real `describe_configs` returns when any
    /// per-resource result carries a non-zero error code.
    pub fn inject_describe_configs_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().describe_configs = Some(InjectableError::BrokerToplevel {
            api: "DescribeConfigs",
            code,
            name,
            message,
        });
    }

    /// Inject an `AdminError::Transport(_)` on the named RPC. The reconcile
    /// loop evicts the cached admin client on this variant (the T3-fix path).
    pub fn inject_metadata_transport_error(&self) {
        self.injected.lock().unwrap().metadata = Some(InjectableError::Transport);
    }

    pub fn inject_metadata_version_update_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().metadata_version_update =
            Some(InjectableError::BrokerToplevel {
                api: "UpdateFeatures",
                code,
                name,
                message,
            });
    }

    pub fn inject_create_acls_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().create_acls = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }
}

fn transport_error() -> AdminError {
    AdminError::Transport(ClientError::Disconnected)
}

#[async_trait::async_trait]
impl AdminClientLike for FakeAdminClient {
    async fn update_metadata_version(
        &mut self,
        level: i16,
        upgrade_type: UpgradeType,
        timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::UpdateMetadataVersion {
                level,
                safe_downgrade: upgrade_type == UpgradeType::SafeDowngrade,
                timeout,
            });
        if let Some(error) = self
            .injected
            .lock()
            .unwrap()
            .metadata_version_update
            .clone()
        {
            return match error {
                InjectableError::Transport => Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                }
                | InjectableError::BrokerToplevel {
                    code,
                    name,
                    message,
                    ..
                } => Err(AdminError::Broker {
                    api: "UpdateFeatures",
                    code,
                    name,
                    message,
                }),
            };
        }
        Ok(MetadataVersionUpdate { level })
    }

    async fn describe_metadata_quorum(&mut self) -> Result<MetadataQuorum, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeMetadataQuorum);
        self.metadata_quorum
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| AdminError::Protocol("metadata quorum is not configured".into()))
    }

    async fn remove_raft_voter(
        &mut self,
        cluster_id: Option<&str>,
        node_id: i32,
        directory_id: uuid::Uuid,
    ) -> Result<(), AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::RemoveRaftVoter {
                cluster_id: cluster_id
                    .map(str::parse)
                    .transpose()
                    .map_err(|error| AdminError::Protocol(format!("invalid cluster id: {error}")))?
                    .unwrap_or_default(),
                node_id,
                directory_id,
            });
        if let Some(quorum) = self.metadata_quorum.lock().unwrap().as_mut() {
            quorum
                .voters
                .retain(|voter| voter.node_id != node_id || voter.directory_id != directory_id);
        }
        Ok(())
    }

    async fn describe_partition_assignments(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<PartitionAssignment>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribePartitionAssignments(
                topics.iter().map(|topic| (*topic).to_string()).collect(),
            ));
        Ok(self.partition_assignments.lock().unwrap().clone())
    }

    async fn list_partition_reassignments(
        &mut self,
        partitions: &BTreeMap<String, Vec<i32>>,
        _timeout: Time,
    ) -> Result<Vec<PartitionAssignment>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::ListPartitionReassignments(partitions.clone()));
        Ok(self.active_reassignments.lock().unwrap().clone())
    }

    async fn unregister_broker(&mut self, broker_id: i32) -> Result<(), AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::UnregisterBroker(broker_id));
        Ok(())
    }

    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::Metadata(
                topics.iter().map(|s| (*s).to_string()).collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().metadata.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::BrokerToplevel {
                    api,
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    });
                }
                InjectableError::Broker { .. } => {
                    // Metadata in the real client doesn't fail per-topic via
                    // top-level error; tests don't use this path today.
                }
            }
        }
        let stored = self.topics.lock().unwrap().clone();
        let entries: Vec<TopicMetadataEntry> = topics
            .iter()
            .map(|t| match stored.get(*t) {
                Some(s) => TopicMetadataEntry {
                    name: (*t).to_string(),
                    topic_id: s.topic_id,
                    partition_count: s.partitions,
                    replication_factor: s.replicas,
                    error: None,
                },
                None => TopicMetadataEntry {
                    name: (*t).to_string(),
                    topic_id: None,
                    partition_count: 0,
                    replication_factor: 0,
                    error: Some(KafkaError {
                        code: 3,
                        name: "UNKNOWN_TOPIC_OR_PARTITION",
                        message: None,
                    }),
                },
            })
            .collect();
        Ok(TopicMetadata {
            controller_id: 0,
            topics: entries,
        })
    }

    async fn reconcile_topic_replication_factor(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::ReconcileTopicReplicationFactor {
                topic: topic.to_string(),
                replication_factor,
                timeout,
            });
        let mut topics = self.topics.lock().unwrap();
        let state = topics.get_mut(topic).ok_or(AdminError::Broker {
            api: "Metadata",
            code: 3,
            name: "UNKNOWN_TOPIC_OR_PARTITION",
            message: None,
        })?;
        if state.replicas == replication_factor {
            return Ok(TopicReplicationStatus::InSync);
        }
        state.replicas = replication_factor;
        Ok(TopicReplicationStatus::ReassignmentSubmitted)
    }

    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        options: TopicMutationOptions,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        self.create_topic_timeouts
            .lock()
            .unwrap()
            .push(options.timeout.unwrap_or_else(|| secs(60)));
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateTopics(specs.to_vec()));
        if let Some(inj) = self.injected.lock().unwrap().create_topics.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(specs
                        .iter()
                        .map(|s| CreateTopicOutcome {
                            name: s.name.clone(),
                            topic_id: None,
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                            throttle_time: None,
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {
                    // Not used for per-outcome RPCs; ignore.
                }
            }
        }
        let mut store = self.topics.lock().unwrap();
        let outcomes = specs
            .iter()
            .map(|s| {
                let id = uuid::Uuid::new_v4();
                store.insert(
                    s.name.clone(),
                    TopicState {
                        partitions: s.partitions,
                        replicas: s.replicas,
                        topic_id: Some(id),
                        config_overrides: s.configs.clone(),
                    },
                );
                CreateTopicOutcome {
                    name: s.name.clone(),
                    topic_id: Some(id),
                    error: None,
                    throttle_time: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn delete_topics(
        &mut self,
        names: &[&str],
        options: TopicMutationOptions,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        self.delete_topic_timeouts
            .lock()
            .unwrap()
            .push(options.timeout.unwrap_or_else(|| secs(60)));
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteTopics(
                names.iter().map(|s| (*s).to_string()).collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().delete_topics.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(names
                        .iter()
                        .map(|n| DeleteTopicOutcome {
                            name: (*n).to_string(),
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                            throttle_time: None,
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        let mut store = self.topics.lock().unwrap();
        let retain_topics_after_delete_ack = *self.retain_topics_after_delete_ack.lock().unwrap();
        let outcomes = names
            .iter()
            .map(|n| {
                if !retain_topics_after_delete_ack {
                    store.remove(*n);
                }
                DeleteTopicOutcome {
                    name: (*n).to_string(),
                    error: None,
                    throttle_time: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        _options: TopicMutationOptions,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreatePartitions(ops.to_vec()));
        if let Some(inj) = self.injected.lock().unwrap().create_partitions.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(ops
                        .iter()
                        .map(|op| CreatePartitionsOutcome {
                            name: op.name.clone(),
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                            throttle_time: None,
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        let mut store = self.topics.lock().unwrap();
        let outcomes = ops
            .iter()
            .map(|op| {
                if let Some(s) = store.get_mut(&op.name) {
                    s.partitions = op.new_total_count;
                }
                CreatePartitionsOutcome {
                    name: op.name.clone(),
                    error: None,
                    throttle_time: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        _timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteRecords(ops.to_vec()));
        Ok(ops
            .iter()
            .map(|op| DeleteRecordsOutcome {
                topic: op.topic.clone(),
                partition: op.partition,
                error_code: 0,
                low_watermark: op.offset,
            })
            .collect())
    }

    async fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
        _options: DescribeConfigsOptions,
    ) -> Result<DescribeConfigsResults, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeConfigs(
                resources
                    .iter()
                    .map(|resource| resource.name.clone())
                    .collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().describe_configs.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::BrokerToplevel {
                    api,
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    });
                }
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api: "DescribeConfigs",
                        code,
                        name,
                        message,
                    });
                }
            }
        }
        let store = self.topics.lock().unwrap();
        Ok(resources
            .iter()
            .map(|resource| {
                let entries = store
                    .get(&resource.name)
                    .map(|state| {
                        state
                            .config_overrides
                            .iter()
                            .map(|(name, value)| {
                                (
                                    name.clone(),
                                    ConfigEntry {
                                        name: name.clone(),
                                        value: Some(value.clone()),
                                        source: ConfigSource::DynamicTopicConfig,
                                        is_sensitive: false,
                                        is_read_only: false,
                                        synonyms: Vec::new(),
                                        config_type: ConfigType::Unknown,
                                        documentation: None,
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (resource.clone(), Ok(Config { entries }))
            })
            .collect())
    }

    async fn incremental_alter_configs(
        &mut self,
        configs: &BTreeMap<ConfigResource, Vec<AlterConfigOp>>,
        options: IncrementalAlterConfigsOptions,
    ) -> Result<AlterConfigsResults, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::IncrementalAlterConfigs(configs.clone()));
        if let Some(inj) = self
            .injected
            .lock()
            .unwrap()
            .incremental_alter_configs
            .clone()
        {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(configs
                        .keys()
                        .map(|resource| {
                            (
                                resource.clone(),
                                Err(KafkaError {
                                    code,
                                    name,
                                    message: message.clone(),
                                }),
                            )
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        if !options.validate_only {
            let mut store = self.topics.lock().unwrap();
            for (resource, ops) in configs {
                for op in ops {
                    match op.op_type {
                        AlterConfigOpType::Set => {
                            if let Some(state) = store.get_mut(&resource.name) {
                                state.config_overrides.insert(
                                    op.name.clone(),
                                    op.value.clone().ok_or_else(|| {
                                        AdminError::InvalidArgument("SET needs a value".into())
                                    })?,
                                );
                            }
                        }
                        AlterConfigOpType::Delete => {
                            if let Some(state) = store.get_mut(&resource.name) {
                                state.config_overrides.remove(&op.name);
                            }
                        }
                        AlterConfigOpType::Append | AlterConfigOpType::Subtract => {
                            return Err(AdminError::InvalidArgument(
                                "test fake supports SET and DELETE only".into(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(configs
            .keys()
            .map(|resource| (resource.clone(), Ok(())))
            .collect())
    }

    async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::AlterUserScramCredentials {
                upsertions: upsertions.to_vec(),
                deletions: deletions.to_vec(),
            });
        let mut users = self.scram_users.lock().unwrap();
        let mut out = Vec::with_capacity(upsertions.len() + deletions.len());
        for u in upsertions {
            users.insert(u.username.clone());
            out.push(ScramUserOutcome {
                username: u.username.clone(),
                error: None,
            });
        }
        for d in deletions {
            users.remove(&d.username);
            out.push(ScramUserOutcome {
                username: d.username.clone(),
                error: None,
            });
        }
        Ok(out)
    }

    async fn alter_user_scram_credentials_sha256(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        // Same in-memory model as SHA-512; the fake doesn't care about
        // the mechanism wire byte, only the recorded-call trace.
        self.alter_user_scram_credentials_sha512(upsertions, deletions)
            .await
    }

    async fn describe_acls(
        &mut self,
        filter: &AclEntryFilter,
    ) -> Result<Vec<AclEntry>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeAcls(filter.clone()));
        let store = self.acls.lock().unwrap();
        Ok(store
            .iter()
            .filter(|e| matches_filter(filter, e))
            .cloned()
            .collect())
    }

    async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateAcls(creations.to_vec()));
        if let Some(injected) = self.injected.lock().unwrap().create_acls.clone() {
            match injected {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(creations
                        .iter()
                        .map(|_| CreateAclOutcome {
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel {
                    api,
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    });
                }
            }
        }
        let mut store = self.acls.lock().unwrap();
        let mut out = Vec::with_capacity(creations.len());
        for e in creations {
            store.insert(e.clone());
            out.push(CreateAclOutcome { error: None });
        }
        Ok(out)
    }

    async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteAcls(filters.to_vec()));
        let mut store = self.acls.lock().unwrap();
        let mut out = Vec::with_capacity(filters.len());
        for f in filters {
            let matched: Vec<AclEntry> = store
                .iter()
                .filter(|e| matches_filter(f, e))
                .cloned()
                .collect();
            for e in &matched {
                store.remove(e);
            }
            out.push(DeleteAclFilterOutcome {
                error: None,
                matched,
            });
        }
        Ok(out)
    }

    async fn describe_user_quotas(
        &mut self,
        username: &str,
    ) -> Result<UserQuotaConfig, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeUserQuotas(username.into()));
        let store = self.user_quotas.lock().unwrap();
        Ok(store.get(username).cloned().unwrap_or_default())
    }

    async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::AlterUserQuotas {
                username: username.into(),
                ops: ops.to_vec(),
                validate_only,
            });
        if validate_only {
            return Ok(None);
        }
        let mut store = self.user_quotas.lock().unwrap();
        let entry = store.entry(username.into()).or_default();
        for op in ops {
            match op {
                QuotaOp::Set { key, value } => {
                    entry.insert(key.clone(), *value);
                }
                QuotaOp::Remove { key } => {
                    entry.remove(key);
                }
            }
        }
        if entry.is_empty() {
            store.remove(username);
        }
        Ok(None)
    }

    // ── delegation-token RPCs ─────────────────────────────────────────
    //
    // In-memory KIP-48 model. Tokens carry:
    //   - `token_id`  — sequential `tok-<n>` strings
    //   - `hmac`      — 32 zero bytes plus the id as a discriminator,
    //                   so `expire(hmac)` / `renew(hmac)` can find them.
    //   - `expiry_timestamp_ms` — `now + 7d` on create.
    //   - `max_timestamp_ms`    — `now + 30d` on create.
    //
    // Renew advances `expiry_timestamp_ms` to `min(now + 7d, max)`. The
    // operator's `reconcile_renews_when_within_threshold` test depends
    // on this — it uses a `renew_before_expiry_ms` of exactly 7d so the
    // decision always lands on Renew, and asserts that the post-renew
    // expiry only ever increases (or holds at `max`).
    async fn create_delegation_token(
        &mut self,
        options: &CreateDelegationTokenOptions,
    ) -> Result<DelegationToken, AdminError> {
        let owner = options.owner.clone().unwrap_or(KafkaPrincipal {
            principal_type: "User".into(),
            name: "operator".into(),
        });
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateDelegationToken {
                owner_principal_name: owner.name.clone(),
                renewers: options.renewers.iter().map(ToString::to_string).collect(),
                max_lifetime: options.max_lifetime,
            });
        let now_ms = chrono::Utc::now().timestamp_millis();
        // The broker caps an absent or over-long lifetime at its 7-day
        // `delegation.token.max.lifetime.ms` default.
        let broker_ceiling = days(7);
        let lifetime_ms = options
            .max_lifetime
            .filter(|lifetime| *lifetime > Time::ZERO)
            .unwrap_or(broker_ceiling)
            .min(broker_ceiling)
            .millis_i64();
        let max_ts = now_ms + 30 * 24 * 60 * 60 * 1_000;
        let id = {
            let mut next = self.next_token_id.lock().unwrap();
            let i = *next;
            *next += 1;
            i
        };
        let token_id = format!("tok-{id}");
        // Discriminating HMAC: 32 bytes where the trailing 8 carry the
        // id LE-encoded so `renew`/`expire` can match the right token.
        let mut hmac = vec![0u8; 32];
        hmac[24..].copy_from_slice(&id.to_le_bytes());

        let token = DelegationToken {
            token_id,
            token_requester: owner.clone(),
            owner,
            hmac,
            issue_timestamp_ms: now_ms,
            expiry_timestamp_ms: now_ms + lifetime_ms,
            max_timestamp_ms: max_ts,
            renewers: options.renewers.clone(),
        };
        self.delegation_tokens.lock().unwrap().push(token.clone());
        Ok(token)
    }

    async fn renew_delegation_token(
        &mut self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> Result<i64, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::RenewDelegationToken {
                hmac: hmac.to_vec(),
            });
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut store = self.delegation_tokens.lock().unwrap();
        let pos = store
            .iter()
            .position(|t| t.hmac == hmac)
            .ok_or_else(|| AdminError::Protocol("renew: hmac not found".into()))?;
        let max = store[pos].max_timestamp_ms;
        let period = options
            .renew_time_period
            .filter(|period| *period > Time::ZERO)
            .unwrap_or(days(7))
            .min(days(7));
        let new_expiry = (now_ms + period.millis_i64()).min(max);
        // Renew never moves expiry backwards.
        if new_expiry > store[pos].expiry_timestamp_ms {
            store[pos].expiry_timestamp_ms = new_expiry;
        }
        Ok(store[pos].expiry_timestamp_ms)
    }

    async fn expire_delegation_token(
        &mut self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> Result<i64, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::ExpireDelegationToken {
                hmac: hmac.to_vec(),
            });
        let mut store = self.delegation_tokens.lock().unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Some(period) = options
            .expiry_time_period
            .filter(|period| *period >= Time::ZERO)
        {
            let token = store
                .iter_mut()
                .find(|token| token.hmac == hmac)
                .ok_or_else(|| AdminError::Protocol("expire: hmac not found".into()))?;
            token.expiry_timestamp_ms = now_ms
                .saturating_add(period.millis_i64())
                .min(token.max_timestamp_ms);
            Ok(token.expiry_timestamp_ms)
        } else {
            store.retain(|t| t.hmac != hmac);
            Ok(now_ms)
        }
    }

    async fn describe_delegation_token(
        &mut self,
        options: &DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>, AdminError> {
        if let Some(owners) = &options.owners {
            for owner in owners {
                self.recorded_calls.lock().unwrap().push(
                    RecordedCall::DescribeDelegationTokensOwnedBy {
                        owner_principal: owner.to_string(),
                    },
                );
            }
        }
        let store = self.delegation_tokens.lock().unwrap();
        Ok(store
            .iter()
            .filter(|token| {
                options
                    .owners
                    .as_ref()
                    .is_none_or(|owners| owners.contains(&token.owner))
            })
            .cloned()
            .collect())
    }
}

/// True if every populated axis of `filter` matches `entry`. Matches
/// the broker's `AclEntryFilter::matches` semantics.
fn matches_filter(f: &AclEntryFilter, e: &AclEntry) -> bool {
    f.resource_type.is_none_or(|rt| rt == e.resource_type)
        && f.resource_name
            .as_ref()
            .is_none_or(|n| n == &e.resource_name)
        && f.pattern_type.is_none_or(|pt| pt == e.pattern_type)
        && f.principal.as_ref().is_none_or(|p| p == &e.principal)
        && f.host.as_ref().is_none_or(|h| h == &e.host)
        && f.operation.is_none_or(|op| op == e.operation)
        && f.permission_type.is_none_or(|p| p == e.permission_type)
}
