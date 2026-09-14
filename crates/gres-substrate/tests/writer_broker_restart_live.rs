//! The producer-backed WAL writer across a real broker restart.
//!
//! A broker restart closes the cached transaction-coordinator connection. An
//! `EndTxn` sent on that connection is lost in transport. Before the fix, the
//! writer called that outcome indeterminate and stopped the compute. These
//! tests restart an in-process broker on the same ports and the same data
//! directory between the produce acknowledgements and `EndTxn(commit)`.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use assert2::assert;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{ProducerError, ProducerRetryPolicy};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_gres_substrate::{
    GroupCommitRequest, LiveRecoveryConfig, ProducerWalWriter, TransactionalWalWriter, WalFrame,
    WalWriterFaultInjector, WalWriterFaultStage, WriterGeneration, recover_live_for_range,
};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot};

/// An in-process broker that can stop and start again on the same ports.
struct RestartableBroker {
    dir: TempDir,
    data_addr: SocketAddr,
    controller_addr: SocketAddr,
    handle: Option<BrokerHandle>,
}

impl RestartableBroker {
    async fn start() -> Self {
        raise_fd_limit_for_broker();
        let dir = TempDir::new().expect("broker tempdir");
        let data = TcpListener::bind("127.0.0.1:0").await.expect("data port");
        let controller = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("controller port");
        let mut broker = Self {
            dir,
            data_addr: data.local_addr().expect("data address"),
            controller_addr: controller.local_addr().expect("controller address"),
            handle: None,
        };
        broker
            .boot(BootstrapMode::Bootstrap, data, controller)
            .await;
        broker
    }

    fn bootstrap(&self) -> String {
        self.data_addr.to_string()
    }

    async fn boot(&mut self, mode: BootstrapMode, data: TcpListener, controller: TcpListener) {
        let mut config = BrokerConfig::for_tests(self.dir.path().to_path_buf());
        config.listen_addr = self.data_addr;
        config.advertised_listener = self.data_addr.to_string();
        config.controller_listen_addr = self.controller_addr;
        config.controller_quorum_voters = vec![(config.node_id, self.controller_addr.to_string())];
        config.bootstrap_mode = mode;
        self.handle = Some(
            Broker::start_with_listeners(config, Some(controller), Some(data))
                .await
                .expect("broker start"),
        );
    }

    async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown().await;
        }
    }

    async fn restart(&mut self) {
        let data = bind_again(self.data_addr).await;
        let controller = bind_again(self.controller_addr).await;
        self.boot(BootstrapMode::Rejoin, data, controller).await;
    }
}

async fn bind_again(address: SocketAddr) -> TcpListener {
    for _ in 0..100 {
        if let Ok(listener) = TcpListener::bind(address).await {
            return listener;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {address} did not become free");
}

#[cfg(unix)]
fn raise_fd_limit_for_broker() {
    let limits = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    if limits.current.unwrap_or(0) < 8192 {
        rustix::process::setrlimit(
            rustix::process::Resource::Nofile,
            rustix::process::Rlimit {
                current: Some(8192),
                maximum: limits.maximum,
            },
        )
        .expect("raise soft file descriptor limit for live broker tests");
    }
}

#[cfg(not(unix))]
fn raise_fd_limit_for_broker() {}

/// Holds the writer after its produce acknowledgements, until the test lets it
/// send `EndTxn(commit)`.
struct PauseBeforeEndTxn {
    armed: AtomicBool,
    reached: Mutex<mpsc::Sender<()>>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl WalWriterFaultInjector for PauseBeforeEndTxn {
    fn inject(&self, stage: WalWriterFaultStage) -> Option<ProducerError> {
        if stage == WalWriterFaultStage::AfterSendAcks && self.armed.swap(false, Ordering::SeqCst) {
            self.reached
                .lock()
                .expect("reached sender")
                .send(())
                .expect("test waits for the pause");
            self.resume
                .lock()
                .expect("resume receiver")
                .recv()
                .expect("test resumes the writer");
        }
        None
    }
}

/// The test side of [`PauseBeforeEndTxn`].
struct PauseControl {
    reached: mpsc::Receiver<()>,
    resume: mpsc::Sender<()>,
}

impl PauseControl {
    async fn wait_until_paused(self) -> mpsc::Sender<()> {
        let Self { reached, resume } = self;
        tokio::task::spawn_blocking(move || reached.recv())
            .await
            .expect("pause wait task")
            .expect("writer reaches the pause");
        resume
    }
}

fn pause_before_end_txn() -> (Arc<PauseBeforeEndTxn>, PauseControl) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    (
        Arc::new(PauseBeforeEndTxn {
            armed: AtomicBool::new(true),
            reached: Mutex::new(reached_tx),
            resume: Mutex::new(resume_rx),
        }),
        PauseControl {
            reached: reached_rx,
            resume: resume_tx,
        },
    )
}

fn recovery_config(bootstrap: &str, tenant: &str, retry_timeout: Duration) -> LiveRecoveryConfig {
    let defaults = ProducerRetryPolicy::default();
    LiveRecoveryConfig::new(
        bootstrap,
        TenantName::parse(tenant).expect("tenant"),
        RangeId::COORDINATOR,
        None,
    )
    .with_producer_retry_policy(
        ProducerRetryPolicy::new(
            defaults.request_timeout(),
            defaults.retries(),
            defaults.retry_backoff(),
            defaults.routing_retry_budget(),
            retry_timeout,
            defaults.init_max_backoff(),
            defaults.transaction_timeout(),
        )
        .expect("retry policy"),
    )
}

fn request(seq: u64, key: &[u8]) -> GroupCommitRequest {
    GroupCommitRequest {
        generation: WriterGeneration(0),
        frames: vec![WalFrame {
            journal_seq: seq,
            ops: vec![WriteOp::Put {
                key: key.to_vec(),
                value: b"committed".to_vec(),
            }],
        }],
    }
}

fn topic(tenant: &str) -> String {
    format!("__gres_wal.{tenant}.r0")
}

/// The writer's action for an unknown commit outcome.
type ExitHandler = Arc<dyn Fn(&ProducerError) + Send + Sync>;

/// A handler that reports the unknown-outcome exit instead of exiting.
fn exit_notifier() -> (ExitHandler, oneshot::Receiver<()>) {
    let (sender, receiver) = oneshot::channel();
    let sender = Mutex::new(Some(sender));
    (
        Arc::new(move |_| {
            if let Some(sender) = sender.lock().expect("exit sender").take() {
                let _ = sender.send(());
            }
        }),
        receiver,
    )
}

/// The rows a fresh recovery replays for these keys.
async fn replayed(config: LiveRecoveryConfig, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let store = MemKv::default();
    recover_live_for_range(config, &store)
        .await
        .expect("successor recovery");
    keys.iter()
        .map(|key| store.get(key).expect("read replayed row"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_lost_in_a_broker_restart_commits_and_the_writer_continues() {
    let tenant = "restart-commit";
    let mut broker = RestartableBroker::start().await;
    let config = recovery_config(&broker.bootstrap(), tenant, Duration::from_secs(30));
    let recovered = recover_live_for_range(config.clone(), &MemKv::default())
        .await
        .expect("initial recovery");
    let (injector, control) = pause_before_end_txn();
    let (handler, mut exited) = exit_notifier();
    let writer = Arc::new(
        ProducerWalWriter::new(recovered.producer, topic(tenant))
            .with_fault_injector(injector)
            .with_indeterminate_handler(handler),
    );
    let commit = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { writer.commit_group(request(0, b"restart/during")).await }
    });

    let resume = control.wait_until_paused().await;
    broker.stop().await;
    broker.restart().await;
    resume.send(()).expect("writer waits to resume");

    let during = tokio::time::timeout(Duration::from_mins(1), commit)
        .await
        .expect("commit answers after the restart")
        .expect("commit task")
        .expect("the retried EndTxn learns that the group committed");
    assert!(during.frames.len() == 1);
    assert!(exited.try_recv().is_err(), "the writer must not exit");

    writer
        .commit_group(request(1, b"restart/after"))
        .await
        .expect("the writer keeps committing after the restart");

    let rows = replayed(config, &[b"restart/during", b"restart/after"]).await;
    assert!(rows == vec![Some(b"committed".to_vec()), Some(b"committed".to_vec())]);
    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_lost_past_the_retry_deadline_exits_without_an_answer() {
    let tenant = "restart-deadline";
    let mut broker = RestartableBroker::start().await;
    let config = recovery_config(&broker.bootstrap(), tenant, Duration::from_secs(2));
    let recovered = recover_live_for_range(config.clone(), &MemKv::default())
        .await
        .expect("initial recovery");
    let (injector, control) = pause_before_end_txn();
    let (handler, exited) = exit_notifier();
    let writer = Arc::new(
        ProducerWalWriter::new(recovered.producer, topic(tenant))
            .with_fault_injector(injector)
            .with_indeterminate_handler(handler),
    );
    let commit = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { writer.commit_group(request(0, b"deadline/lost")).await }
    });

    let resume = control.wait_until_paused().await;
    broker.stop().await;
    resume.send(()).expect("writer waits to resume");

    tokio::time::timeout(Duration::from_mins(1), exited)
        .await
        .expect("the writer exits after the retry deadline")
        .expect("exit signal");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), commit)
            .await
            .is_err(),
        "an unknown outcome must not answer its caller"
    );

    broker.restart().await;
    // `EndTxn` never reached the broker, so the successor aborts the group.
    let rows = replayed(config, &[b"deadline/lost"]).await;
    assert!(rows == vec![None]);
    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_commits_after_an_idle_broker_restart() {
    let tenant = "restart-idle";
    let mut broker = RestartableBroker::start().await;
    let config = recovery_config(&broker.bootstrap(), tenant, Duration::from_secs(30));
    let recovered = recover_live_for_range(config.clone(), &MemKv::default())
        .await
        .expect("initial recovery");
    let (handler, mut exited) = exit_notifier();
    let writer = ProducerWalWriter::new(recovered.producer, topic(tenant))
        .with_indeterminate_handler(handler);
    writer
        .commit_group(request(0, b"idle/before"))
        .await
        .expect("commit before the restart");

    broker.stop().await;
    broker.restart().await;

    // The restarted broker rebuilds producer state from the log. A
    // transaction-version-2 end marker raised the epoch and cleared the
    // sequence, so the next batch must start again at sequence 0.
    tokio::time::timeout(
        Duration::from_mins(1),
        writer.commit_group(request(1, b"idle/after")),
    )
    .await
    .expect("commit answers after the restart")
    .expect("the writer commits after the restart");
    assert!(exited.try_recv().is_err(), "the writer must not exit");

    let rows = replayed(config, &[b"idle/before", b"idle/after"]).await;
    assert!(rows == vec![Some(b"committed".to_vec()), Some(b"committed".to_vec())]);
    broker.stop().await;
}
