//! Boots an in-process, multi-broker Kafka cluster for the live suites.
//!
//! Every broker starts in static-voter bootstrap mode (KIP-595) with the same
//! controller voter set. The client and controller listeners are bound up
//! front and handed to the brokers, so no other test can take a port between
//! the reservation and the bind.

use std::{net::SocketAddr, time::Duration};

use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use tempfile::TempDir;

/// One running broker, its configuration, and the directory that holds its log.
pub type ClusterNode = (BrokerHandle, BrokerConfig, TempDir);

/// Starts an `n`-broker cluster, and retries up to three times.
///
/// The short raft timings of the test configuration can split the vote on a
/// slow runner. A retry uses fresh directories and ports.
///
/// # Panics
/// Panics when no attempt elects a controller leader.
pub async fn start_n_node_with_retry(n: usize) -> Vec<ClusterNode> {
    let mut last_error = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(error) => {
                tracing::warn!(attempt, %error, "cluster start failed; retrying");
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_error:?}");
}

async fn start_n_node(n: usize) -> Result<Vec<ClusterNode>, BrokerError> {
    let mut clients = Vec::with_capacity(n);
    let mut controllers = Vec::with_capacity(n);
    for _ in 0..n {
        clients.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
        controllers.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let client_addrs = clients
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<Vec<SocketAddr>>>()?;
    let voters = controllers
        .iter()
        .zip(1_u64..)
        .map(|(listener, id)| Ok((NodeId(id), listener.local_addr()?.to_string())))
        .collect::<std::io::Result<Vec<_>>>()?;

    // A broker's start waits for a controller leader, which needs a majority
    // of the voters, so the brokers start concurrently.
    let mut starts = Vec::with_capacity(n);
    let mut metas = Vec::with_capacity(n);
    for (index, (client, controller)) in clients.into_iter().zip(controllers).enumerate() {
        let dir = TempDir::new()?;
        let config = node_config(index, client_addrs[index], &voters, dir.path(), n);
        let spawned = config.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(spawned, Some(controller), Some(client)).await
        }));
        metas.push((config, dir));
    }

    let mut cluster = Vec::with_capacity(n);
    for (start, (config, dir)) in starts.into_iter().zip(metas) {
        let handle = start.await.map_err(|error| {
            BrokerError::Startup(format!("broker start task panicked: {error}"))
        })??;
        cluster.push((handle, config, dir));
    }

    let mut leader = cluster[0].0.watch_leader_for_test();
    let elected = tokio::time::timeout(
        Duration::from_secs(30),
        leader.wait_for(|leader| matches!(leader, Some(id) if *id != 0)),
    )
    .await;
    if !matches!(elected, Ok(Ok(_))) {
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n} voters within 30s"
        )));
    }
    Ok(cluster)
}

fn node_config(
    index: usize,
    client_addr: SocketAddr,
    voters: &[(NodeId, String)],
    log_dir: &std::path::Path,
    brokers: usize,
) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
    let id = u64::try_from(index + 1).expect("node id");
    config.broker_id = i32::try_from(id).expect("broker id");
    config.node_id = NodeId(id);
    config.directory_id = uuid::Uuid::from_u128(u128::from(id));
    config.listen_addr = client_addr;
    config.advertised_listener = client_addr.to_string();
    config.controller_listen_addr = voters[index].1.parse().expect("controller address");
    config.controller_quorum_voters = voters.to_vec();
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.auto_join = false;
    config.bootstrap_servers = vec![];
    config.with_internal_topics_for(brokers)
}
