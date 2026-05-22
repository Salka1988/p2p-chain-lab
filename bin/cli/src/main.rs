mod task_group;

use std::time::Duration;

use crate::task_group::TaskGroup;
use p2p_chain_lib_chain::store::ChainStore;
use p2p_chain_lib_consensus::SimpleConsensus;
use p2p_chain_lib_mempool::Mempool;
use p2p_chain_lib_network::{
    peer_score::{PeerScoreActor, PeerScoreHandle},
    service::{NetworkConfig, NetworkService},
};
use p2p_chain_lib_node::{
    config::Config,
    node::{Node, NodeRuntime},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args()?;
    let block_production_interval = config.block_production_interval;
    let consensus_node_id = config
        .node_key_seed
        .clone()
        .unwrap_or_else(|| config.listen_addr.as_str().to_string());
    let network_config = NetworkConfig {
        listen_addr: config.listen_addr.clone(),
        bootstrap_peers: config.bootstrap_peers.clone(),
        node_key_seed: config.node_key_seed.clone(),
    };
    let mut task_group = TaskGroup::new().with_shutdown_timeout(Duration::from_secs(5));
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_command_tx, peer_score_command_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_event_tx, peer_score_event_rx) = tokio::sync::mpsc::channel(1024);

    let peer_score = PeerScoreHandle::new(peer_score_command_tx);
    let chain_store = ChainStore::new();
    let mempool = Mempool::new();

    let (network_service, network) = NetworkService::new(
        network_config,
        network_event_tx,
        peer_score.clone(),
        peer_score_event_rx,
    )?;
    let consensus = SimpleConsensus::new(consensus_node_id, chain_store.clone());
    let network_cancellation_token = task_group.child_token();
    let peer_score_cancellation_token = task_group.child_token();
    let node_cancellation_token = task_group.child_token();

    task_group.spawn("network service", async move {
        network_service.run(network_cancellation_token).await
    });
    task_group.spawn("peer score service", async move {
        PeerScoreActor::new(peer_score_command_rx, peer_score_event_tx)
            .run(peer_score_cancellation_token)
            .await
    });

    let node = Node::new(NodeRuntime {
        network,
        chain_store,
        mempool,
        consensus,
        peer_reputation: peer_score,
        network_event_rx,
        block_production_interval,
        cancellation_token: node_cancellation_token.clone(),
    });

    task_group.spawn(
        "node",
        async move { node.run(node_cancellation_token).await },
    );

    task_group.wait().await
}
