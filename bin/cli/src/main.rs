mod config;
mod task_group;

use std::{sync::Arc, time::Duration};

use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_ports::PeerReputationHandle;

use crate::task_group::TaskGroup;
use p2p_chain_lib_consensus::{SimpleConsensus, validator_id_from_seed};
use p2p_chain_lib_mempool::Mempool;
use p2p_chain_lib_network::{
    peer_score::PeerScoreActor,
    service::{NetworkConfig, NetworkService},
};
use p2p_chain_lib_node::node::{Node, NodeRuntime};
use p2p_chain_lib_storage::ChainStore;
use p2p_chain_lib_sync::SyncService;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = config::from_args()?;
    let block_production_interval = config.block_production_interval;
    let consensus_seed = config.node_key_seed.as_deref().ok_or_else(|| {
        anyhow::anyhow!("--node-key-seed is required for signed block production")
    })?;
    let consensus_node_id = validator_id_from_seed(consensus_seed);
    let network_config = NetworkConfig {
        listen_addr: config.listen_addr.clone(),
        bootstrap_peers: config.bootstrap_peers.clone(),
        node_key_seed: config.node_key_seed.clone(),
        validator_id: Some(consensus_node_id.clone()),
        validators: config.genesis.validators.clone(),
        max_gossip_events_per_second: config.max_gossip_events_per_second,
        max_block_requests_per_second: config.max_block_requests_per_second,
    };
    let mut task_group = TaskGroup::new().with_shutdown_timeout(Duration::from_secs(5));
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_command_tx, peer_score_command_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_event_tx, peer_score_event_rx) = tokio::sync::mpsc::channel(1024);

    let peer_score = PeerReputationHandle::new(peer_score_command_tx);
    let chain_store: Arc<dyn BlockStorePort> =
        Arc::new(ChainStore::open(&config.chain_path).await?);
    let consensus = SimpleConsensus::with_genesis(
        consensus_seed,
        chain_store.clone(),
        &config.genesis.chain_id,
        config.genesis.validators,
    )
    .await?;
    let mempool = Mempool::new();

    let (network_service, network) = NetworkService::new(
        network_config,
        network_event_tx,
        peer_score.clone(),
        peer_score_event_rx,
    )?;
    let sync = SyncService::new(network.clone(), chain_store.clone(), peer_score.clone());
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
        sync,
        peer_reputation: peer_score,
        network_event_rx,
        block_production_interval,
    });

    task_group.spawn(
        "node",
        async move { node.run(node_cancellation_token).await },
    );

    task_group.wait().await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}
