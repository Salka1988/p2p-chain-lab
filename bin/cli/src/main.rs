mod task_group;

use std::time::Duration;

use crate::task_group::TaskGroup;
use p2p_chain_lib_chain::store::ChainStore;
use p2p_chain_lib_mempool::Mempool;
use p2p_chain_lib_network::{
    peer_score::{PeerScoreActor, PeerScoreHandle},
    service::{NetworkHandle, NetworkService},
};
use p2p_chain_lib_node::{config::Config, node::Node};
use p2p_chain_lib_sync::SyncService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args()?;
    let mut task_group = TaskGroup::new().with_shutdown_timeout(Duration::from_secs(5));
    let (network_command_tx, network_command_rx) = tokio::sync::mpsc::channel(1024);
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_command_tx, peer_score_command_rx) = tokio::sync::mpsc::channel(1024);
    let (peer_score_event_tx, peer_score_event_rx) = tokio::sync::mpsc::channel(1024);

    let network = NetworkHandle::with_listen_addr(network_command_tx, config.listen_addr.clone());
    let peer_score = PeerScoreHandle::new(peer_score_command_tx);
    let chain_store = ChainStore::new();
    let mempool = Mempool::new();
    let sync_service = SyncService::new(network.clone(), chain_store.clone(), peer_score.clone());

    let network_service = NetworkService::new(
        config.into(),
        network_command_rx,
        network_event_tx,
        peer_score.clone(),
        peer_score_event_rx,
    )?;
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

    let node = Node::new(
        network,
        chain_store,
        mempool,
        sync_service,
        peer_score,
        network_event_rx,
        node_cancellation_token.clone(),
    );

    task_group.spawn(
        "node",
        async move { node.run(node_cancellation_token).await },
    );

    task_group.wait().await
}
