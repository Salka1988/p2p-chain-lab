use p2p_chain_lab_chain::store::ChainStore;
use p2p_chain_lab_mempool::Mempool;
use p2p_chain_lab_network::{
    peer_score::{PeerScoreActor, PeerScoreHandle},
    service::{NetworkHandle, NetworkService},
};
use p2p_chain_lab_node::{config::Config, node::Node};
use p2p_chain_lab_sync::SyncService;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args()?;
    let cancellation_token = CancellationToken::new();
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
    let network_cancellation_token = cancellation_token.child_token();
    let peer_score_cancellation_token = cancellation_token.child_token();

    let mut network_task =
        tokio::spawn(async move { network_service.run(network_cancellation_token).await });
    let mut peer_score_task = tokio::spawn(async move {
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
        cancellation_token.clone(),
    );

    tokio::select! {
        node_result = node.run(cancellation_token.clone()) => {
            cancellation_token.cancel();
            node_result?;
            (&mut network_task).await??;
            (&mut peer_score_task).await??;
        }
        network_result = &mut network_task => {
            cancellation_token.cancel();
            network_result??;
            (&mut peer_score_task).await??;
            anyhow::bail!("network service stopped unexpectedly");
        }
        peer_score_result = &mut peer_score_task => {
            cancellation_token.cancel();
            peer_score_result??;
            (&mut network_task).await??;
            anyhow::bail!("peer score service stopped unexpectedly");
        }
    }

    Ok(())
}
