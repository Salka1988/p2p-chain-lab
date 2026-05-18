use libp2p_mini_chain_chain::store::ChainStore;
use libp2p_mini_chain_mempool::Mempool;
use libp2p_mini_chain_network::service::{NetworkHandle, NetworkService};
use libp2p_mini_chain_node::{config::Config, node::Node};
use libp2p_mini_chain_sync::SyncService;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args()?;
    let cancellation_token = CancellationToken::new();
    let (network_command_tx, network_command_rx) = tokio::sync::mpsc::channel(1024);
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);

    let network = NetworkHandle::with_listen_addr(network_command_tx, config.listen_addr.clone());
    let chain_store = ChainStore::new();
    let mempool = Mempool::new();
    let sync_service = SyncService::new(network.clone(), chain_store.clone());

    let network_service = NetworkService::new(config.into(), network_command_rx, network_event_tx)?;
    let network_cancellation_token = cancellation_token.child_token();

    let mut network_task =
        tokio::spawn(async move { network_service.run(network_cancellation_token).await });

    let node = Node::new(
        network,
        chain_store,
        mempool,
        sync_service,
        network_event_rx,
        cancellation_token.clone(),
    );

    tokio::select! {
        node_result = node.run(cancellation_token.clone()) => {
            cancellation_token.cancel();
            node_result?;
            (&mut network_task).await??;
        }
        network_result = &mut network_task => {
            cancellation_token.cancel();
            network_result??;
            anyhow::bail!("network service stopped unexpectedly");
        }
    }

    Ok(())
}
