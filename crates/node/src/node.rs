use std::marker::PhantomData;

use libp2p_mini_chain_chain::store::BlockStorePort;
use libp2p_mini_chain_mempool::MempoolPort;
use libp2p_mini_chain_network::{
    event::NetworkEvent,
    service::{BlockSendPort, NetworkStatusPort},
};
use libp2p_mini_chain_protocol::validation::validate_block;
use libp2p_mini_chain_sync::SyncPort;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct Node<N, C, M, S>
where
    N: NetworkStatusPort + BlockSendPort,
    C: BlockStorePort,
    M: MempoolPort,
    S: SyncPort,
{
    network: N,
    event_loop_handle: JoinHandle<()>,
    _ports: PhantomData<(C, M, S)>,
}

impl<N, C, M, S> Node<N, C, M, S>
where
    N: NetworkStatusPort + BlockSendPort,
    C: BlockStorePort,
    M: MempoolPort,
    S: SyncPort,
{
    pub fn new(
        network: N,
        chain_store: C,
        mempool: M,
        sync_service: S,
        network_event_rx: mpsc::Receiver<NetworkEvent>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let event_loop_handle = Self::spawn_event_loop(
            chain_store,
            mempool,
            network.clone(),
            sync_service,
            network_event_rx,
            cancellation_token.child_token(),
        );

        Self {
            network,
            event_loop_handle,
            _ports: PhantomData,
        }
    }

    pub async fn run(self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(listen_addr = %self.network.listen_addr(), "node running");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                tracing::info!("shutdown signal received");
                cancellation_token.cancel();
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("node shutdown requested");
            }
        }

        self.event_loop_handle.await?;
        tracing::info!("node stopped");
        Ok(())
    }

    fn spawn_event_loop(
        chain_store: C,
        mempool: M,
        network: N,
        sync_service: S,
        mut network_event_rx: tokio::sync::mpsc::Receiver<NetworkEvent>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        tracing::info!("node event loop shutdown requested");
                        break;
                    }
                    event = network_event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };

                        match event {
                            NetworkEvent::TransactionReceived { peer_id, tx } => {
                                tracing::info!(%peer_id, ?tx, "transaction received");
                                let inserted = mempool.insert_transaction(tx).await;
                                tracing::debug!(inserted, "transaction mempool insert finished");
                            }
                            NetworkEvent::BlockAnnouncementReceived {
                                peer_id,
                                announcement,
                            } => {
                                tracing::info!(%peer_id, ?announcement, "block announcement received");
                                if let Err(error) = sync_service
                                    .on_block_announcement(peer_id, announcement)
                                    .await
                                {
                                    tracing::warn!(?error, "failed to handle block announcement");
                                }
                            }
                            NetworkEvent::BlockReceived { peer_id, block } => {
                                tracing::info!(%peer_id, ?block, "block received");
                                match validate_block(&block) {
                                    Ok(()) => {
                                        let inserted = chain_store.insert_block(block).await;
                                        tracing::debug!(inserted, "block store insert finished");
                                    }
                                    Err(error) => {
                                        tracing::warn!(%peer_id, ?error, "received invalid block");
                                    }
                                }
                            }
                            NetworkEvent::BlockRequestReceived { peer_id, block_hash } => {
                                tracing::info!(%peer_id, ?block_hash, "block request received");
                                match chain_store.get_block(&block_hash).await {
                                    Some(block) => {
                                        if let Err(error) = network.send_block(peer_id, block).await {
                                            tracing::warn!(?error, "failed to send requested block");
                                        }
                                    }
                                    None => {
                                        tracing::debug!(?block_hash, "requested block not found");
                                    }
                                }
                            }
                            NetworkEvent::PeerConnected(peer_id) => {
                                tracing::info!(%peer_id, "peer connected");
                            }
                            NetworkEvent::PeerDisconnected(peer_id) => {
                                tracing::info!(%peer_id, "peer disconnected");
                            }
                            NetworkEvent::PeerMisbehaved { peer_id, reason } => {
                                tracing::warn!(%peer_id, %reason, "peer misbehaved");
                            }
                        }
                    }
                }
            }
            tracing::info!("node event loop stopped");
        })
    }
}
