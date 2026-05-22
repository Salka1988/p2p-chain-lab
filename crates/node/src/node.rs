use std::marker::PhantomData;

use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_mempool::MempoolPort;
use p2p_chain_lib_ports::{BlockSendPort, NetworkEvent, NetworkStatusPort, PeerReputationPort};
use p2p_chain_lib_protocol::validation::validate_block;
use p2p_chain_lib_sync::SyncPort;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct Node<N, C, M, S, R>
where
    N: NetworkStatusPort + BlockSendPort,
    C: BlockStorePort,
    M: MempoolPort,
    S: SyncPort,
    R: PeerReputationPort,
{
    network: N,
    event_loop_handle: JoinHandle<()>,
    _ports: PhantomData<(C, M, S, R)>,
}

impl<N, C, M, S, R> Node<N, C, M, S, R>
where
    N: NetworkStatusPort + BlockSendPort,
    C: BlockStorePort,
    M: MempoolPort,
    S: SyncPort,
    R: PeerReputationPort,
{
    pub fn new(
        network: N,
        chain_store: C,
        mempool: M,
        sync_service: S,
        peer_reputation: R,
        network_event_rx: mpsc::Receiver<NetworkEvent>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let event_loop_handle = Self::spawn_event_loop(
            chain_store,
            mempool,
            network.clone(),
            sync_service,
            peer_reputation,
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
        cancellation_token.cancelled().await;
        tracing::info!("node shutdown requested");

        self.event_loop_handle.await?;
        tracing::info!("node stopped");
        Ok(())
    }

    fn spawn_event_loop(
        chain_store: C,
        mempool: M,
        network: N,
        sync_service: S,
        peer_reputation: R,
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
                            NetworkEvent::Listening(address) => {
                                tracing::info!(%address, "network listening");
                            }
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
                                        if let Err(error) = peer_reputation
                                            .report_good_peer(peer_id, "valid block received".to_string())
                                            .await
                                        {
                                            tracing::warn!(?error, "failed to report good peer");
                                        }
                                    }
                                    Err(error) => {
                                        tracing::warn!(%peer_id, ?error, "received invalid block");
                                        if let Err(report_error) = peer_reputation
                                            .report_bad_peer(peer_id, format!("invalid block: {error:?}"))
                                            .await
                                        {
                                            tracing::warn!(?report_error, "failed to report bad peer");
                                        }
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
                            NetworkEvent::PeerDiscovered(peer_id) => {
                                tracing::info!(%peer_id, "peer discovered");
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
