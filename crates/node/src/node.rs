use std::{marker::PhantomData, time::Duration};

use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_consensus::{ConsensusCommand, ConsensusEvent, ConsensusPort};
use p2p_chain_lib_mempool::MempoolPort;
use p2p_chain_lib_ports::{
    BlockBroadcastPort, BlockRequestPort, BlockSendPort, NetworkEvent, NetworkStatusPort,
    PeerReputationPort,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct Node<N, C, M, K, R>
where
    N: NetworkStatusPort + BlockRequestPort + BlockSendPort + BlockBroadcastPort,
    C: BlockStorePort,
    M: MempoolPort,
    K: ConsensusPort,
    R: PeerReputationPort,
{
    network: N,
    event_loop_handle: JoinHandle<()>,
    _ports: PhantomData<(C, M, K, R)>,
}

pub struct NodeRuntime<N, C, M, K, R> {
    pub network: N,
    pub chain_store: C,
    pub mempool: M,
    pub consensus: K,
    pub peer_reputation: R,
    pub network_event_rx: mpsc::Receiver<NetworkEvent>,
    pub block_production_interval: Option<Duration>,
    pub cancellation_token: CancellationToken,
}

impl<N, C, M, K, R> Node<N, C, M, K, R>
where
    N: NetworkStatusPort + BlockRequestPort + BlockSendPort + BlockBroadcastPort,
    C: BlockStorePort,
    M: MempoolPort,
    K: ConsensusPort,
    R: PeerReputationPort,
{
    pub fn new(runtime: NodeRuntime<N, C, M, K, R>) -> Self {
        let network = runtime.network.clone();
        let event_loop_handle = Self::spawn_event_loop(NodeRuntime {
            network: runtime.network,
            chain_store: runtime.chain_store,
            mempool: runtime.mempool,
            consensus: runtime.consensus,
            peer_reputation: runtime.peer_reputation,
            network_event_rx: runtime.network_event_rx,
            block_production_interval: runtime.block_production_interval,
            cancellation_token: runtime.cancellation_token.child_token(),
        });

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

    fn spawn_event_loop(runtime: NodeRuntime<N, C, M, K, R>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut block_production_ticker =
                runtime.block_production_interval.map(tokio::time::interval);
            let mut network_event_rx = runtime.network_event_rx;
            loop {
                tokio::select! {
                    _ = runtime.cancellation_token.cancelled() => {
                        tracing::info!("node event loop shutdown requested");
                        break;
                    }
                    _ = async {
                        match block_production_ticker.as_mut() {
                            Some(ticker) => {
                                ticker.tick().await;
                            }
                            None => std::future::pending::<()>().await,
                        }
                    } => {
                        if let Err(error) = Self::handle_consensus_event(
                            &runtime.chain_store,
                            &runtime.network,
                            &runtime.peer_reputation,
                            &runtime.consensus,
                            ConsensusEvent::Tick,
                        ).await {
                            tracing::warn!(?error, "failed to handle consensus tick");
                        }
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
                                let inserted = runtime.mempool.insert_transaction(tx).await;
                                tracing::debug!(inserted, "transaction mempool insert finished");
                            }
                            NetworkEvent::BlockAnnouncementReceived {
                                peer_id,
                                announcement,
                            } => {
                                tracing::info!(%peer_id, ?announcement, "block announcement received");
                                if let Err(error) = Self::handle_consensus_event(
                                    &runtime.chain_store,
                                    &runtime.network,
                                    &runtime.peer_reputation,
                                    &runtime.consensus,
                                    ConsensusEvent::BlockAnnouncement { peer_id, announcement },
                                ).await {
                                    tracing::warn!(?error, "failed to handle block announcement through consensus");
                                }
                            }
                            NetworkEvent::BlockReceived { peer_id, block } => {
                                tracing::info!(%peer_id, ?block, "block received");
                                if let Err(error) = Self::handle_consensus_event(
                                    &runtime.chain_store,
                                    &runtime.network,
                                    &runtime.peer_reputation,
                                    &runtime.consensus,
                                    ConsensusEvent::BlockReceived { peer_id, block },
                                ).await {
                                    tracing::warn!(?error, "failed to handle received block through consensus");
                                }
                            }
                            NetworkEvent::BlockRequestReceived { peer_id, block_hash } => {
                                tracing::info!(%peer_id, ?block_hash, "block request received");
                                match runtime.chain_store.get_block(&block_hash).await {
                                    Some(block) => {
                                        if let Err(error) = runtime.network.send_block(peer_id, block).await {
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

    async fn handle_consensus_event(
        chain_store: &C,
        network: &N,
        peer_reputation: &R,
        consensus: &K,
        event: ConsensusEvent,
    ) -> anyhow::Result<()> {
        for command in consensus.handle_event(event).await? {
            match command {
                ConsensusCommand::StoreBlock(block) => {
                    let inserted = chain_store.insert_block(block).await;
                    tracing::debug!(inserted, "consensus block store insert finished");
                }
                ConsensusCommand::BroadcastBlock(block) => {
                    network.broadcast_block(block).await?;
                }
                ConsensusCommand::RequestBlock {
                    peer_id,
                    block_hash,
                } => {
                    network.request_block(peer_id, block_hash).await?;
                }
                ConsensusCommand::ReportGoodPeer { peer_id, reason } => {
                    peer_reputation.report_good_peer(peer_id, reason).await?;
                }
                ConsensusCommand::ReportBadPeer { peer_id, reason } => {
                    peer_reputation.report_bad_peer(peer_id, reason).await?;
                }
            }
        }

        Ok(())
    }
}
