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

impl<N, C, M, K, R> Node<N, C, M, K, R>
where
    N: NetworkStatusPort + BlockRequestPort + BlockSendPort + BlockBroadcastPort,
    C: BlockStorePort,
    M: MempoolPort,
    K: ConsensusPort,
    R: PeerReputationPort,
{
    pub fn new(
        network: N,
        chain_store: C,
        mempool: M,
        consensus: K,
        peer_reputation: R,
        network_event_rx: mpsc::Receiver<NetworkEvent>,
        block_production_interval: Option<Duration>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let event_loop_handle = Self::spawn_event_loop(
            chain_store,
            mempool,
            network.clone(),
            consensus,
            peer_reputation,
            network_event_rx,
            block_production_interval,
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
        consensus: K,
        peer_reputation: R,
        mut network_event_rx: tokio::sync::mpsc::Receiver<NetworkEvent>,
        block_production_interval: Option<Duration>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut block_production_ticker = block_production_interval.map(tokio::time::interval);
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
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
                            &chain_store,
                            &network,
                            &peer_reputation,
                            &consensus,
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
                                let inserted = mempool.insert_transaction(tx).await;
                                tracing::debug!(inserted, "transaction mempool insert finished");
                            }
                            NetworkEvent::BlockAnnouncementReceived {
                                peer_id,
                                announcement,
                            } => {
                                tracing::info!(%peer_id, ?announcement, "block announcement received");
                                if let Err(error) = Self::handle_consensus_event(
                                    &chain_store,
                                    &network,
                                    &peer_reputation,
                                    &consensus,
                                    ConsensusEvent::BlockAnnouncement { peer_id, announcement },
                                ).await {
                                    tracing::warn!(?error, "failed to handle block announcement through consensus");
                                }
                            }
                            NetworkEvent::BlockReceived { peer_id, block } => {
                                tracing::info!(%peer_id, ?block, "block received");
                                if let Err(error) = Self::handle_consensus_event(
                                    &chain_store,
                                    &network,
                                    &peer_reputation,
                                    &consensus,
                                    ConsensusEvent::BlockReceived { peer_id, block },
                                ).await {
                                    tracing::warn!(?error, "failed to handle received block through consensus");
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
