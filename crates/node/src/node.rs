use std::time::Duration;

use p2p_chain_lib_consensus::{
    ConsensusCommand, ConsensusEvent, ConsensusOutcome, SimpleConsensus,
};
use p2p_chain_lib_mempool::Mempool;
use p2p_chain_lib_network::{peer_score::PeerScoreHandle, service::NetworkHandle};
use p2p_chain_lib_ports::{NetworkEvent, PeerReputationPort};
use p2p_chain_lib_protocol::{messages::ChainTip, validation::validate_transaction};
use p2p_chain_lib_storage::ChainStore;
use p2p_chain_lib_sync::SyncService;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub struct Node {
    runtime: NodeRuntime,
}

pub struct NodeRuntime {
    pub network: NetworkHandle,
    pub chain_store: ChainStore,
    pub mempool: Mempool,
    pub consensus: SimpleConsensus<ChainStore>,
    pub sync: SyncService<NetworkHandle, ChainStore, PeerScoreHandle>,
    pub peer_reputation: PeerScoreHandle,
    pub network_event_rx: mpsc::Receiver<NetworkEvent>,
    pub block_production_interval: Option<Duration>,
}

const CHAIN_TIP_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(10);

impl Node {
    pub fn new(runtime: NodeRuntime) -> Self {
        Self { runtime }
    }

    pub async fn run(self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(listen_addr = %self.runtime.network.listen_addr(), "node running");
        let result = tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                tracing::info!("node shutdown requested");
                Ok(())
            }
            result = Self::run_event_loop(self.runtime) => result,
        };
        tracing::info!("node stopped");
        result
    }

    async fn run_event_loop(runtime: NodeRuntime) -> anyhow::Result<()> {
        let mut sync_tasks = JoinSet::new();
        let sync = runtime.sync.clone();
        sync_tasks.spawn(async move { sync.run_requests().await });
        let mut block_production_ticker = runtime.block_production_interval.map(|interval| {
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval)
        });
        let mut chain_tip_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + CHAIN_TIP_REANNOUNCE_INTERVAL,
            CHAIN_TIP_REANNOUNCE_INTERVAL,
        );
        let mut network_event_rx = runtime.network_event_rx;
        loop {
            tokio::select! {
                result = sync_tasks.join_next(), if !sync_tasks.is_empty() => {
                    if let Some(result) = result {
                        result??;
                        anyhow::bail!("sync request worker stopped unexpectedly");
                    }
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
                _ = chain_tip_ticker.tick() => {
                    if let Some(tip) = runtime.chain_store.tip().await
                        && let Err(error) = runtime.network.broadcast_chain_tip(ChainTip {
                            block_hash: tip.hash,
                            height: tip.height,
                        }).await
                    {
                        tracing::warn!(?error, "failed to republish chain tip");
                    }
                }
                event = network_event_rx.recv() => {
                    let Some(event) = event else {
                        anyhow::bail!("network event stream closed unexpectedly");
                    };

                    match event {
                        NetworkEvent::Listening(address) => {
                            tracing::info!(%address, "network listening");
                        }
                        NetworkEvent::TransactionReceived { peer_id, tx } => {
                            tracing::info!(%peer_id, ?tx, "transaction received");
                            if let Err(error) = validate_transaction(&tx) {
                                if let Err(report_error) = runtime
                                    .peer_reputation
                                    .report_bad_peer(peer_id, format!("invalid transaction: {error}"))
                                    .await
                                {
                                    tracing::warn!(?report_error, "failed to report invalid transaction");
                                }
                                continue;
                            }
                            let inserted = runtime.mempool.insert(tx).await;
                            tracing::debug!(inserted, "transaction mempool insert finished");
                        }
                        NetworkEvent::BlockAnnouncementReceived {
                            peer_id,
                            announcement,
                        } => {
                            tracing::info!(
                                %peer_id,
                                block_hash = %announcement.block_hash,
                                height = announcement.height,
                                "block announcement received",
                            );
                            if let Err(error) = Self::handle_consensus_event(
                                &runtime.chain_store,
                                &runtime.network,
                                &runtime.peer_reputation,
                                &runtime.consensus,
                                ConsensusEvent::BlockAnnouncement {
                                    peer_id: peer_id.clone(),
                                    announcement: announcement.clone(),
                                },
                            ).await {
                                tracing::warn!(?error, "failed to handle block announcement through consensus");
                            }
                            // The full block is delivered through gossip. Request/response
                            // sync is reserved for a valid block whose parent is missing.
                        }
                        NetworkEvent::BlockReceived { peer_id, block } => {
                            tracing::info!(
                                %peer_id,
                                block_hash = %block.hash,
                                height = block.height,
                                "block received",
                            );
                            let mut blocks = vec![(peer_id, block)];
                            while let Some((peer_id, block)) = blocks.pop() {
                                let block_hash = block.hash.clone();
                                match Self::handle_consensus_event(
                                    &runtime.chain_store,
                                    &runtime.network,
                                    &runtime.peer_reputation,
                                    &runtime.consensus,
                                    ConsensusEvent::BlockReceived {
                                        peer_id: peer_id.clone(),
                                        block: block.clone(),
                                    },
                                ).await {
                                    Ok(ConsensusOutcome::Accepted | ConsensusOutcome::AlreadyKnown) => {
                                        if let Err(error) = runtime.sync.on_block_received(&block_hash).await {
                                            tracing::warn!(?error, "failed to clear block sync request");
                                        }
                                        blocks.extend(runtime.sync.take_children(&block_hash).await);
                                    }
                                    Ok(ConsensusOutcome::MissingParent) => {
                                        if let Err(error) = runtime.sync.on_unaccepted_block(peer_id, block).await
                                        {
                                            tracing::warn!(?error, "failed to request an unaccepted block's parent");
                                        }
                                    }
                                    Ok(ConsensusOutcome::Rejected | ConsensusOutcome::NoChange) => {}
                                    Err(error) => {
                                        tracing::warn!(?error, "failed to handle received block through consensus");
                                        if let Err(error) = runtime.sync.on_block_request_failed(&block_hash).await {
                                            tracing::warn!(?error, "failed to clear failed block sync request");
                                        }
                                    }
                                }
                            }
                        }
                        NetworkEvent::ChainTipReceived { peer_id, tip } => {
                            tracing::info!(
                                %peer_id,
                                block_hash = %tip.block_hash,
                                height = tip.height,
                                "chain tip received",
                            );
                            if let Err(error) = runtime.sync.on_chain_tip(peer_id, tip).await {
                                tracing::warn!(?error, "failed to enqueue chain-tip sync");
                            }
                        }
                        NetworkEvent::BlockNotFound {
                            peer_id,
                            block_hash,
                        } => {
                            tracing::debug!(%peer_id, %block_hash, "requested block was not found");
                            if let Err(error) = runtime.sync.on_block_received(&block_hash).await {
                                tracing::warn!(?error, "failed to clear missing block sync request");
                            }
                        }
                        NetworkEvent::BlockRequestFailed {
                            peer_id,
                            block_hash,
                        } => {
                            tracing::debug!(%peer_id, %block_hash, "block request failed");
                            if let Err(error) = runtime.sync.on_block_request_failed(&block_hash).await {
                                tracing::warn!(?error, "failed to clear failed block sync request");
                            }
                        }
                        NetworkEvent::BlockRequestReceived { peer_id, block_hash } => {
                            tracing::info!(%peer_id, block_hash = %block_hash, "block request received");
                            match runtime.chain_store.get(&block_hash).await {
                                Some(block) => {
                                    if let Err(error) = runtime.network.send_block(peer_id, block).await {
                                        tracing::warn!(?error, "failed to send requested block");
                                    }
                                }
                                None => {
                                    tracing::debug!(block_hash = %block_hash, "requested block not found");
                                    if let Err(error) = runtime.network.send_block_not_found(peer_id, block_hash).await {
                                        tracing::warn!(?error, "failed to send block-not-found response");
                                    }
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
    }

    async fn handle_consensus_event(
        chain_store: &ChainStore,
        network: &NetworkHandle,
        peer_reputation: &PeerScoreHandle,
        consensus: &SimpleConsensus<ChainStore>,
        event: ConsensusEvent,
    ) -> anyhow::Result<ConsensusOutcome> {
        let decision = consensus.handle_event(event).await?;
        for command in decision.commands {
            match command {
                ConsensusCommand::StoreBlock(block) => {
                    let block_hash = block.hash.clone();
                    let height = block.height;
                    let producer = block.producer.clone();
                    let inserted = chain_store.insert(block).await?;
                    if inserted {
                        tracing::info!(
                            %block_hash,
                            height,
                            %producer,
                            "chain advanced",
                        );
                    }
                }
                ConsensusCommand::BroadcastBlock(block) => {
                    tracing::info!(
                        block_hash = %block.hash,
                        height = block.height,
                        "block produced",
                    );
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

        Ok(decision.outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p_chain_lib_consensus::ConsensusCommand;
    use p2p_chain_lib_network::{
        PeerAddress,
        peer_score::{PeerScoreCommand, PeerScoreHandle},
        service::{NetworkConfig, NetworkService},
    };
    use p2p_chain_lib_ports::PeerId;
    use tokio::{sync::mpsc, time::timeout};

    fn lifecycle_node() -> anyhow::Result<(Node, NetworkService)> {
        let (event_tx, event_rx) = mpsc::channel(8);
        let (score_tx, _score_rx) = mpsc::channel(8);
        let (_score_event_tx, score_event_rx) = mpsc::channel(8);
        let reputation = PeerScoreHandle::new(score_tx);
        let (service, network) = NetworkService::new(
            NetworkConfig {
                listen_addr: PeerAddress::from("/ip4/127.0.0.1/tcp/0"),
                bootstrap_peers: Vec::new(),
                node_key_seed: Some("lifecycle-test".into()),
                validator_id: None,
                validators: Vec::new(),
                max_gossip_events_per_second: 64,
                max_block_requests_per_second: 16,
            },
            event_tx,
            reputation.clone(),
            score_event_rx,
        )?;
        let store = ChainStore::new();
        let node = Node::new(NodeRuntime {
            network: network.clone(),
            chain_store: store.clone(),
            mempool: Mempool::new(),
            consensus: SimpleConsensus::new("lifecycle-test", store.clone()),
            sync: SyncService::new(network, store, reputation.clone()),
            peer_reputation: reputation,
            network_event_rx: event_rx,
            block_production_interval: None,
        });
        Ok((node, service))
    }

    #[test]
    fn construction_does_not_require_a_tokio_runtime() -> anyhow::Result<()> {
        let (node, service) = lifecycle_node()?;
        drop(node);
        drop(service);
        Ok(())
    }

    #[tokio::test]
    async fn reports_closed_event_stream_without_waiting_for_cancellation() -> anyhow::Result<()> {
        let (node, service) = lifecycle_node()?;
        drop(service);
        let token = CancellationToken::new();
        let error = timeout(Duration::from_secs(1), node.run(token.clone()))
            .await?
            .expect_err("closed network stream must stop the node");
        assert!(error.to_string().contains("network event stream closed"));
        assert!(!token.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn run_token_stops_the_event_loop() -> anyhow::Result<()> {
        let (node, _service) = lifecycle_node()?;
        let token = CancellationToken::new();
        let mut running = Box::pin(node.run(token.clone()));
        assert!(
            timeout(Duration::from_millis(10), &mut running)
                .await
                .is_err()
        );
        token.cancel();
        timeout(Duration::from_secs(1), running).await??;
        Ok(())
    }

    #[tokio::test]
    async fn event_loop_panic_reaches_the_supervising_task() -> anyhow::Result<()> {
        let (mut node, _service) = lifecycle_node()?;
        // Bypass CLI validation to trigger the timer's zero-interval panic.
        node.runtime.block_production_interval = Some(Duration::ZERO);
        let task = tokio::spawn(node.run(CancellationToken::new()));
        let error = timeout(Duration::from_secs(1), task)
            .await?
            .expect_err("event-loop panic must reach its owner");
        assert!(error.is_panic());
        Ok(())
    }

    #[tokio::test]
    async fn empty_node_rebuilds_a_chain_from_tip_first_blocks() -> anyhow::Result<()> {
        let source_store = ChainStore::new();
        let source_consensus = SimpleConsensus::new("node-a", source_store.clone());
        let mut blocks = Vec::new();
        for _ in 0..20 {
            let commands = source_consensus.handle_event(ConsensusEvent::Tick).await?;
            let block = commands
                .commands
                .into_iter()
                .find_map(|command| match command {
                    ConsensusCommand::StoreBlock(block) => Some(block),
                    _ => None,
                })
                .ok_or_else(|| anyhow::anyhow!("expected produced block"))?;
            source_store.insert(block.clone()).await?;
            blocks.push(block);
        }

        let (network_event_tx, network_event_rx) = mpsc::channel(8);
        let (peer_score_command_tx, mut peer_score_command_rx) = mpsc::channel(64);
        let (_peer_score_event_tx, peer_score_event_rx) = mpsc::channel(8);
        let peer_score = PeerScoreHandle::new(peer_score_command_tx);
        let (_network_service, network) = NetworkService::new(
            NetworkConfig {
                listen_addr: PeerAddress::from("/ip4/127.0.0.1/tcp/0"),
                bootstrap_peers: Vec::new(),
                node_key_seed: Some("node-a".to_string()),
                validator_id: None,
                validators: Vec::new(),
                max_gossip_events_per_second: 64,
                max_block_requests_per_second: 16,
            },
            network_event_tx.clone(),
            peer_score.clone(),
            peer_score_event_rx,
        )?;
        let chain_store = ChainStore::new();
        let cancellation_token = CancellationToken::new();
        let node = Node::new(NodeRuntime {
            network: network.clone(),
            chain_store: chain_store.clone(),
            mempool: Mempool::new(),
            consensus: SimpleConsensus::new("node-a", chain_store.clone()),
            sync: SyncService::new(network, chain_store.clone(), peer_score.clone()),
            peer_reputation: peer_score,
            network_event_rx,
            block_production_interval: None,
        });
        let node_task = tokio::spawn(node.run(cancellation_token.clone()));

        for block in blocks.iter().rev() {
            network_event_tx
                .send(NetworkEvent::BlockReceived {
                    peer_id: PeerId::from("peer-a"),
                    block: block.clone(),
                })
                .await?;
            tokio::task::yield_now().await;
        }

        timeout(std::time::Duration::from_secs(5), async {
            while chain_store.len().await != blocks.len() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("node did not rebuild the announced chain"))?;
        assert_eq!(chain_store.tip().await, blocks.last().cloned());

        cancellation_token.cancel();
        node_task.await??;
        while let Ok(command) = peer_score_command_rx.try_recv() {
            assert!(
                !matches!(command, PeerScoreCommand::BadMessage { .. }),
                "valid reverse-order history must not penalize its sender: {command:?}"
            );
        }
        Ok(())
    }
}
