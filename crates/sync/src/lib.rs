use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use p2p_chain_lib_chain::block::BlockHash;
use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_ports::{BlockRequestPort, PeerId, PeerReputationPort};
use p2p_chain_lib_protocol::{
    messages::BlockAnnouncement, validation::validate_block_announcement,
};
use tokio::sync::Mutex;

#[async_trait]
pub trait SyncPort: Clone + Send + Sync + 'static {
    async fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> anyhow::Result<()>;

    async fn on_block_received(&self, block_hash: &BlockHash) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SyncService<N, C, R> {
    network: N,
    chain_store: C,
    peer_reputation: R,
    pending_blocks: Arc<Mutex<HashSet<BlockHash>>>,
}

impl<N, C, R> SyncService<N, C, R>
where
    N: BlockRequestPort,
    C: BlockStorePort,
    R: PeerReputationPort,
{
    pub fn new(network: N, chain_store: C, peer_reputation: R) -> Self {
        Self {
            network,
            chain_store,
            peer_reputation,
            pending_blocks: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            %peer_id,
            block_hash = %announcement.block_hash,
            height = announcement.height,
            "handling block announcement",
        );

        if let Err(error) = validate_block_announcement(&announcement) {
            self.peer_reputation
                .report_bad_peer(
                    peer_id,
                    format!("invalid block announcement for sync: {error}"),
                )
                .await?;
            return Ok(());
        }

        if self
            .chain_store
            .get_block(&announcement.block_hash)
            .await
            .is_none()
        {
            if !self.mark_pending(announcement.block_hash.clone()).await {
                tracing::debug!(
                    block_hash = %announcement.block_hash,
                    height = announcement.height,
                    "block sync request already pending",
                );
                return Ok(());
            }

            if let Err(error) = self
                .network
                .request_block(peer_id.clone(), announcement.block_hash.clone())
                .await
            {
                if let Err(report_error) = self
                    .peer_reputation
                    .report_bad_peer(
                        peer_id,
                        format!("failed to request announced block: {error}"),
                    )
                    .await
                {
                    tracing::warn!(?report_error, "failed to report block request failure");
                }
                return Err(error);
            }
            tracing::info!(
                %peer_id,
                block_hash = %announcement.block_hash,
                height = announcement.height,
                "block sync requested",
            );
        } else if let Err(error) = self
            .peer_reputation
            .report_good_peer(peer_id, "announced already-known block".to_string())
            .await
        {
            tracing::warn!(?error, "failed to report already-known block announcement");
        }

        Ok(())
    }

    pub async fn on_block_received(&self, block_hash: &BlockHash) -> anyhow::Result<()> {
        self.pending_blocks.lock().await.remove(block_hash);
        Ok(())
    }

    async fn mark_pending(&self, block_hash: BlockHash) -> bool {
        self.pending_blocks.lock().await.insert(block_hash)
    }
}

#[async_trait]
impl<N, C, R> SyncPort for SyncService<N, C, R>
where
    N: BlockRequestPort,
    C: BlockStorePort,
    R: PeerReputationPort,
{
    async fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> anyhow::Result<()> {
        self.on_block_announcement(peer_id, announcement).await
    }

    async fn on_block_received(&self, block_hash: &BlockHash) -> anyhow::Result<()> {
        self.on_block_received(block_hash).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use p2p_chain_lib_chain::block::{Block, BlockHash};
    use p2p_chain_lib_chain::store::ChainStore;
    use p2p_chain_lib_ports::PeerId;

    #[derive(Clone)]
    struct RecordingNetwork {
        request_tx: mpsc::Sender<(PeerId, BlockHash)>,
    }

    #[async_trait]
    impl BlockRequestPort for RecordingNetwork {
        async fn request_block(
            &self,
            peer_id: PeerId,
            block_hash: BlockHash,
        ) -> anyhow::Result<()> {
            self.request_tx.send((peer_id, block_hash)).await?;
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingReputation {
        reports: Arc<tokio::sync::Mutex<Vec<(PeerId, String, bool)>>>,
    }

    #[async_trait]
    impl PeerReputationPort for RecordingReputation {
        async fn report_good_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()> {
            self.reports.lock().await.push((peer_id, reason, true));
            Ok(())
        }

        async fn report_bad_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()> {
            self.reports.lock().await.push((peer_id, reason, false));
            Ok(())
        }
    }

    fn announcement(hash: u8) -> BlockAnnouncement {
        BlockAnnouncement {
            block_hash: BlockHash::from_u8(hash),
            height: 1,
        }
    }

    fn block(hash: u8) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: None,
            height: 1,
            body: vec![1],
        }
    }

    #[tokio::test]
    async fn requests_missing_announced_block() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;

        assert!(matches!(
            request_rx.recv().await,
            Some((peer_id, block_hash))
                if peer_id == PeerId::from("peer-a") && block_hash == BlockHash::from_u8(1)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn skips_known_announced_block() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let store = ChainStore::new();
        store.insert(block(1)).await;
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            store,
            RecordingReputation::default(),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;

        assert!(request_rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn skips_duplicate_pending_announced_block() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;
        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        assert!(request_rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn received_block_clears_pending_request() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;
        sync.on_block_received(&BlockHash::from_u8(1)).await?;
        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        assert!(request_rx.recv().await.is_some());
        Ok(())
    }
}
