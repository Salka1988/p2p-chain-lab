use async_trait::async_trait;
use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_ports::{BlockRequestPort, PeerId, PeerReputationPort};
use p2p_chain_lib_protocol::messages::BlockAnnouncement;

#[async_trait]
pub trait SyncPort: Clone + Send + Sync + 'static {
    async fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SyncService<N, C, R> {
    network: N,
    chain_store: C,
    peer_reputation: R,
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
        }
    }

    pub async fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> anyhow::Result<()> {
        tracing::debug!(%peer_id, ?announcement, "handling block announcement");

        if self
            .chain_store
            .get_block(&announcement.block_hash)
            .await
            .is_none()
        {
            if let Err(error) = self
                .network
                .request_block(peer_id.clone(), announcement.block_hash)
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
        } else if let Err(error) = self
            .peer_reputation
            .report_good_peer(peer_id, "announced already-known block".to_string())
            .await
        {
            tracing::warn!(?error, "failed to report already-known block announcement");
        }

        Ok(())
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
}
