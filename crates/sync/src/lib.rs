use async_trait::async_trait;
use libp2p_mini_chain_chain::store::BlockStorePort;
use libp2p_mini_chain_network::{
    PeerId, peer_score::PeerReputationPort, service::BlockRequestPort,
};
use libp2p_mini_chain_protocol::messages::BlockAnnouncement;

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
    use tokio::sync::mpsc;

    use super::*;
    use libp2p_mini_chain_chain::block::{Block, BlockHash};
    use libp2p_mini_chain_chain::store::ChainStore;
    use libp2p_mini_chain_network::command::NetworkCommand;
    use libp2p_mini_chain_network::peer_score::PeerScoreHandle;
    use libp2p_mini_chain_network::service::NetworkHandle;

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
    async fn requests_missing_announced_block() {
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let (peer_score_tx, _peer_score_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(command_tx),
            ChainStore::new(),
            PeerScoreHandle::new(peer_score_tx),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await
            .unwrap();

        assert!(matches!(
            command_rx.recv().await,
            Some(NetworkCommand::RequestBlock { peer_id, block_hash })
                if peer_id == PeerId::from("peer-a") && block_hash == BlockHash::from_u8(1)
        ));
    }

    #[tokio::test]
    async fn skips_known_announced_block() {
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let (peer_score_tx, _peer_score_rx) = mpsc::channel(4);
        let store = ChainStore::new();
        store.insert(block(1)).await;
        let sync = SyncService::new(
            NetworkHandle::new(command_tx),
            store,
            PeerScoreHandle::new(peer_score_tx),
        );

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await
            .unwrap();

        assert!(command_rx.try_recv().is_err());
    }
}
