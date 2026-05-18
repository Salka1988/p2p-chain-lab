use std::future::Future;

use libp2p_mini_chain_chain::store::BlockStorePort;
use libp2p_mini_chain_network::{PeerId, service::BlockRequestPort};
use libp2p_mini_chain_protocol::messages::BlockAnnouncement;

pub trait SyncPort: Clone + Send + Sync + 'static {
    fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_;
}

#[derive(Debug, Clone)]
pub struct SyncService<N, C> {
    network: N,
    chain_store: C,
}

impl<N, C> SyncService<N, C>
where
    N: BlockRequestPort,
    C: BlockStorePort,
{
    pub fn new(network: N, chain_store: C) -> Self {
        Self {
            network,
            chain_store,
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
            self.network
                .request_block(peer_id, announcement.block_hash)
                .await?;
        }

        Ok(())
    }
}

impl<N, C> SyncPort for SyncService<N, C>
where
    N: BlockRequestPort,
    C: BlockStorePort,
{
    fn on_block_announcement(
        &self,
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_ {
        self.on_block_announcement(peer_id, announcement)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use libp2p_mini_chain_chain::block::{Block, BlockHash};
    use libp2p_mini_chain_chain::store::ChainStore;
    use libp2p_mini_chain_network::command::NetworkCommand;
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
        let sync = SyncService::new(NetworkHandle::new(command_tx), ChainStore::new());

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
        let store = ChainStore::new();
        store.insert(block(1)).await;
        let sync = SyncService::new(NetworkHandle::new(command_tx), store);

        sync.on_block_announcement(PeerId::from("peer-a"), announcement(1))
            .await
            .unwrap();

        assert!(command_rx.try_recv().is_err());
    }
}
