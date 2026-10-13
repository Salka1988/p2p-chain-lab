use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_ports::{BlockRequestPort, PeerId, PeerReputationPort};
use p2p_chain_lib_protocol::{
    messages::ChainTip,
    validation::{validate_block, validate_chain_tip},
};
use tokio::sync::Mutex;

const PENDING_BLOCK_TIMEOUT: Duration = Duration::from_secs(10);
// Leave headroom below the network's default 16 requests/second ingress limit.
// shortcut: peers with stricter limits need negotiated request pacing before use.
const BLOCK_REQUEST_INTERVAL: Duration = Duration::from_millis(125);
const MAX_ORPHAN_BLOCKS: usize = 1024;
const MAX_ORPHAN_BLOCK_BYTES: usize = 16 * 1024 * 1024;

#[async_trait]
pub trait SyncPort: Clone + Send + Sync + 'static {
    async fn on_chain_tip(&self, peer_id: PeerId, tip: ChainTip) -> anyhow::Result<()>;

    async fn on_block_received(&self, block_hash: &BlockHash) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SyncService<N, C, R> {
    network: N,
    chain_store: C,
    peer_reputation: R,
    pending_blocks: Arc<Mutex<HashMap<BlockHash, tokio::time::Instant>>>,
    orphan_blocks: Arc<Mutex<OrphanBlocks>>,
    next_block_request: Arc<Mutex<tokio::time::Instant>>,
}

#[derive(Debug, Default)]
struct OrphanBlocks {
    blocks: HashMap<BlockHash, (PeerId, Block)>,
    insertion_order: VecDeque<BlockHash>,
    total_bytes: usize,
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
            pending_blocks: Arc::new(Mutex::new(HashMap::new())),
            orphan_blocks: Arc::new(Mutex::new(OrphanBlocks::default())),
            next_block_request: Arc::new(Mutex::new(tokio::time::Instant::now())),
        }
    }

    pub async fn on_chain_tip(&self, peer_id: PeerId, tip: ChainTip) -> anyhow::Result<()> {
        tracing::debug!(
            %peer_id,
            block_hash = %tip.block_hash,
            height = tip.height,
            "handling chain tip",
        );

        if let Err(error) = validate_chain_tip(&tip) {
            self.peer_reputation
                .report_bad_peer(peer_id, format!("invalid chain tip for sync: {error}"))
                .await?;
            return Ok(());
        }

        if self.chain_store.get_block(&tip.block_hash).await.is_none() {
            if !self.mark_pending(tip.block_hash.clone()).await {
                tracing::debug!(
                    block_hash = %tip.block_hash,
                    height = tip.height,
                    "block sync request already pending",
                );
                return Ok(());
            }

            if let Err(error) = self
                .request_block(peer_id.clone(), tip.block_hash.clone())
                .await
            {
                if let Err(report_error) = self
                    .peer_reputation
                    .report_bad_peer(
                        peer_id,
                        format!("failed to request advertised chain tip: {error}"),
                    )
                    .await
                {
                    tracing::warn!(?report_error, "failed to report block request failure");
                }
                self.clear_pending(&tip.block_hash).await;
                return Err(error);
            }
            tracing::info!(
                %peer_id,
                block_hash = %tip.block_hash,
                height = tip.height,
                "block sync requested",
            );
        } else if let Err(error) = self
            .peer_reputation
            .report_good_peer(peer_id, "advertised already-known chain tip".to_string())
            .await
        {
            tracing::warn!(?error, "failed to report already-known chain tip");
        }

        Ok(())
    }

    pub async fn on_block_received(&self, block_hash: &BlockHash) -> anyhow::Result<()> {
        self.clear_pending(block_hash).await;
        Ok(())
    }

    pub async fn on_block_request_failed(&self, block_hash: &BlockHash) -> anyhow::Result<()> {
        self.clear_pending(block_hash).await;
        Ok(())
    }

    pub async fn on_unaccepted_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()> {
        self.clear_pending(&block.hash).await;
        if validate_block(&block).is_err() {
            return Ok(());
        }
        let Some(parent_hash) = block.parent_hash.clone() else {
            return Ok(());
        };
        if self.chain_store.get_block(&parent_hash).await.is_some() {
            return Ok(());
        }
        if !self.cache_orphan(peer_id.clone(), block).await {
            return Ok(());
        }
        self.request_missing_block(peer_id, parent_hash).await
    }

    pub async fn take_children(&self, parent_hash: &BlockHash) -> Vec<(PeerId, Block)> {
        let mut orphans = self.orphan_blocks.lock().await;
        let child_hashes = orphans
            .blocks
            .iter()
            .filter_map(|(hash, (_, block))| {
                (block.parent_hash.as_ref() == Some(parent_hash)).then_some(hash.clone())
            })
            .collect::<Vec<_>>();
        let mut children = Vec::with_capacity(child_hashes.len());
        for child_hash in &child_hashes {
            if let Some((peer_id, block)) = orphans.blocks.remove(child_hash) {
                orphans.total_bytes -= block_size(&block);
                children.push((peer_id, block));
            }
        }
        orphans
            .insertion_order
            .retain(|hash| !child_hashes.contains(hash));
        children
    }

    async fn request_missing_block(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        if !self.mark_pending(block_hash.clone()).await {
            return Ok(());
        }
        if let Err(error) = self
            .request_block(peer_id.clone(), block_hash.clone())
            .await
        {
            self.clear_pending(&block_hash).await;
            self.peer_reputation
                .report_bad_peer(
                    peer_id,
                    format!("failed to request missing parent block: {error}"),
                )
                .await?;
            return Err(error);
        }
        Ok(())
    }

    async fn request_block(&self, peer_id: PeerId, block_hash: BlockHash) -> anyhow::Result<()> {
        let mut next_request = self.next_block_request.lock().await;
        tokio::time::sleep_until(*next_request).await;
        self.network.request_block(peer_id, block_hash).await?;
        *next_request = tokio::time::Instant::now() + BLOCK_REQUEST_INTERVAL;
        Ok(())
    }

    async fn cache_orphan(&self, peer_id: PeerId, block: Block) -> bool {
        let bytes = block_size(&block);
        if bytes > MAX_ORPHAN_BLOCK_BYTES {
            return false;
        }
        let mut orphans = self.orphan_blocks.lock().await;
        if orphans.blocks.contains_key(&block.hash) {
            return true;
        }
        while orphans.blocks.len() >= MAX_ORPHAN_BLOCKS
            || orphans.total_bytes.saturating_add(bytes) > MAX_ORPHAN_BLOCK_BYTES
        {
            let Some(oldest_hash) = orphans.insertion_order.pop_front() else {
                return false;
            };
            if let Some((_, oldest)) = orphans.blocks.remove(&oldest_hash) {
                orphans.total_bytes -= block_size(&oldest);
            }
        }
        orphans.total_bytes += bytes;
        orphans.insertion_order.push_back(block.hash.clone());
        orphans.blocks.insert(block.hash.clone(), (peer_id, block));
        true
    }

    async fn mark_pending(&self, block_hash: BlockHash) -> bool {
        let now = tokio::time::Instant::now();
        let mut pending_blocks = self.pending_blocks.lock().await;
        match pending_blocks.get(&block_hash) {
            Some(started_at) if now.duration_since(*started_at) < PENDING_BLOCK_TIMEOUT => false,
            _ => {
                pending_blocks.insert(block_hash, now);
                true
            }
        }
    }

    async fn clear_pending(&self, block_hash: &BlockHash) {
        self.pending_blocks.lock().await.remove(block_hash);
    }
}

fn block_size(block: &Block) -> usize {
    block.body.len() + block.producer.len() + block.signature.len() + 72
}

#[async_trait]
impl<N, C, R> SyncPort for SyncService<N, C, R>
where
    N: BlockRequestPort,
    C: BlockStorePort,
    R: PeerReputationPort,
{
    async fn on_chain_tip(&self, peer_id: PeerId, tip: ChainTip) -> anyhow::Result<()> {
        self.on_chain_tip(peer_id, tip).await
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

    fn chain_tip(hash: u8) -> ChainTip {
        ChainTip {
            block_hash: BlockHash::from_u8(hash),
            height: 1,
        }
    }

    fn block(hash: u8) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: None,
            height: 1,
            producer: "validator".to_string(),
            signature: vec![1],
            body: vec![1],
        }
    }

    fn child_block(hash: u8, parent_hash: u8, height: u64) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: Some(BlockHash::from_u8(parent_hash)),
            height,
            producer: "validator".to_string(),
            signature: vec![1],
            body: vec![1],
        }
    }

    #[tokio::test]
    async fn paces_tip_and_parent_requests_across_clones() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(3))
            .await?;
        assert!(request_rx.recv().await.is_some());
        let next_request = *sync.next_block_request.lock().await;
        sync.clone()
            .on_unaccepted_block(PeerId::from("peer-a"), child_block(3, 2, 3))
            .await?;
        assert!(request_rx.recv().await.is_some());
        assert!(tokio::time::Instant::now() >= next_request);
        Ok(())
    }

    #[tokio::test]
    async fn requests_missing_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        assert!(matches!(
            request_rx.recv().await,
            Some((peer_id, block_hash))
                if peer_id == PeerId::from("peer-a") && block_hash == BlockHash::from_u8(1)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn skips_known_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let store = ChainStore::new();
        store.insert(block(1)).await?;
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            store,
            RecordingReputation::default(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        assert!(request_rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn skips_duplicate_pending_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
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

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_block_received(&BlockHash::from_u8(1)).await?;
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        assert!(request_rx.recv().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn retries_after_request_failure() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );
        let hash = BlockHash::from_u8(1);

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_block_request_failed(&hash).await?;
        sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        assert!(request_rx.recv().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn retries_after_rejected_block_response() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );
        let hash = BlockHash::from_u8(1);

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_block_request_failed(&hash).await?;
        sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        assert!(request_rx.recv().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn retries_after_pending_request_times_out() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );
        let hash = BlockHash::from_u8(1);

        sync.mark_pending(hash.clone()).await;
        sync.pending_blocks
            .lock()
            .await
            .insert(hash, tokio::time::Instant::now() - PENDING_BLOCK_TIMEOUT);
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        assert!(request_rx.recv().await.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn requests_a_missing_parent_and_releases_its_cached_child() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            RecordingNetwork { request_tx },
            ChainStore::new(),
            RecordingReputation::default(),
        );
        let child = child_block(2, 1, 2);

        sync.on_unaccepted_block(PeerId::from("peer-a"), child.clone())
            .await?;

        assert!(matches!(
            request_rx.recv().await,
            Some((peer_id, hash)) if peer_id == PeerId::from("peer-a") && hash == BlockHash::from_u8(1)
        ));
        assert_eq!(
            sync.take_children(&BlockHash::from_u8(1)).await,
            vec![(PeerId::from("peer-a"), child)]
        );
        Ok(())
    }
}
