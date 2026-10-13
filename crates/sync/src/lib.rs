use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_chain::store::BlockStorePort;
use p2p_chain_lib_ports::{NetworkHandle, PeerId, PeerReputationHandle};
use p2p_chain_lib_protocol::{
    messages::ChainTip,
    validation::{validate_block, validate_chain_tip},
};
use tokio::sync::{Mutex, Notify};

const PENDING_BLOCK_TIMEOUT: Duration = Duration::from_secs(10);
// Leave headroom below the network's default 16 requests/second ingress limit.
// shortcut: peers with stricter limits need negotiated request pacing before use.
const BLOCK_REQUEST_INTERVAL: Duration = Duration::from_millis(125);
const CHAIN_TIP_SYNC_DELAY: Duration = Duration::from_millis(250);
const MAX_PENDING_REQUESTS: usize = 128;
const MAX_TIP_REQUESTS: usize = 64;
const MAX_ORPHAN_BLOCKS: usize = 1024;
const MAX_ORPHAN_BLOCK_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct SyncService {
    network: NetworkHandle,
    chain_store: Arc<dyn BlockStorePort>,
    peer_reputation: PeerReputationHandle,
    pending_blocks: Arc<Mutex<HashMap<BlockHash, PendingRequest>>>,
    orphan_blocks: Arc<Mutex<OrphanBlocks>>,
    request_worker: Arc<Mutex<()>>,
    requests_changed: Arc<Notify>,
}

#[derive(Debug)]
struct PendingRequest {
    peer_id: PeerId,
    queued_at: tokio::time::Instant,
    ready_at: tokio::time::Instant,
    sent_at: Option<tokio::time::Instant>,
    parent: bool,
}

#[derive(Debug, Default)]
struct OrphanBlocks {
    blocks: HashMap<BlockHash, (PeerId, Block)>,
    insertion_order: VecDeque<BlockHash>,
    total_bytes: usize,
}

impl SyncService {
    pub fn new(
        network: NetworkHandle,
        chain_store: Arc<dyn BlockStorePort>,
        peer_reputation: PeerReputationHandle,
    ) -> Self {
        Self {
            network,
            chain_store,
            peer_reputation,
            pending_blocks: Arc::new(Mutex::new(HashMap::new())),
            orphan_blocks: Arc::new(Mutex::new(OrphanBlocks::default())),
            request_worker: Arc::new(Mutex::new(())),
            requests_changed: Arc::new(Notify::new()),
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
            self.enqueue_request(peer_id, tip.block_hash, false).await;
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
        self.enqueue_request(peer_id, parent_hash, true).await;
        Ok(())
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

    /// Drive queued requests alongside the node event loop. Dropping this future
    /// stops dispatch; handlers only enqueue bounded work and never wait for pacing.
    /// When idle, wait for work; timers exist only for queued work or expiry.
    pub async fn run_requests(&self) -> anyhow::Result<()> {
        let _worker = self
            .request_worker
            .try_lock()
            .map_err(|_| anyhow::anyhow!("sync request worker already running"))?;
        let mut next_send = tokio::time::Instant::now();
        loop {
            let deadline = self.next_deadline(next_send).await;
            tokio::select! {
                _ = self.requests_changed.notified() => {}
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if tokio::time::Instant::now() >= next_send && self.dispatch_request().await {
                        next_send = tokio::time::Instant::now() + BLOCK_REQUEST_INTERVAL;
                    }
                }
            }
        }
    }

    async fn next_deadline(&self, next_send: tokio::time::Instant) -> Option<tokio::time::Instant> {
        let now = tokio::time::Instant::now();
        let mut pending = self.pending_blocks.lock().await;
        pending.retain(|_, request| {
            now.duration_since(request.sent_at.unwrap_or(request.queued_at)) < PENDING_BLOCK_TIMEOUT
        });
        pending
            .values()
            .map(|request| {
                let expiry = request.sent_at.unwrap_or(request.queued_at) + PENDING_BLOCK_TIMEOUT;
                if request.sent_at.is_some() {
                    expiry
                } else {
                    expiry.min(request.ready_at.max(next_send))
                }
            })
            .min()
    }

    async fn dispatch_request(&self) -> bool {
        let now = tokio::time::Instant::now();
        let selected = {
            let mut pending = self.pending_blocks.lock().await;
            pending.retain(|_, request| {
                now.duration_since(request.sent_at.unwrap_or(request.queued_at))
                    < PENDING_BLOCK_TIMEOUT
            });
            let hash = pending
                .iter()
                .filter(|(_, request)| request.sent_at.is_none() && request.ready_at <= now)
                .min_by_key(|(_, request)| (!request.parent, request.queued_at))
                .map(|(hash, _)| hash.clone());
            hash.map(|hash| {
                let request = pending.get_mut(&hash).expect("selected pending request");
                request.sent_at = Some(now);
                (hash, request.peer_id.clone())
            })
        };
        let Some((hash, peer_id)) = selected else {
            return false;
        };
        if self.chain_store.get_block(&hash).await.is_some() {
            self.clear_pending(&hash).await;
            return false;
        }
        let result = tokio::time::timeout(
            BLOCK_REQUEST_INTERVAL,
            self.network.request_block(peer_id.clone(), hash.clone()),
        )
        .await;
        if !matches!(result, Ok(Ok(()))) {
            // Local queue pressure is not evidence of remote peer misconduct.
            tracing::warn!(%peer_id, block_hash = %hash, ?result, "failed to dispatch sync request");
            self.clear_pending(&hash).await;
        }
        true
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

    async fn enqueue_request(&self, peer_id: PeerId, mut block_hash: BlockHash, mut parent: bool) {
        // Follow downloaded orphans to the missing ancestor instead of repeatedly
        // fetching the same suffix. This also permits retry after request expiry.
        {
            let orphans = self.orphan_blocks.lock().await;
            for depth in 0..=MAX_ORPHAN_BLOCKS {
                let Some((_, block)) = orphans.blocks.get(&block_hash) else {
                    break;
                };
                if depth == MAX_ORPHAN_BLOCKS {
                    return;
                }
                let Some(hash) = &block.parent_hash else {
                    return;
                };
                block_hash = hash.clone();
                parent = true;
            }
        }
        if self.chain_store.get_block(&block_hash).await.is_some() {
            return;
        }
        let now = tokio::time::Instant::now();
        let mut pending_blocks = self.pending_blocks.lock().await;
        pending_blocks.retain(|_, request| {
            now.duration_since(request.sent_at.unwrap_or(request.queued_at)) < PENDING_BLOCK_TIMEOUT
        });
        if let Some(request) = pending_blocks.get_mut(&block_hash) {
            if parent && !request.parent {
                request.parent = true;
                request.ready_at = now;
                request.peer_id = peer_id;
                self.requests_changed.notify_one();
            }
            return;
        }
        // Reserve half the capacity for ancestry, so tip floods cannot block catch-up.
        if pending_blocks.len() >= MAX_PENDING_REQUESTS
            || (!parent
                && pending_blocks
                    .values()
                    .filter(|request| !request.parent)
                    .count()
                    >= MAX_TIP_REQUESTS)
        {
            tracing::debug!(%block_hash, "sync request queue full; await a later announcement");
            return;
        }
        pending_blocks.insert(
            block_hash,
            PendingRequest {
                peer_id,
                queued_at: now,
                ready_at: if parent {
                    now
                } else {
                    now + CHAIN_TIP_SYNC_DELAY
                },
                sent_at: None,
                parent,
            },
        );
        self.requests_changed.notify_one();
    }

    async fn clear_pending(&self, block_hash: &BlockHash) {
        if self
            .pending_blocks
            .lock()
            .await
            .remove(block_hash)
            .is_some()
        {
            self.requests_changed.notify_one();
        }
    }
}

fn block_size(block: &Block) -> usize {
    block.body.len() + block.producer.len() + block.signature.len() + 72
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use p2p_chain_lib_chain::block::{Block, BlockHash};
    use p2p_chain_lib_ports::{NetworkCommand, PeerId};
    use p2p_chain_lib_storage::ChainStore;

    fn reputation() -> PeerReputationHandle {
        let (tx, _rx) = mpsc::channel(8);
        PeerReputationHandle::new(tx)
    }

    fn request(command: NetworkCommand) -> (PeerId, BlockHash) {
        match command {
            NetworkCommand::RequestBlock {
                peer_id,
                block_hash,
            } => (peer_id, block_hash),
            other => panic!("unexpected command: {other:?}"),
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

    #[tokio::test(start_paused = true)]
    async fn idle_worker_has_no_timer_and_wakes_for_new_work() -> anyhow::Result<()> {
        use std::{
            future::Future,
            sync::atomic::{AtomicUsize, Ordering},
        };
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let worker_polls = polls.clone();
        let worker = sync.clone();
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(async move {
            let future = worker.run_requests();
            tokio::pin!(future);
            std::future::poll_fn(|cx| {
                worker_polls.fetch_add(1, Ordering::SeqCst);
                future.as_mut().poll(cx)
            })
            .await
        });
        tokio::task::yield_now().await;
        let idle_polls = polls.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), idle_polls);
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        assert_eq!(
            request_rx.recv().await.map(request).unwrap().1,
            BlockHash::from_u8(1)
        );
        tokio::time::advance(PENDING_BLOCK_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(sync.pending_blocks.lock().await.is_empty());
        let idle_polls = polls.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), idle_polls);
        workers.shutdown().await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn parent_promotion_wakes_worker_before_tip_deadline() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(2))
            .await?;
        let worker = sync.clone();
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(async move { worker.run_requests().await });
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        sync.on_unaccepted_block(PeerId::from("peer-b"), child_block(3, 2, 3))
            .await?;
        assert_eq!(
            request_rx.recv().await.map(request).unwrap(),
            (PeerId::from("peer-b"), BlockHash::from_u8(2))
        );
        assert!(started.elapsed() < CHAIN_TIP_SYNC_DELAY);
        sync.on_block_received(&BlockHash::from_u8(2)).await?;
        assert!(sync.next_deadline(started).await.is_none());
        workers.shutdown().await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn paces_tip_and_parent_requests_across_clones() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(3))
            .await?;
        let mut workers = tokio::task::JoinSet::new();
        let worker = sync.clone();
        workers.spawn(async move { worker.run_requests().await });
        let started = tokio::time::Instant::now();
        assert!(request_rx.recv().await.map(request).is_some());
        assert!(started.elapsed() >= CHAIN_TIP_SYNC_DELAY);
        let sent = tokio::time::Instant::now();
        sync.clone()
            .on_unaccepted_block(PeerId::from("peer-a"), child_block(3, 2, 3))
            .await?;
        assert!(request_rx.recv().await.map(request).is_some());
        assert!(sent.elapsed() >= BLOCK_REQUEST_INTERVAL);
        assert!(sync.run_requests().await.is_err());
        workers.shutdown().await;
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(4))
            .await?;
        tokio::time::advance(PENDING_BLOCK_TIMEOUT).await;
        assert!(request_rx.try_recv().map(request).is_err());
        assert!(sync.request_worker.try_lock().is_ok());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn requests_missing_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(matches!(
            request_rx.recv().await.map(request),
            Some((peer_id, block_hash))
                if peer_id == PeerId::from("peer-a") && block_hash == BlockHash::from_u8(1)
        ));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn skips_known_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let store = ChainStore::new();
        store.insert(block(1)).await?;
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(store),
            reputation(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.try_recv().map(request).is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn skips_duplicate_pending_advertised_tip() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        sync.dispatch_request().await;
        assert!(request_rx.recv().await.map(request).is_some());
        assert!(request_rx.try_recv().map(request).is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn received_block_clears_pending_request() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_block_received(&BlockHash::from_u8(1)).await?;
        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.try_recv().map(request).is_err());
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.recv().await.map(request).is_some());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retries_after_request_failure() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        let hash = BlockHash::from_u8(1);

        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        sync.on_block_request_failed(&hash).await?;
        sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.recv().await.map(request).is_some());
        assert!(request_rx.recv().await.map(request).is_some());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retries_after_pending_request_times_out() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(2))
            .await?;
        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.recv().await.map(request).is_some());
        tokio::time::advance(PENDING_BLOCK_TIMEOUT).await;
        sync.dispatch_request().await;
        assert!(sync.pending_blocks.lock().await.is_empty());
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(1))
            .await?;

        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        assert!(request_rx.recv().await.map(request).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn requests_a_missing_parent_and_releases_its_cached_child() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        let child = child_block(2, 1, 2);

        sync.on_unaccepted_block(PeerId::from("peer-a"), child.clone())
            .await?;

        sync.dispatch_request().await;
        assert!(matches!(
            request_rx.recv().await.map(request),
            Some((peer_id, hash)) if peer_id == PeerId::from("peer-a") && hash == BlockHash::from_u8(1)
        ));
        assert_eq!(
            sync.take_children(&BlockHash::from_u8(1)).await,
            vec![(PeerId::from("peer-a"), child)]
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn bounds_tip_floods_and_reserves_priority_for_parents() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let (score_tx, score_rx) = mpsc::channel(8);
        let reputation = PeerReputationHandle::new(score_tx);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation.clone(),
        );
        for hash in 1..201 {
            sync.clone()
                .on_chain_tip(PeerId::from("peer-a"), chain_tip(hash))
                .await?;
        }
        assert_eq!(sync.pending_blocks.lock().await.len(), MAX_TIP_REQUESTS);
        for hash in 1..65 {
            sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(hash))
                .await?;
        }
        assert_eq!(sync.pending_blocks.lock().await.len(), MAX_TIP_REQUESTS);
        for hash in 100..200 {
            sync.enqueue_request(PeerId::from("peer-b"), BlockHash::from_u8(hash), true)
                .await;
        }
        assert_eq!(sync.pending_blocks.lock().await.len(), MAX_PENDING_REQUESTS);
        tokio::time::advance(CHAIN_TIP_SYNC_DELAY).await;
        sync.dispatch_request().await;
        let (peer, _) = request_rx.try_recv().map(request)?;
        assert_eq!(peer, PeerId::from("peer-b"));
        assert!(score_rx.is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_transport_does_not_block_enqueue_or_penalize_peers() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(1);
        request_tx
            .send(NetworkCommand::RequestBlock {
                peer_id: PeerId::from("occupied"),
                block_hash: BlockHash::from_u8(0),
            })
            .await?;
        let (score_tx, score_rx) = mpsc::channel(8);
        let reputation = PeerReputationHandle::new(score_tx);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation.clone(),
        );
        // Neither handler waits for a free network slot or the pacing timer.
        let started = tokio::time::Instant::now();
        sync.on_chain_tip(PeerId::from("peer-a"), chain_tip(3))
            .await?;
        sync.on_unaccepted_block(PeerId::from("peer-a"), child_block(3, 2, 3))
            .await?;
        assert_eq!(started.elapsed(), Duration::ZERO);
        sync.dispatch_request().await;
        assert!(
            !sync
                .pending_blocks
                .lock()
                .await
                .contains_key(&BlockHash::from_u8(2))
        );
        assert!(score_rx.is_empty());
        request_rx.try_recv().map(request)?;
        sync.on_unaccepted_block(PeerId::from("peer-b"), child_block(3, 2, 3))
            .await?;
        sync.dispatch_request().await;
        assert_eq!(
            request_rx.try_recv().map(request)?,
            (PeerId::from("peer-b"), BlockHash::from_u8(2))
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn cached_history_is_not_requested_again_and_missing_ancestry_can_retry()
    -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let sync = SyncService::new(
            NetworkHandle::new(request_tx),
            Arc::new(ChainStore::new()),
            reputation(),
        );
        sync.on_unaccepted_block(PeerId::from("peer-a"), child_block(3, 2, 3))
            .await?;
        sync.on_unaccepted_block(PeerId::from("peer-a"), child_block(2, 1, 2))
            .await?;
        sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(3))
            .await?;
        assert_eq!(sync.pending_blocks.lock().await.len(), 1);
        sync.dispatch_request().await;
        assert_eq!(request_rx.try_recv().map(request)?.1, BlockHash::from_u8(1));
        tokio::time::advance(PENDING_BLOCK_TIMEOUT).await;
        sync.on_chain_tip(PeerId::from("peer-b"), chain_tip(3))
            .await?;
        sync.dispatch_request().await;
        assert_eq!(
            request_rx.try_recv().map(request)?,
            (PeerId::from("peer-b"), BlockHash::from_u8(1))
        );
        assert!(request_rx.try_recv().map(request).is_err());
        Ok(())
    }
}
