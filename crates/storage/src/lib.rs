use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use tokio::sync::Mutex;

use p2p_chain_lib_chain::{
    block::{Block, BlockHash},
    store::BlockStorePort,
};

#[derive(Debug, Clone, Default)]
pub struct ChainStore {
    state: Arc<Mutex<StoreState>>,
    persistence_path: Option<Arc<PathBuf>>,
    write_gate: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct StoreState {
    blocks: HashMap<BlockHash, Block>,
    selected_tip: Option<Block>,
}

impl ChainStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a block directory. Use clones of this handle for a single writer.
    /// Snapshot files from the old format are deliberately not migrated.
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            fs::create_dir_all(&path).with_context(|| {
                format!("chain_path must be a block directory: {}", path.display())
            })?;
            // shortcut: keep full history in memory; add indexed reads before large deployments.
            let mut blocks = HashMap::new();
            for entry in fs::read_dir(&path)? {
                let entry = entry?;
                let file = entry.path();
                // An interrupted write has not been committed until its rename.
                if file.extension().is_some_and(|ext| ext == "tmp") {
                    continue;
                }
                ensure!(
                    file.extension().is_some_and(|ext| ext == "block"),
                    "unexpected chain file: {}",
                    file.display()
                );
                let block: Block = bincode::deserialize(&fs::read(&file)?)
                    .with_context(|| format!("invalid block file: {}", file.display()))?;
                ensure!(
                    entry.file_name() == format!("{}.block", block.hash).as_str(),
                    "block filename/hash mismatch: {}",
                    file.display()
                );
                blocks.insert(block.hash.clone(), block);
            }
            let selected_tip = blocks.values().max_by_key(|block| block.height).cloned();
            Ok(Self {
                state: Arc::new(Mutex::new(StoreState {
                    blocks,
                    selected_tip,
                })),
                persistence_path: Some(Arc::new(path)),
                write_gate: Arc::new(Mutex::new(())),
            })
        })
        .await?
    }

    pub async fn get(&self, hash: &BlockHash) -> Option<Block> {
        self.state.lock().await.blocks.get(hash).cloned()
    }

    pub async fn insert(&self, block: Block) -> anyhow::Result<bool> {
        let gate = self.write_gate.clone().lock_owned().await;
        if self.state.lock().await.blocks.contains_key(&block.hash) {
            return Ok(false);
        }
        let Some(path) = self.persistence_path.clone() else {
            let mut state = self.state.lock().await;
            Self::commit(&mut state, block);
            return Ok(true);
        };
        let state = self.state.clone();
        // The worker owns both the gate and memory commit, even if its caller is cancelled.
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            Self::persist_block(&path, &block)?;
            // Rename made the block visible. Keep memory in step with disk even if
            // directory syncing reports a durability error to the caller.
            let directory_sync = Self::sync_directory(&path);
            let mut state = state.blocking_lock();
            Self::commit(&mut state, block);
            directory_sync.map(|()| true)
        })
        .await?
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.blocks.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    pub async fn tip(&self) -> Option<Block> {
        self.state.lock().await.selected_tip.clone()
    }

    fn commit(state: &mut StoreState, block: Block) {
        if state
            .selected_tip
            .as_ref()
            .is_none_or(|tip| block.height > tip.height)
        {
            state.selected_tip = Some(block.clone());
        }
        state.blocks.insert(block.hash.clone(), block);
    }

    fn persist_block(path: &Path, block: &Block) -> anyhow::Result<()> {
        let bytes = bincode::serialize(block)?;
        let temporary_path = path.join(format!("{}.tmp", block.hash));
        let final_path = path.join(format!("{}.block", block.hash));
        let mut file = File::create(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, &final_path)?;
        Ok(())
    }

    fn sync_directory(path: &Path) -> anyhow::Result<()> {
        File::open(path)?.sync_all()?;
        Ok(())
    }
}

#[async_trait]
impl BlockStorePort for ChainStore {
    async fn get_block(&self, hash: &BlockHash) -> Option<Block> {
        self.get(hash).await
    }

    async fn insert_block(&self, block: Block) -> anyhow::Result<bool> {
        self.insert(block).await
    }

    async fn selected_tip(&self) -> Option<Block> {
        self.tip().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[tokio::test]
    async fn insert_and_fetch_block() -> anyhow::Result<()> {
        let store = ChainStore::new();
        let block = block(1);

        assert!(store.insert(block.clone()).await?);
        let stored = store
            .get(&block.hash)
            .await
            .ok_or_else(|| anyhow::anyhow!("expected stored block"))?;
        assert_eq!(stored.height, 1);
        assert_eq!(store.len().await, 1);
        Ok(())
    }

    #[tokio::test]
    async fn selected_tip_is_updated_only_by_a_higher_block() -> anyhow::Result<()> {
        let store = ChainStore::new();
        let mut tip = block(2);
        tip.height = 2;
        let lower = block(1);

        assert!(store.insert(tip.clone()).await?);
        assert!(store.insert(lower).await?);
        assert_eq!(store.tip().await, Some(tip));
        Ok(())
    }

    #[tokio::test]
    async fn persists_blocks_across_reopen() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "p2p-chain-lib-chain-store-{}.bin",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = ChainStore::open(&path).await?;
        let expected = block(1);
        store.insert(expected.clone()).await?;
        drop(store);

        let reopened = ChainStore::open(&path).await?;
        assert_eq!(reopened.get(&expected.hash).await, Some(expected));
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chain-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn writes_only_new_blocks_and_recovers_past_uncommitted_files() -> anyhow::Result<()> {
        let path = test_path("incremental");
        let store = ChainStore::open(&path).await?;
        let first = block(1);
        store.insert(first.clone()).await?;
        let first_path = path.join(format!("{}.block", first.hash));
        let original = fs::read(&first_path)?;
        let modified = fs::metadata(&first_path)?.modified()?;
        let mut second = block(2);
        second.height = 2;
        second.parent_hash = Some(first.hash.clone());
        let a = store.clone();
        let b = store.clone();
        let (a, b) = tokio::join!(a.insert(second.clone()), b.insert(second.clone()));
        assert_ne!(a?, b?, "only one duplicate insertion commits");
        assert_eq!(fs::read(&first_path)?, original);
        assert_eq!(fs::metadata(&first_path)?.modified()?, modified);
        assert_eq!(fs::read_dir(&path)?.count(), 2);
        fs::write(path.join("interrupted.tmp"), b"partial block")?;
        let reopened = ChainStore::open(&path).await?;
        assert_eq!(reopened.len().await, 2);
        assert_eq!(reopened.tip().await, Some(second));
        assert_eq!(reopened.get(&first.hash).await, Some(first));
        fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_corrupt_committed_blocks_and_old_snapshots() -> anyhow::Result<()> {
        let path = test_path("corrupt");
        fs::write(&path, b"old snapshot")?;
        assert!(ChainStore::open(&path).await.is_err());
        assert_eq!(fs::read(&path)?, b"old snapshot");
        fs::remove_file(&path)?;
        let store = ChainStore::open(&path).await?;
        let block = block(1);
        store.insert(block.clone()).await?;
        let committed = path.join(format!("{}.block", block.hash));
        fs::rename(
            &committed,
            path.join(format!("{}.block", BlockHash::zero())),
        )?;
        assert!(ChainStore::open(&path).await.is_err());
        fs::rename(
            path.join(format!("{}.block", BlockHash::zero())),
            &committed,
        )?;
        fs::write(committed, b"truncated")?;
        assert!(ChainStore::open(&path).await.is_err());
        fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_does_not_split_disk_and_memory_commit() -> anyhow::Result<()> {
        let path = test_path("cancel");
        let store = ChainStore::open(&path).await?;
        let expected = block(1);
        // Hold the writer gate until the insert future is queued.
        let gate = store.write_gate.lock().await;
        let writer = store.clone();
        let candidate = expected.clone();
        let task = tokio::spawn(async move { writer.insert(candidate).await });
        tokio::task::yield_now().await;
        drop(gate);
        // Let the insert perform its existence check and dispatch the blocking worker.
        tokio::task::yield_now().await;
        let memory = store.state.lock().await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !path.join(format!("{}.block", expected.hash)).exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        task.abort();
        let _ = task.await;
        drop(memory);
        // A second insert waits for the original worker, including its memory commit.
        assert!(!store.insert(expected.clone()).await?);
        assert_eq!(store.get(&expected.hash).await, Some(expected.clone()));
        assert_eq!(ChainStore::open(&path).await?.tip().await, Some(expected));
        fs::remove_dir_all(path)?;
        Ok(())
    }
}
