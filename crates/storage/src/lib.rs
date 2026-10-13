use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::Mutex;

use p2p_chain_lib_chain::{
    block::{Block, BlockHash},
    store::BlockStorePort,
};

#[derive(Debug, Clone, Default)]
pub struct ChainStore {
    blocks: Arc<Mutex<HashMap<BlockHash, Block>>>,
    persistence_path: Option<Arc<PathBuf>>,
}

impl ChainStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let blocks = if path.exists() {
            let bytes = fs::read(&path)?;
            bincode::deserialize::<Vec<Block>>(&bytes)?
        } else {
            Vec::new()
        };
        let blocks = blocks
            .into_iter()
            .map(|block| (block.hash.clone(), block))
            .collect();

        Ok(Self {
            blocks: Arc::new(Mutex::new(blocks)),
            persistence_path: Some(Arc::new(path)),
        })
    }

    pub async fn get(&self, hash: &BlockHash) -> Option<Block> {
        self.blocks.lock().await.get(hash).cloned()
    }

    pub async fn insert(&self, block: Block) -> anyhow::Result<bool> {
        let mut blocks = self.blocks.lock().await;
        if blocks.contains_key(&block.hash) {
            return Ok(false);
        }
        self.persist(&blocks, &block)?;
        blocks.insert(block.hash.clone(), block);
        Ok(true)
    }

    pub async fn len(&self) -> usize {
        self.blocks.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    pub async fn tip(&self) -> Option<Block> {
        self.blocks
            .lock()
            .await
            .values()
            .max_by_key(|block| block.height)
            .cloned()
    }

    fn persist(&self, blocks: &HashMap<BlockHash, Block>, block: &Block) -> anyhow::Result<()> {
        let Some(path) = &self.persistence_path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = bincode::serialize(
            &blocks
                .values()
                .chain(std::iter::once(block))
                .collect::<Vec<_>>(),
        )?;
        let temporary_path = path.with_extension("tmp");
        fs::write(&temporary_path, bytes)?;
        fs::rename(temporary_path, path.as_ref())?;
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
    async fn persists_blocks_across_reopen() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "p2p-chain-lib-chain-store-{}.bin",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = ChainStore::open(&path)?;
        let expected = block(1);
        store.insert(expected.clone()).await?;
        drop(store);

        let reopened = ChainStore::open(&path)?;
        assert_eq!(reopened.get(&expected.hash).await, Some(expected));
        std::fs::remove_file(path)?;
        Ok(())
    }
}
