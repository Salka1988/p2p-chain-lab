use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::block::{Block, BlockHash};

#[async_trait]
pub trait BlockStorePort: Clone + Send + Sync + 'static {
    async fn get_block(&self, hash: &BlockHash) -> Option<Block>;

    async fn insert_block(&self, block: Block) -> bool;
}

#[derive(Debug, Clone, Default)]
pub struct ChainStore {
    blocks: Arc<Mutex<HashMap<BlockHash, Block>>>,
}

impl ChainStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, hash: &BlockHash) -> Option<Block> {
        self.blocks.lock().await.get(hash).cloned()
    }

    pub async fn insert(&self, block: Block) -> bool {
        self.blocks
            .lock()
            .await
            .insert(block.hash.clone(), block)
            .is_none()
    }

    pub async fn len(&self) -> usize {
        self.blocks.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[async_trait]
impl BlockStorePort for ChainStore {
    async fn get_block(&self, hash: &BlockHash) -> Option<Block> {
        self.get(hash).await
    }

    async fn insert_block(&self, block: Block) -> bool {
        self.insert(block).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(hash: u8) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: None,
            height: 1,
            body: vec![1],
        }
    }

    #[tokio::test]
    async fn insert_and_fetch_block() -> Result<(), &'static str> {
        let store = ChainStore::new();
        let block = block(1);

        assert!(store.insert(block.clone()).await);
        let stored = store.get(&block.hash).await.ok_or("expected stored block")?;
        assert_eq!(stored.height, 1);
        assert_eq!(store.len().await, 1);
        Ok(())
    }
}
