use std::{collections::HashMap, future::Future, sync::Arc};

use tokio::sync::Mutex;

use crate::block::{Block, BlockHash};

pub trait BlockStorePort: Clone + Send + Sync + 'static {
    fn get_block<'a>(
        &'a self,
        hash: &'a BlockHash,
    ) -> impl Future<Output = Option<Block>> + Send + 'a;

    fn insert_block(&self, block: Block) -> impl Future<Output = bool> + Send + '_;
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

impl BlockStorePort for ChainStore {
    fn get_block<'a>(
        &'a self,
        hash: &'a BlockHash,
    ) -> impl Future<Output = Option<Block>> + Send + 'a {
        self.get(hash)
    }

    fn insert_block(&self, block: Block) -> impl Future<Output = bool> + Send + '_ {
        self.insert(block)
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
    async fn insert_and_fetch_block() {
        let store = ChainStore::new();
        let block = block(1);

        assert!(store.insert(block.clone()).await);
        assert_eq!(store.get(&block.hash).await.unwrap().height, 1);
        assert_eq!(store.len().await, 1);
    }
}
