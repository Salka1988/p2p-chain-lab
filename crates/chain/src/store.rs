use async_trait::async_trait;

use crate::block::{Block, BlockHash};

#[async_trait]
pub trait BlockStorePort: Clone + Send + Sync + 'static {
    async fn get_block(&self, hash: &BlockHash) -> Option<Block>;

    async fn insert_block(&self, block: Block) -> anyhow::Result<bool>;

    async fn selected_tip(&self) -> Option<Block> {
        None
    }
}
