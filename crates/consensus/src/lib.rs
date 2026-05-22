use async_trait::async_trait;
use p2p_chain_lib_chain::{
    block::{Block, BlockHash},
    store::BlockStorePort,
};
use p2p_chain_lib_ports::PeerId;
use p2p_chain_lib_protocol::{
    messages::BlockAnnouncement,
    validation::{validate_block, validate_block_announcement},
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum ConsensusEvent {
    Tick,
    BlockAnnouncement {
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    },
    BlockReceived {
        peer_id: PeerId,
        block: Block,
    },
}

#[derive(Debug, Clone)]
pub enum ConsensusCommand {
    StoreBlock(Block),
    BroadcastBlock(Block),
    RequestBlock {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    ReportGoodPeer {
        peer_id: PeerId,
        reason: String,
    },
    ReportBadPeer {
        peer_id: PeerId,
        reason: String,
    },
}

#[async_trait]
pub trait ConsensusPort: Clone + Send + Sync + 'static {
    async fn handle_event(&self, event: ConsensusEvent) -> anyhow::Result<Vec<ConsensusCommand>>;
}

#[derive(Debug, Clone)]
pub struct SimpleConsensus<C> {
    node_id: String,
    chain_store: C,
    producer_state: std::sync::Arc<Mutex<ProducerState>>,
}

#[derive(Debug, Default)]
struct ProducerState {
    next_height: u64,
    parent_hash: Option<BlockHash>,
}

impl<C> SimpleConsensus<C>
where
    C: BlockStorePort,
{
    pub fn new(node_id: impl Into<String>, chain_store: C) -> Self {
        Self {
            node_id: node_id.into(),
            chain_store,
            producer_state: std::sync::Arc::new(Mutex::new(ProducerState::default())),
        }
    }

    async fn produce_block(&self) -> Block {
        let mut state = self.producer_state.lock().await;
        let height = state.next_height;
        let parent_hash = state.parent_hash.clone();
        let hash = block_hash(&self.node_id, height, parent_hash.as_ref());
        let block = Block {
            hash: hash.clone(),
            parent_hash,
            height,
            body: format!("simple block from {} at height {height}", self.node_id).into_bytes(),
        };

        state.next_height += 1;
        state.parent_hash = Some(hash);
        block
    }
}

#[async_trait]
impl<C> ConsensusPort for SimpleConsensus<C>
where
    C: BlockStorePort,
{
    async fn handle_event(&self, event: ConsensusEvent) -> anyhow::Result<Vec<ConsensusCommand>> {
        match event {
            ConsensusEvent::Tick => {
                let block = self.produce_block().await;
                Ok(vec![
                    ConsensusCommand::StoreBlock(block.clone()),
                    ConsensusCommand::BroadcastBlock(block),
                ])
            }
            ConsensusEvent::BlockAnnouncement {
                peer_id,
                announcement,
            } => {
                if let Err(error) = validate_block_announcement(&announcement) {
                    return Ok(vec![ConsensusCommand::ReportBadPeer {
                        peer_id,
                        reason: format!("invalid block announcement: {error}"),
                    }]);
                }

                if self
                    .chain_store
                    .get_block(&announcement.block_hash)
                    .await
                    .is_some()
                {
                    return Ok(vec![ConsensusCommand::ReportGoodPeer {
                        peer_id,
                        reason: "announced already-known block".to_string(),
                    }]);
                }

                Ok(vec![ConsensusCommand::RequestBlock {
                    peer_id,
                    block_hash: announcement.block_hash,
                }])
            }
            ConsensusEvent::BlockReceived { peer_id, block } => match validate_block(&block) {
                Ok(()) => Ok(vec![
                    ConsensusCommand::StoreBlock(block),
                    ConsensusCommand::ReportGoodPeer {
                        peer_id,
                        reason: "valid block received".to_string(),
                    },
                ]),
                Err(error) => Ok(vec![ConsensusCommand::ReportBadPeer {
                    peer_id,
                    reason: format!("invalid block: {error}"),
                }]),
            },
        }
    }
}

fn block_hash(node_id: &str, height: u64, parent_hash: Option<&BlockHash>) -> BlockHash {
    let mut hasher = Sha256::new();
    hasher.update(node_id.as_bytes());
    hasher.update(height.to_le_bytes());
    if let Some(parent_hash) = parent_hash {
        hasher.update(parent_hash.as_bytes());
    }

    let digest = hasher.finalize();
    let mut bytes = [0; 32];
    bytes.copy_from_slice(&digest);
    BlockHash::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p_chain_lib_chain::store::ChainStore;

    #[tokio::test]
    async fn tick_produces_store_and_broadcast_commands() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let commands = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(matches!(commands.as_slice(), [
            ConsensusCommand::StoreBlock(block_a),
            ConsensusCommand::BroadcastBlock(block_b),
        ] if block_a == block_b && block_a.height == 0 && block_a.parent_hash.is_none()));
        Ok(())
    }

    #[tokio::test]
    async fn announcement_requests_missing_block() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let block_hash = BlockHash::from_u8(7);
        let commands = consensus
            .handle_event(ConsensusEvent::BlockAnnouncement {
                peer_id: PeerId::from("peer-a"),
                announcement: BlockAnnouncement {
                    block_hash: block_hash.clone(),
                    height: 1,
                },
            })
            .await?;

        assert!(matches!(commands.as_slice(), [
            ConsensusCommand::RequestBlock { peer_id, block_hash: requested_hash }
        ] if peer_id == &PeerId::from("peer-a") && requested_hash == &block_hash));
        Ok(())
    }
}
