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
    validators: Vec<String>,
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
        let node_id = node_id.into();
        Self::with_validators(node_id.clone(), chain_store, vec![node_id])
    }

    pub fn with_validators(
        node_id: impl Into<String>,
        chain_store: C,
        validators: Vec<String>,
    ) -> Self {
        let node_id = node_id.into();
        let validators = if validators.is_empty() {
            vec![node_id.clone()]
        } else {
            validators
        };

        Self {
            node_id,
            validators,
            chain_store,
            producer_state: std::sync::Arc::new(Mutex::new(ProducerState::default())),
        }
    }

    async fn produce_block(&self) -> Option<Block> {
        let mut state = self.producer_state.lock().await;
        let expected_producer = self.expected_producer(state.next_height)?;

        if expected_producer != self.node_id {
            return None;
        }

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
        Some(block)
    }

    async fn observe_valid_block(&self, block: &Block) {
        let mut state = self.producer_state.lock().await;
        if block.height >= state.next_height {
            state.next_height = block.height + 1;
            state.parent_hash = Some(block.hash.clone());
        }
    }

    fn expected_producer(&self, height: u64) -> Option<&str> {
        let validator_count = u64::try_from(self.validators.len()).ok()?;
        let validator_index = usize::try_from(height % validator_count).ok()?;
        self.validators.get(validator_index).map(String::as_str)
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
                let Some(block) = self.produce_block().await else {
                    return Ok(Vec::new());
                };

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

                Ok(Vec::new())
            }
            ConsensusEvent::BlockReceived { peer_id, block } => match validate_block(&block) {
                Ok(()) => {
                    self.observe_valid_block(&block).await;
                    Ok(vec![
                        ConsensusCommand::StoreBlock(block),
                        ConsensusCommand::ReportGoodPeer {
                            peer_id,
                            reason: "valid block received".to_string(),
                        },
                    ])
                }
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
    async fn tick_waits_when_it_is_not_local_turn() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::with_validators(
            "node-b",
            ChainStore::new(),
            vec!["node-a".to_string(), "node-b".to_string()],
        );
        let commands = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(commands.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn received_block_advances_next_local_turn() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::with_validators(
            "node-b",
            ChainStore::new(),
            vec!["node-a".to_string(), "node-b".to_string()],
        );
        let parent = Block {
            hash: BlockHash::from_u8(1),
            parent_hash: None,
            height: 0,
            body: b"node-a block".to_vec(),
        };

        consensus
            .handle_event(ConsensusEvent::BlockReceived {
                peer_id: PeerId::from("node-a"),
                block: parent.clone(),
            })
            .await?;
        let commands = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(matches!(commands.as_slice(), [
            ConsensusCommand::StoreBlock(block_a),
            ConsensusCommand::BroadcastBlock(block_b),
        ] if block_a == block_b && block_a.height == 1 && block_a.parent_hash == Some(parent.hash)));
        Ok(())
    }

    #[tokio::test]
    async fn announcement_does_not_request_missing_block() -> anyhow::Result<()> {
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

        assert!(commands.is_empty());
        Ok(())
    }
}
