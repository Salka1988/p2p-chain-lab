use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
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

#[derive(Debug, Clone)]
pub struct SimpleConsensus<C> {
    signing_key: SigningKey,
    validator_keys: Vec<VerifyingKey>,
    chain_store: C,
    producer_state: std::sync::Arc<Mutex<ProducerState>>,
}

#[derive(Debug, Default)]
struct ProducerState {
    next_height: u64,
    parent_hash: Option<BlockHash>,
    initialized: bool,
}

impl<C> SimpleConsensus<C>
where
    C: BlockStorePort,
{
    pub fn new(node_key_seed: impl AsRef<str>, chain_store: C) -> Self {
        Self::with_validators(node_key_seed, chain_store, Vec::new())
            .expect("an empty validator set always uses the local validator")
    }

    pub fn with_validators(
        node_key_seed: impl AsRef<str>,
        chain_store: C,
        validators: Vec<String>,
    ) -> anyhow::Result<Self> {
        let signing_key = signing_key(node_key_seed.as_ref());
        let validators = if validators.is_empty() {
            vec![validator_id(&signing_key.verifying_key())]
        } else {
            validators
        };
        let validator_keys = validators
            .iter()
            .map(|validator| parse_validator_id(validator))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            signing_key,
            validator_keys,
            chain_store,
            producer_state: std::sync::Arc::new(Mutex::new(ProducerState::default())),
        })
    }

    async fn produce_block(&self) -> Option<Block> {
        self.initialize_state().await;
        let mut state = self.producer_state.lock().await;
        let expected_producer = self.expected_producer(state.next_height)?;

        if *expected_producer != self.signing_key.verifying_key() {
            return None;
        }

        let height = state.next_height;
        let parent_hash = state.parent_hash.clone();
        let producer = validator_id(&self.signing_key.verifying_key());
        let mut block = Block {
            hash: BlockHash::zero(),
            parent_hash,
            height,
            producer,
            signature: Vec::new(),
            body: format!("simple block at height {height}").into_bytes(),
        };
        block.signature = self
            .signing_key
            .sign(&block_signing_bytes(&block))
            .to_bytes()
            .to_vec();
        block.hash = block_hash(&block);

        state.next_height += 1;
        state.parent_hash = Some(block.hash.clone());
        Some(block)
    }

    async fn observe_valid_block(&self, block: &Block) {
        let mut state = self.producer_state.lock().await;
        if block.height >= state.next_height {
            state.next_height = block.height + 1;
            state.parent_hash = Some(block.hash.clone());
        }
    }

    fn expected_producer(&self, height: u64) -> Option<&VerifyingKey> {
        let validator_count = u64::try_from(self.validator_keys.len()).ok()?;
        let validator_index = usize::try_from(height % validator_count).ok()?;
        self.validator_keys.get(validator_index)
    }

    async fn validate_received_block(&self, block: &Block) -> anyhow::Result<()> {
        self.initialize_state().await;
        validate_block(block)?;
        anyhow::ensure!(
            block.hash == block_hash(block),
            "block hash does not match contents"
        );
        let expected_producer = self
            .expected_producer(block.height)
            .ok_or_else(|| anyhow::anyhow!("no configured validators"))?;
        anyhow::ensure!(
            block.producer == validator_id(expected_producer),
            "block producer is not scheduled for this height"
        );
        let signature = Signature::from_slice(&block.signature)
            .map_err(|_| anyhow::anyhow!("block signature has invalid length"))?;
        expected_producer
            .verify(&block_signing_bytes(block), &signature)
            .map_err(|_| anyhow::anyhow!("block signature is invalid"))?;

        let state = self.producer_state.lock().await;
        anyhow::ensure!(
            block.height == state.next_height,
            "block does not extend the selected chain"
        );
        anyhow::ensure!(
            block.parent_hash == state.parent_hash,
            "block parent is not the selected tip"
        );

        Ok(())
    }

    async fn initialize_state(&self) {
        let tip = self.chain_store.selected_tip().await;
        let mut state = self.producer_state.lock().await;
        if state.initialized {
            return;
        }
        if let Some(tip) = tip {
            state.next_height = tip.height + 1;
            state.parent_hash = Some(tip.hash);
        }
        state.initialized = true;
    }
}

impl<C> SimpleConsensus<C>
where
    C: BlockStorePort,
{
    pub async fn handle_event(
        &self,
        event: ConsensusEvent,
    ) -> anyhow::Result<Vec<ConsensusCommand>> {
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
            ConsensusEvent::BlockReceived { peer_id, block } => {
                match self.validate_received_block(&block).await {
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
                }
            }
        }
    }
}

pub fn validator_id_from_seed(seed: &str) -> String {
    validator_id(&signing_key(seed).verifying_key())
}

fn signing_key(seed: &str) -> SigningKey {
    let bytes: [u8; 32] = Sha256::digest(seed.as_bytes()).into();
    SigningKey::from_bytes(&bytes)
}

fn validator_id(key: &VerifyingKey) -> String {
    key.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parse_validator_id(value: &str) -> anyhow::Result<VerifyingKey> {
    anyhow::ensure!(
        value.len() == 64,
        "validator id must be a 32-byte public key encoded as hex"
    );
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("validator id must be hexadecimal"))?;
    }
    VerifyingKey::from_bytes(&bytes)
        .map_err(|_| anyhow::anyhow!("validator id is not a valid Ed25519 public key"))
}

fn block_signing_bytes(block: &Block) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&block.height.to_le_bytes());
    match &block.parent_hash {
        Some(parent_hash) => {
            bytes.push(1);
            bytes.extend_from_slice(parent_hash.as_bytes());
        }
        None => bytes.push(0),
    }
    bytes.extend_from_slice(&(block.producer.len() as u64).to_le_bytes());
    bytes.extend_from_slice(block.producer.as_bytes());
    bytes.extend_from_slice(&(block.body.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&block.body);
    bytes
}

fn block_hash(block: &Block) -> BlockHash {
    let mut hasher = Sha256::new();
    hasher.update(block_signing_bytes(block));
    hasher.update(&block.signature);

    let digest = hasher.finalize();
    let mut bytes = [0; 32];
    bytes.copy_from_slice(&digest);
    BlockHash::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p_chain_lib_chain::store::ChainStore;

    fn signed_block(seed: &str, parent_hash: Option<BlockHash>, height: u64) -> Block {
        let key = signing_key(seed);
        let mut block = Block {
            hash: BlockHash::zero(),
            parent_hash,
            height,
            producer: validator_id(&key.verifying_key()),
            signature: Vec::new(),
            body: format!("block {height}").into_bytes(),
        };
        block.signature = key.sign(&block_signing_bytes(&block)).to_bytes().to_vec();
        block.hash = block_hash(&block);
        block
    }

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
            vec![
                validator_id_from_seed("node-a"),
                validator_id_from_seed("node-b"),
            ],
        )?;
        let commands = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(commands.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn resumes_production_after_the_persisted_tip() -> anyhow::Result<()> {
        let store = ChainStore::new();
        store.insert(signed_block("node-a", None, 0)).await?;
        let consensus = SimpleConsensus::new("node-a", store);

        let commands = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(
            matches!(commands.as_slice(), [ConsensusCommand::StoreBlock(block), ..] if block.height == 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn received_block_advances_next_local_turn() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::with_validators(
            "node-b",
            ChainStore::new(),
            vec![
                validator_id_from_seed("node-a"),
                validator_id_from_seed("node-b"),
            ],
        )?;
        let parent = signed_block("node-a", None, 0);

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
    async fn rejects_block_with_unknown_parent() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let block = signed_block("node-a", Some(BlockHash::from_u8(1)), 1);

        let commands = consensus
            .handle_event(ConsensusEvent::BlockReceived {
                peer_id: PeerId::from("peer-a"),
                block,
            })
            .await?;

        assert!(matches!(
            commands.as_slice(),
            [ConsensusCommand::ReportBadPeer { .. }]
        ));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_tampered_hash_wrong_producer_and_invalid_signature() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::with_validators(
            "node-a",
            ChainStore::new(),
            vec![validator_id_from_seed("node-a")],
        )?;

        let mut wrong_hash = signed_block("node-a", None, 0);
        wrong_hash.hash = BlockHash::from_u8(9);
        let mut wrong_producer = signed_block("node-b", None, 0);
        wrong_producer.producer = validator_id_from_seed("node-a");
        wrong_producer.hash = block_hash(&wrong_producer);
        let mut invalid_signature = signed_block("node-a", None, 0);
        invalid_signature.signature[0] ^= 1;
        invalid_signature.hash = block_hash(&invalid_signature);

        for block in [wrong_hash, wrong_producer, invalid_signature] {
            let commands = consensus
                .handle_event(ConsensusEvent::BlockReceived {
                    peer_id: PeerId::from("peer-a"),
                    block,
                })
                .await?;
            assert!(matches!(
                commands.as_slice(),
                [ConsensusCommand::ReportBadPeer { .. }]
            ));
        }
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
