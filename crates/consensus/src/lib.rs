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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusOutcome {
    /// No block transition, e.g. an idle tick or an unknown announcement.
    NoChange,
    /// Validated for acceptance; storage commands must still succeed.
    Accepted,
    AlreadyKnown,
    MissingParent,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct ConsensusDecision {
    pub outcome: ConsensusOutcome,
    pub commands: Vec<ConsensusCommand>,
}

#[derive(Debug, Clone)]
pub struct SimpleConsensus<C> {
    signing_key: SigningKey,
    validator_keys: Vec<VerifyingKey>,
    chain_store: C,
    genesis: Option<Block>,
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
            genesis: None,
        })
    }

    /// Initialize a chain anchored to configuration, without a producer signature.
    pub async fn with_genesis(
        seed: impl AsRef<str>,
        chain_store: C,
        chain_id: &str,
        validators: Vec<String>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !chain_id.trim().is_empty(),
            "genesis chain_id must not be empty"
        );
        anyhow::ensure!(
            !validators.is_empty(),
            "genesis validators must not be empty"
        );
        let mut consensus = Self::with_validators(seed, chain_store, validators)?;
        let mut body = b"p2p-chain-lib/genesis/v1".to_vec();
        body.extend_from_slice(&(chain_id.len() as u64).to_le_bytes());
        body.extend_from_slice(chain_id.as_bytes());
        for (index, key) in consensus.validator_keys.iter().enumerate() {
            anyhow::ensure!(
                !consensus.validator_keys[..index].contains(key),
                "duplicate genesis validator"
            );
            body.extend_from_slice(key.as_bytes());
        }
        let mut genesis = Block {
            hash: BlockHash::zero(),
            parent_hash: None,
            height: 0,
            producer: "genesis".into(),
            signature: Vec::new(),
            body,
        };
        genesis.hash = block_hash(&genesis);
        if let Some(tip) = consensus.chain_store.selected_tip().await {
            let mut chain = vec![tip];
            while chain.last().is_some_and(|block| block.height > 0) {
                let block = chain.last().expect("chain contains its tip");
                let parent = block
                    .parent_hash
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("stored chain has no parent"))?;
                let parent = consensus
                    .chain_store
                    .get_block(parent)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("stored chain is missing an ancestor"))?;
                anyhow::ensure!(parent.height == block.height - 1, "invalid stored ancestry");
                chain.push(parent);
            }
            chain.reverse();
            anyhow::ensure!(
                chain.first() == Some(&genesis),
                "stored genesis does not match configuration; use a new chain_path for a new chain"
            );
            for blocks in chain.windows(2) {
                let [parent, block] = blocks else {
                    unreachable!("windows of two have two blocks")
                };
                anyhow::ensure!(
                    block.parent_hash.as_ref() == Some(&parent.hash),
                    "invalid stored ancestry"
                );
                consensus.validate_block_candidate(block)?;
            }
        } else {
            consensus.chain_store.insert_block(genesis.clone()).await?;
        }
        consensus.genesis = Some(genesis);
        Ok(consensus)
    }

    async fn produce_block(&self) -> Option<Block> {
        let (height, parent_hash) = self.next_block_position().await;
        let expected_producer = self.expected_producer(height)?;

        if *expected_producer != self.signing_key.verifying_key() {
            return None;
        }

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

        Some(block)
    }

    fn expected_producer(&self, height: u64) -> Option<&VerifyingKey> {
        let validator_count = u64::try_from(self.validator_keys.len()).ok()?;
        let validator_index = usize::try_from(height % validator_count).ok()?;
        self.validator_keys.get(validator_index)
    }

    async fn validate_chain_extension(&self, block: &Block) -> anyhow::Result<()> {
        let (next_height, parent_hash) = self.next_block_position().await;
        anyhow::ensure!(
            block.height == next_height,
            "block does not extend the selected chain"
        );
        anyhow::ensure!(
            block.parent_hash == parent_hash,
            "block parent is not the selected tip"
        );

        Ok(())
    }

    pub fn validate_block_candidate(&self, block: &Block) -> anyhow::Result<()> {
        if block.height == 0
            && let Some(genesis) = &self.genesis
        {
            anyhow::ensure!(block == genesis, "block does not match configured genesis");
            return Ok(());
        }
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

        Ok(())
    }

    async fn next_block_position(&self) -> (u64, Option<BlockHash>) {
        // Only successfully stored blocks advance consensus.
        match self.chain_store.selected_tip().await {
            Some(tip) => (tip.height + 1, Some(tip.hash)),
            None => (0, None),
        }
    }
}

impl<C> SimpleConsensus<C>
where
    C: BlockStorePort,
{
    pub async fn handle_event(&self, event: ConsensusEvent) -> anyhow::Result<ConsensusDecision> {
        let decision = |outcome, commands| Ok(ConsensusDecision { outcome, commands });
        match event {
            ConsensusEvent::Tick => {
                let Some(block) = self.produce_block().await else {
                    return decision(ConsensusOutcome::NoChange, Vec::new());
                };

                decision(
                    ConsensusOutcome::Accepted,
                    vec![
                        ConsensusCommand::StoreBlock(block.clone()),
                        ConsensusCommand::BroadcastBlock(block),
                    ],
                )
            }
            ConsensusEvent::BlockAnnouncement {
                peer_id,
                announcement,
            } => {
                if let Err(error) = validate_block_announcement(&announcement) {
                    return decision(
                        ConsensusOutcome::Rejected,
                        vec![ConsensusCommand::ReportBadPeer {
                            peer_id,
                            reason: format!("invalid block announcement: {error}"),
                        }],
                    );
                }

                if self
                    .chain_store
                    .get_block(&announcement.block_hash)
                    .await
                    .is_some()
                {
                    return decision(
                        ConsensusOutcome::AlreadyKnown,
                        vec![ConsensusCommand::ReportGoodPeer {
                            peer_id,
                            reason: "announced already-known block".to_string(),
                        }],
                    );
                }

                decision(ConsensusOutcome::NoChange, Vec::new())
            }
            ConsensusEvent::BlockReceived { peer_id, block } => {
                if let Err(error) = self.validate_block_candidate(&block) {
                    return decision(
                        ConsensusOutcome::Rejected,
                        vec![ConsensusCommand::ReportBadPeer {
                            peer_id,
                            reason: format!("invalid block: {error}"),
                        }],
                    );
                }

                if self.chain_store.get_block(&block.hash).await.is_some() {
                    return decision(
                        ConsensusOutcome::AlreadyKnown,
                        vec![ConsensusCommand::ReportGoodPeer {
                            peer_id,
                            reason: "already-known valid block received".to_string(),
                        }],
                    );
                }

                // A valid candidate with missing ancestry must be buffered by sync first.
                if let Some(parent_hash) = &block.parent_hash
                    && self.chain_store.get_block(parent_hash).await.is_none()
                {
                    return decision(ConsensusOutcome::MissingParent, Vec::new());
                }

                match self.validate_chain_extension(&block).await {
                    Ok(()) => decision(
                        ConsensusOutcome::Accepted,
                        vec![
                            ConsensusCommand::StoreBlock(block),
                            ConsensusCommand::ReportGoodPeer {
                                peer_id,
                                reason: "valid block received".to_string(),
                            },
                        ],
                    ),
                    Err(error) => decision(
                        ConsensusOutcome::Rejected,
                        vec![ConsensusCommand::ReportBadPeer {
                            peer_id,
                            reason: format!("invalid block: {error}"),
                        }],
                    ),
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
        value.len() == 64 && value.is_ascii(),
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
    use p2p_chain_lib_storage::ChainStore;

    #[tokio::test]
    async fn configured_genesis_is_shared_and_anchors_production() -> anyhow::Result<()> {
        let validators = vec![validator_id_from_seed("a")];
        let store = ChainStore::new();
        let producer =
            SimpleConsensus::with_genesis("a", store.clone(), "dev", validators.clone()).await?;
        let other_store = ChainStore::new();
        let observer = SimpleConsensus::with_genesis(
            "observer",
            other_store.clone(),
            "dev",
            validators.clone(),
        )
        .await?;
        let genesis = store.tip().await.unwrap();
        assert_eq!(Some(genesis.clone()), other_store.tip().await);
        assert_eq!(genesis.height, 0);
        observer.validate_block_candidate(&genesis)?;
        let decision = producer.handle_event(ConsensusEvent::Tick).await?;
        let ConsensusCommand::StoreBlock(block) = &decision.commands[0] else {
            panic!("expected block")
        };
        assert_eq!(block.height, 1);
        assert_eq!(block.parent_hash, Some(genesis.hash.clone()));
        store.insert(block.clone()).await?;
        SimpleConsensus::with_genesis("a", store.clone(), "dev", validators.clone()).await?;
        assert!(
            SimpleConsensus::with_genesis("a", store.clone(), "other", validators)
                .await
                .is_err()
        );
        assert_eq!(store.len().await, 2);
        let mut altered = genesis;
        altered.body.push(1);
        assert!(observer.validate_block_candidate(&altered).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_genesis_settings() {
        let key = validator_id_from_seed("a");
        for (id, validators) in [
            ("", vec![key.clone()]),
            ("dev", vec![]),
            ("dev", vec![key.clone(), key]),
            ("dev", vec!["invalid".into()]),
        ] {
            assert!(
                SimpleConsensus::with_genesis("a", ChainStore::new(), id, validators)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn persisted_genesis_mismatch_leaves_file_unchanged() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "genesis-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let validators = vec![validator_id_from_seed("a")];
        SimpleConsensus::with_genesis(
            "a",
            ChainStore::open(&path).await?,
            "dev",
            validators.clone(),
        )
        .await?;
        let genesis = ChainStore::open(&path).await?.tip().await.unwrap();
        let genesis_path = path.join(format!("{}.block", genesis.hash));
        let bytes = std::fs::read(&genesis_path)?;
        SimpleConsensus::with_genesis(
            "observer",
            ChainStore::open(&path).await?,
            "dev",
            validators.clone(),
        )
        .await?;
        assert!(
            SimpleConsensus::with_genesis(
                "a",
                ChainStore::open(&path).await?,
                "different",
                validators
            )
            .await
            .is_err()
        );
        assert!(
            SimpleConsensus::with_genesis(
                "a",
                ChainStore::open(&path).await?,
                "dev",
                vec![validator_id_from_seed("b")]
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(&genesis_path)?, bytes);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_a_recovered_block_with_altered_contents() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "recovered-block-validation-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let validators = vec![validator_id_from_seed("node-a")];
        let store = ChainStore::open(&path).await?;
        let consensus = SimpleConsensus::with_genesis(
            "node-a",
            store.clone(),
            "recovery-validation",
            validators.clone(),
        )
        .await?;
        let decision = consensus.handle_event(ConsensusEvent::Tick).await?;
        let ConsensusCommand::StoreBlock(block) = &decision.commands[0] else {
            panic!("tick must propose a block");
        };
        let block = block.clone();
        assert!(store.insert(block.clone()).await?);
        drop(consensus);
        drop(store);

        let block_path = path.join(format!("{}.block", block.hash));
        let mut altered: Block = bincode::deserialize(&std::fs::read(&block_path)?)?;
        altered.body.push(0);
        std::fs::write(&block_path, bincode::serialize(&altered)?)?;

        assert!(
            SimpleConsensus::with_genesis(
                "node-a",
                ChainStore::open(&path).await?,
                "recovery-validation",
                validators,
            )
            .await
            .is_err()
        );
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

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
    async fn failed_persistence_does_not_advance_production_or_received_blocks()
    -> anyhow::Result<()> {
        let directory = std::env::temp_dir().join(format!(
            "consensus-write-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&directory)?;
        let path = directory.join("chain.bin");
        let store = ChainStore::open(&path).await?;
        let consensus = SimpleConsensus::with_genesis(
            "node-a",
            store.clone(),
            "write-failure",
            vec![validator_id_from_seed("node-a")],
        )
        .await?;

        for received in [false, true] {
            let previous_tip = store.tip().await.unwrap();
            let previous_path = path.join(format!("{}.block", previous_tip.hash));
            let previous_bytes = std::fs::read(&previous_path)?;
            let decision = consensus.handle_event(ConsensusEvent::Tick).await?;
            let ConsensusCommand::StoreBlock(candidate) = &decision.commands[0] else {
                panic!("expected proposed block");
            };
            let candidate = candidate.clone();
            let event = if received {
                ConsensusEvent::BlockReceived {
                    peer_id: PeerId::from("peer-a"),
                    block: candidate.clone(),
                }
            } else {
                ConsensusEvent::Tick
            };
            // A directory at the temporary-file path forces a real write failure.
            std::fs::create_dir(path.join(format!("{}.tmp", candidate.hash)))?;
            let decision = consensus.handle_event(event.clone()).await?;
            let ConsensusCommand::StoreBlock(block) = &decision.commands[0] else {
                panic!("expected store command");
            };
            assert!(store.insert(block.clone()).await.is_err());
            assert_eq!(store.tip().await, Some(previous_tip.clone()));
            assert!(store.get(&candidate.hash).await.is_none());
            assert_eq!(std::fs::read(&previous_path)?, previous_bytes);
            assert_eq!(
                ChainStore::open(&path).await?.tip().await,
                Some(previous_tip)
            );

            std::fs::remove_dir(path.join(format!("{}.tmp", candidate.hash)))?;
            let retry = consensus.handle_event(event).await?;
            let ConsensusCommand::StoreBlock(block) = &retry.commands[0] else {
                panic!("retry must still accept the same block");
            };
            assert_eq!(block, &candidate);
            assert!(store.insert(block.clone()).await?);
            assert!(!store.insert(block.clone()).await?);
            assert_eq!(
                ChainStore::open(&path).await?.tip().await,
                Some(candidate.clone())
            );
            let next = consensus.handle_event(ConsensusEvent::Tick).await?;
            assert!(matches!(
                &next.commands[0],
                ConsensusCommand::StoreBlock(block)
                    if block.height == candidate.height + 1
                        && block.parent_hash == Some(candidate.hash.clone())
            ));
        }
        std::fs::remove_dir_all(path)?;
        std::fs::remove_dir(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn tick_produces_store_and_broadcast_commands() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let decision = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(matches!(decision.commands.as_slice(), [
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
        let decision = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert_eq!(decision.outcome, ConsensusOutcome::NoChange);
        assert!(decision.commands.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn resumes_production_after_the_persisted_tip() -> anyhow::Result<()> {
        let store = ChainStore::new();
        store.insert(signed_block("node-a", None, 0)).await?;
        let consensus = SimpleConsensus::new("node-a", store);

        let decision = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(
            matches!(decision.commands.as_slice(), [ConsensusCommand::StoreBlock(block), ..] if block.height == 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn received_block_advances_next_local_turn() -> anyhow::Result<()> {
        let store = ChainStore::new();
        let consensus = SimpleConsensus::with_validators(
            "node-b",
            store.clone(),
            vec![
                validator_id_from_seed("node-a"),
                validator_id_from_seed("node-b"),
            ],
        )?;
        let parent = signed_block("node-a", None, 0);

        let received = consensus
            .handle_event(ConsensusEvent::BlockReceived {
                peer_id: PeerId::from("node-a"),
                block: parent.clone(),
            })
            .await?;
        assert_eq!(received.outcome, ConsensusOutcome::Accepted);
        assert!(matches!(received.commands.as_slice(), [
            ConsensusCommand::StoreBlock(block),
            ConsensusCommand::ReportGoodPeer { .. },
        ] if block == &parent));
        assert!(
            consensus
                .handle_event(ConsensusEvent::Tick)
                .await?
                .commands
                .is_empty()
        );
        store.insert(parent.clone()).await?;
        let decision = consensus.handle_event(ConsensusEvent::Tick).await?;

        assert!(matches!(decision.commands.as_slice(), [
            ConsensusCommand::StoreBlock(block_a),
            ConsensusCommand::BroadcastBlock(block_b),
        ] if block_a == block_b && block_a.height == 1 && block_a.parent_hash == Some(parent.hash)));
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_valid_block_is_idempotent() -> anyhow::Result<()> {
        let store = ChainStore::new();
        let consensus = SimpleConsensus::new("node-a", store.clone());
        let block = signed_block("node-a", None, 0);
        store.insert(block.clone()).await?;

        let decision = consensus
            .handle_event(ConsensusEvent::BlockReceived {
                peer_id: PeerId::from("peer-a"),
                block: block.clone(),
            })
            .await?;

        assert_eq!(decision.outcome, ConsensusOutcome::AlreadyKnown);
        assert!(matches!(
            decision.commands.as_slice(),
            [ConsensusCommand::ReportGoodPeer { .. },]
        ));
        Ok(())
    }

    #[tokio::test]
    async fn defers_block_with_unknown_parent_without_penalizing_peer() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let block = signed_block("node-a", Some(BlockHash::from_u8(1)), 1);

        let decision = consensus
            .handle_event(ConsensusEvent::BlockReceived {
                peer_id: PeerId::from("peer-a"),
                block,
            })
            .await?;

        assert_eq!(decision.outcome, ConsensusOutcome::MissingParent);
        assert!(decision.commands.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_orphan_and_wrong_height_with_known_parent() -> anyhow::Result<()> {
        let store = ChainStore::new();
        let parent = signed_block("node-a", None, 0);
        store.insert(parent.clone()).await?;
        let consensus = SimpleConsensus::new("node-a", store);
        let mut invalid_orphan = signed_block("node-a", Some(BlockHash::from_u8(1)), 10);
        invalid_orphan.signature[0] ^= 1;
        invalid_orphan.hash = block_hash(&invalid_orphan);
        let wrong_height = signed_block("node-a", Some(parent.hash), 10);

        for block in [invalid_orphan, wrong_height] {
            let decision = consensus
                .handle_event(ConsensusEvent::BlockReceived {
                    peer_id: PeerId::from("peer-a"),
                    block,
                })
                .await?;
            assert_eq!(decision.outcome, ConsensusOutcome::Rejected);
            assert!(matches!(
                decision.commands.as_slice(),
                [ConsensusCommand::ReportBadPeer { .. }]
            ));
        }
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
            let decision = consensus
                .handle_event(ConsensusEvent::BlockReceived {
                    peer_id: PeerId::from("peer-a"),
                    block,
                })
                .await?;
            assert_eq!(decision.outcome, ConsensusOutcome::Rejected);
            assert!(matches!(
                decision.commands.as_slice(),
                [ConsensusCommand::ReportBadPeer { .. }]
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn announcement_does_not_request_missing_block() -> anyhow::Result<()> {
        let consensus = SimpleConsensus::new("node-a", ChainStore::new());
        let block_hash = BlockHash::from_u8(7);
        let decision = consensus
            .handle_event(ConsensusEvent::BlockAnnouncement {
                peer_id: PeerId::from("peer-a"),
                announcement: BlockAnnouncement {
                    block_hash: block_hash.clone(),
                    height: 1,
                },
            })
            .await?;

        assert!(decision.commands.is_empty());
        Ok(())
    }
}
