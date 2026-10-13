use p2p_chain_lib_chain::block::{Block, BlockHash};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId([u8; 32]);

impl TransactionId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_u8(value: u8) -> Self {
        Self([value; 32])
    }

    pub fn zero() -> Self {
        Self([0; 32])
    }

    pub fn is_zero(&self) -> bool {
        self.0 == [0; 32]
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Transaction {
    pub id: TransactionId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockAnnouncement {
    pub block_hash: BlockHash,
    pub height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRequest {
    pub block_hash: BlockHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockResponse {
    pub block_hash: BlockHash,
    pub block: Option<Block>,
}
