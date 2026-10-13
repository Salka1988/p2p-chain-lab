use std::fmt;

mod network;
mod reputation;
pub use network::{NetworkCommand, NetworkHandle};
pub use reputation::{PeerReputationHandle, PeerScoreCommand};

use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_protocol::messages::{BlockAnnouncement, ChainTip, Transaction};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for PeerId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for PeerId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerAddress(String);

impl PeerAddress {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for PeerAddress {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for PeerAddress {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug)]
pub enum NetworkEvent {
    Listening(PeerAddress),
    TransactionReceived {
        peer_id: PeerId,
        tx: Transaction,
    },
    BlockAnnouncementReceived {
        peer_id: PeerId,
        announcement: BlockAnnouncement,
    },
    ChainTipReceived {
        peer_id: PeerId,
        tip: ChainTip,
    },
    BlockReceived {
        peer_id: PeerId,
        block: Block,
    },
    BlockNotFound {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    BlockRequestFailed {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    BlockRequestReceived {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    PeerDiscovered(PeerId),
    PeerMisbehaved {
        peer_id: PeerId,
        reason: String,
    },
}
