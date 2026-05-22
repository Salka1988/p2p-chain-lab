use std::fmt;

use async_trait::async_trait;
use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_protocol::messages::{BlockAnnouncement, Transaction};

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
    BlockReceived {
        peer_id: PeerId,
        block: Block,
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

pub trait NetworkStatusPort: Clone + Send + Sync + 'static {
    fn listen_addr(&self) -> &str;
}

#[async_trait]
pub trait BlockRequestPort: Clone + Send + Sync + 'static {
    async fn request_block(&self, peer_id: PeerId, block_hash: BlockHash) -> anyhow::Result<()>;
}

#[async_trait]
pub trait BlockSendPort: Clone + Send + Sync + 'static {
    async fn send_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()>;
}

#[async_trait]
pub trait BlockBroadcastPort: Clone + Send + Sync + 'static {
    async fn broadcast_block(&self, block: Block) -> anyhow::Result<()>;
}

#[async_trait]
pub trait PeerReputationPort: Clone + Send + Sync + 'static {
    async fn report_good_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()>;
    async fn report_bad_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()>;
}
