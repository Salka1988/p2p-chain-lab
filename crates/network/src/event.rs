use libp2p_mini_chain_chain::block::{Block, BlockHash};
use libp2p_mini_chain_protocol::messages::{BlockAnnouncement, Transaction};

use crate::{PeerAddress, PeerId};

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
