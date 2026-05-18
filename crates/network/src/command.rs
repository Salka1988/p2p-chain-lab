use libp2p_mini_chain_chain::block::{Block, BlockHash};
use libp2p_mini_chain_protocol::messages::Transaction;

use crate::PeerId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkCommand {
    BroadcastTransaction(Transaction),
    RequestBlock {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    SendBlock {
        peer_id: PeerId,
        block: Block,
    },
    BroadcastBlock(Block),
}
