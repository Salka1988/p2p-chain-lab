use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_ports::PeerId;
use p2p_chain_lib_protocol::messages::Transaction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NetworkCommand {
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
