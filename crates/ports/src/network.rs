use crate::PeerId;
use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_protocol::messages::{ChainTip, Transaction};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkCommand {
    BroadcastTransaction(Transaction),
    RequestBlock {
        peer_id: PeerId,
        block_hash: BlockHash,
    },
    SendBlock {
        peer_id: PeerId,
        block_hash: BlockHash,
        block: Option<Block>,
    },
    BroadcastBlock(Block),
    BroadcastChainTip(ChainTip),
}

#[derive(Debug, Clone)]
pub struct NetworkHandle {
    command_tx: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    pub fn new(command_tx: mpsc::Sender<NetworkCommand>) -> Self {
        Self { command_tx }
    }

    async fn send(&self, command: NetworkCommand) -> anyhow::Result<()> {
        self.command_tx.send(command).await?;
        Ok(())
    }

    pub async fn broadcast_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastTransaction(tx)).await
    }

    pub async fn broadcast_block(&self, block: Block) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastBlock(block)).await
    }

    pub async fn broadcast_chain_tip(&self, tip: ChainTip) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastChainTip(tip)).await
    }

    pub async fn request_block(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        self.send(NetworkCommand::RequestBlock {
            peer_id,
            block_hash,
        })
        .await
    }

    pub async fn send_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()> {
        self.send(NetworkCommand::SendBlock {
            peer_id,
            block_hash: block.hash.clone(),
            block: Some(block),
        })
        .await
    }

    pub async fn send_block_not_found(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        self.send(NetworkCommand::SendBlock {
            peer_id,
            block_hash,
            block: None,
        })
        .await
    }
}
