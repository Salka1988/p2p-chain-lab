use p2p_chain_lab_chain::block::Block;

use crate::messages::{BlockAnnouncement, Transaction};

pub fn validate_transaction(tx: &Transaction) -> anyhow::Result<()> {
    anyhow::ensure!(!tx.id.is_zero(), "transaction id cannot be zero");
    anyhow::ensure!(
        !tx.payload.is_empty(),
        "transaction payload cannot be empty"
    );
    Ok(())
}

pub fn validate_block_announcement(announcement: &BlockAnnouncement) -> anyhow::Result<()> {
    anyhow::ensure!(
        !announcement.block_hash.is_zero(),
        "block hash cannot be zero"
    );
    Ok(())
}

pub fn validate_block(block: &Block) -> anyhow::Result<()> {
    anyhow::ensure!(!block.hash.is_zero(), "block hash cannot be zero");
    anyhow::ensure!(!block.body.is_empty(), "block body cannot be empty");

    match (block.parent_hash.as_ref(), block.height) {
        (None, 0) => Ok(()),
        (Some(parent_hash), 1..) => {
            anyhow::ensure!(!parent_hash.is_zero(), "parent hash cannot be zero");
            Ok(())
        }
        (None, _) => anyhow::bail!("non-genesis block must have parent hash"),
        (Some(_), 0) => anyhow::bail!("genesis block cannot have parent hash"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::TransactionId;
    use p2p_chain_lab_chain::block::{Block, BlockHash};

    #[test]
    fn validates_transaction_shape() {
        assert!(
            validate_transaction(&Transaction {
                id: TransactionId::from_u8(1),
                payload: vec![1],
            })
            .is_ok()
        );

        assert!(
            validate_transaction(&Transaction {
                id: TransactionId::zero(),
                payload: vec![1],
            })
            .is_err()
        );
    }

    #[test]
    fn validates_block_announcement_shape() {
        assert!(
            validate_block_announcement(&BlockAnnouncement {
                block_hash: BlockHash::from_u8(1),
                height: 1,
            })
            .is_ok()
        );

        assert!(
            validate_block_announcement(&BlockAnnouncement {
                block_hash: BlockHash::zero(),
                height: 1,
            })
            .is_err()
        );
    }

    #[test]
    fn validates_block_shape() {
        assert!(
            validate_block(&Block {
                hash: BlockHash::from_u8(1),
                parent_hash: None,
                height: 0,
                body: vec![1],
            })
            .is_ok()
        );

        assert!(
            validate_block(&Block {
                hash: BlockHash::from_u8(2),
                parent_hash: Some(BlockHash::from_u8(1)),
                height: 1,
                body: vec![1],
            })
            .is_ok()
        );

        assert!(
            validate_block(&Block {
                hash: BlockHash::zero(),
                parent_hash: None,
                height: 0,
                body: vec![1],
            })
            .is_err()
        );

        assert!(
            validate_block(&Block {
                hash: BlockHash::from_u8(1),
                parent_hash: None,
                height: 1,
                body: vec![1],
            })
            .is_err()
        );
    }
}
