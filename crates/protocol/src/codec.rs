use serde::{Serialize, de::DeserializeOwned};

pub fn encode<T>(message: &T) -> anyhow::Result<Vec<u8>>
where
    T: Serialize,
{
    Ok(bincode::serialize(message)?)
}

pub fn decode<T>(bytes: &[u8]) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    Ok(bincode::deserialize(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{
        BlockAnnouncement, BlockRequest, BlockResponse, Transaction, TransactionId,
    };
    use p2p_chain_lib_chain::block::{Block, BlockHash};

    fn block(hash: u8) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: Some(BlockHash::from_u8(9)),
            height: 2,
            producer: "validator".to_string(),
            signature: vec![1; 64],
            body: vec![1, 2, 3],
        }
    }

    #[test]
    fn transaction_round_trips() -> anyhow::Result<()> {
        let message = Transaction {
            id: TransactionId::from_u8(1),
            payload: vec![1, 2, 3],
        };

        let encoded = encode(&message)?;
        let decoded: Transaction = decode(&encoded)?;

        assert_eq!(decoded, message);
        Ok(())
    }

    #[test]
    fn block_announcement_round_trips() -> anyhow::Result<()> {
        let message = BlockAnnouncement {
            block_hash: BlockHash::from_u8(1),
            height: 1,
        };

        let encoded = encode(&message)?;
        let decoded: BlockAnnouncement = decode(&encoded)?;

        assert_eq!(decoded, message);
        Ok(())
    }

    #[test]
    fn block_request_round_trips() -> anyhow::Result<()> {
        let message = BlockRequest {
            block_hash: BlockHash::from_u8(1),
        };

        let encoded = encode(&message)?;
        let decoded: BlockRequest = decode(&encoded)?;

        assert_eq!(decoded, message);
        Ok(())
    }

    #[test]
    fn block_response_round_trips() -> anyhow::Result<()> {
        let message = BlockResponse {
            block_hash: BlockHash::from_u8(1),
            block: Some(block(1)),
        };

        let encoded = encode(&message)?;
        let decoded: BlockResponse = decode(&encoded)?;

        assert_eq!(decoded, message);
        Ok(())
    }

    #[test]
    fn rejects_invalid_bytes() {
        let result: anyhow::Result<Transaction> = decode(&[0, 1, 2]);

        assert!(result.is_err());
    }
}
