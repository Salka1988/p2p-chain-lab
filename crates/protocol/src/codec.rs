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
    use libp2p_mini_chain_chain::block::{Block, BlockHash};

    fn block(hash: u8) -> Block {
        Block {
            hash: BlockHash::from_u8(hash),
            parent_hash: Some(BlockHash::from_u8(9)),
            height: 2,
            body: vec![1, 2, 3],
        }
    }

    #[test]
    fn transaction_round_trips() {
        let message = Transaction {
            id: TransactionId::from_u8(1),
            payload: vec![1, 2, 3],
        };

        let encoded = encode(&message).unwrap();
        let decoded: Transaction = decode(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn block_announcement_round_trips() {
        let message = BlockAnnouncement {
            block_hash: BlockHash::from_u8(1),
            height: 1,
        };

        let encoded = encode(&message).unwrap();
        let decoded: BlockAnnouncement = decode(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn block_request_round_trips() {
        let message = BlockRequest {
            block_hash: BlockHash::from_u8(1),
        };

        let encoded = encode(&message).unwrap();
        let decoded: BlockRequest = decode(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn block_response_round_trips() {
        let message = BlockResponse { block: block(1) };

        let encoded = encode(&message).unwrap();
        let decoded: BlockResponse = decode(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn rejects_invalid_bytes() {
        let result: anyhow::Result<Transaction> = decode(&[0, 1, 2]);

        assert!(result.is_err());
    }
}
