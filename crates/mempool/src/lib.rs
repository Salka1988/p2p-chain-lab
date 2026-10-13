use std::{collections::HashMap, sync::Arc};

use p2p_chain_lib_protocol::messages::{Transaction, TransactionId};
use tokio::sync::Mutex;

const MAX_TRANSACTIONS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 64 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct Mempool {
    state: Arc<Mutex<MempoolState>>,
}

#[derive(Debug, Default)]
struct MempoolState {
    transactions: HashMap<TransactionId, Transaction>,
    total_bytes: usize,
}

impl Mempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, tx: Transaction) -> bool {
        let tx_bytes = tx.payload.len();
        if tx_bytes > MAX_TRANSACTION_BYTES {
            return false;
        }

        let mut state = self.state.lock().await;
        if state.transactions.contains_key(&tx.id)
            || state.transactions.len() >= MAX_TRANSACTIONS
            || state.total_bytes.saturating_add(tx_bytes) > MAX_TOTAL_BYTES
        {
            return false;
        }

        state.total_bytes += tx_bytes;
        state.transactions.insert(tx.id.clone(), tx);
        true
    }

    pub async fn contains(&self, tx_id: &TransactionId) -> bool {
        self.state.lock().await.transactions.contains_key(tx_id)
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.transactions.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(id: u8) -> Transaction {
        Transaction {
            id: TransactionId::from_u8(id),
            payload: vec![1],
        }
    }

    #[tokio::test]
    async fn insert_rejects_duplicates() {
        let mempool = Mempool::new();

        assert!(mempool.insert(tx(1)).await);
        assert!(!mempool.insert(tx(1)).await);
        assert_eq!(mempool.len().await, 1);
        assert!(mempool.contains(&TransactionId::from_u8(1)).await);
    }

    #[tokio::test]
    async fn insert_rejects_oversized_transactions() {
        let mempool = Mempool::new();

        assert!(
            !mempool
                .insert(Transaction {
                    id: TransactionId::from_u8(1),
                    payload: vec![0; MAX_TRANSACTION_BYTES + 1],
                })
                .await
        );
        assert!(mempool.is_empty().await);
    }
}
