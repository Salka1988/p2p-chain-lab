use std::{collections::HashMap, future::Future, sync::Arc};

use libp2p_mini_chain_protocol::messages::{Transaction, TransactionId};
use tokio::sync::Mutex;

pub trait MempoolPort: Clone + Send + Sync + 'static {
    fn insert_transaction(&self, tx: Transaction) -> impl Future<Output = bool> + Send + '_;
}

#[derive(Debug, Clone, Default)]
pub struct Mempool {
    transactions: Arc<Mutex<HashMap<TransactionId, Transaction>>>,
}

impl Mempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, tx: Transaction) -> bool {
        self.transactions
            .lock()
            .await
            .insert(tx.id.clone(), tx)
            .is_none()
    }

    pub async fn contains(&self, tx_id: &TransactionId) -> bool {
        self.transactions.lock().await.contains_key(tx_id)
    }

    pub async fn len(&self) -> usize {
        self.transactions.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl MempoolPort for Mempool {
    fn insert_transaction(&self, tx: Transaction) -> impl Future<Output = bool> + Send + '_ {
        self.insert(tx)
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
}
