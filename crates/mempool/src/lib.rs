use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use libp2p_mini_chain_protocol::messages::{Transaction, TransactionId};
use tokio::sync::Mutex;

#[async_trait]
pub trait MempoolPort: Clone + Send + Sync + 'static {
    async fn insert_transaction(&self, tx: Transaction) -> bool;
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

#[async_trait]
impl MempoolPort for Mempool {
    async fn insert_transaction(&self, tx: Transaction) -> bool {
        self.insert(tx).await
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
