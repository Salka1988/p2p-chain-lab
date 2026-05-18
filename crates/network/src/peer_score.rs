use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::PeerId;

#[derive(Debug, Clone, Default)]
pub struct PeerScore {
    scores: Arc<Mutex<HashMap<PeerId, i32>>>,
}

impl PeerScore {
    pub async fn record_misbehavior(&self, peer_id: PeerId) {
        let mut scores = self.scores.lock().await;
        *scores.entry(peer_id).or_default() -= 1;
    }
}
