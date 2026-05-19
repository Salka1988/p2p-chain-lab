use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::PeerId;

const DEFAULT_SCORE: i32 = 0;
const BAD_MESSAGE_PENALTY: i32 = 10;
const FAILED_REQUEST_PENALTY: i32 = 5;
const USEFUL_RESPONSE_REWARD: i32 = 1;
const BAN_THRESHOLD: i32 = -50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerScoreCommand {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    BadMessage { peer_id: PeerId, reason: String },
    FailedRequest { peer_id: PeerId, reason: String },
    UsefulResponse(PeerId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerScoreEvent {
    PeerBanned { peer_id: PeerId, reason: String },
}

#[async_trait]
pub trait PeerReputationPort: Clone + Send + Sync + 'static {
    async fn report_good_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()>;
    async fn report_bad_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct PeerScoreHandle {
    command_tx: mpsc::Sender<PeerScoreCommand>,
}

impl PeerScoreHandle {
    pub fn new(command_tx: mpsc::Sender<PeerScoreCommand>) -> Self {
        Self { command_tx }
    }

    pub async fn connected(&self, peer_id: PeerId) -> anyhow::Result<()> {
        self.send(PeerScoreCommand::PeerConnected(peer_id)).await
    }

    pub async fn disconnected(&self, peer_id: PeerId) -> anyhow::Result<()> {
        self.send(PeerScoreCommand::PeerDisconnected(peer_id)).await
    }

    pub async fn bad_message(
        &self,
        peer_id: PeerId,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.send(PeerScoreCommand::BadMessage {
            peer_id,
            reason: reason.into(),
        })
        .await
    }

    pub async fn failed_request(
        &self,
        peer_id: PeerId,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.send(PeerScoreCommand::FailedRequest {
            peer_id,
            reason: reason.into(),
        })
        .await
    }

    pub async fn useful_response(&self, peer_id: PeerId) -> anyhow::Result<()> {
        self.send(PeerScoreCommand::UsefulResponse(peer_id)).await
    }

    async fn send(&self, command: PeerScoreCommand) -> anyhow::Result<()> {
        self.command_tx.send(command).await?;
        Ok(())
    }
}

#[async_trait]
impl PeerReputationPort for PeerScoreHandle {
    async fn report_good_peer(&self, peer_id: PeerId, _reason: String) -> anyhow::Result<()> {
        self.useful_response(peer_id).await
    }

    async fn report_bad_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()> {
        self.bad_message(peer_id, reason).await
    }
}

pub struct PeerScoreActor {
    scores: HashMap<PeerId, i32>,
    banned_peers: HashSet<PeerId>,
    command_rx: mpsc::Receiver<PeerScoreCommand>,
    event_tx: mpsc::Sender<PeerScoreEvent>,
}

impl PeerScoreActor {
    pub fn new(
        command_rx: mpsc::Receiver<PeerScoreCommand>,
        event_tx: mpsc::Sender<PeerScoreEvent>,
    ) -> Self {
        Self {
            scores: HashMap::new(),
            banned_peers: HashSet::new(),
            command_rx,
            event_tx,
        }
    }

    pub async fn run(mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => break,
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    self.handle_command(command).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_command(&mut self, command: PeerScoreCommand) -> anyhow::Result<()> {
        match command {
            PeerScoreCommand::PeerConnected(peer_id) => {
                self.scores.entry(peer_id).or_insert(DEFAULT_SCORE);
            }
            PeerScoreCommand::PeerDisconnected(peer_id) => {
                self.scores.remove(&peer_id);
            }
            PeerScoreCommand::BadMessage { peer_id, reason } => {
                self.apply_delta(peer_id, -BAD_MESSAGE_PENALTY, reason)
                    .await?;
            }
            PeerScoreCommand::FailedRequest { peer_id, reason } => {
                self.apply_delta(peer_id, -FAILED_REQUEST_PENALTY, reason)
                    .await?;
            }
            PeerScoreCommand::UsefulResponse(peer_id) => {
                let score = self.scores.entry(peer_id).or_insert(DEFAULT_SCORE);
                *score += USEFUL_RESPONSE_REWARD;
            }
        }

        Ok(())
    }

    async fn apply_delta(
        &mut self,
        peer_id: PeerId,
        delta: i32,
        reason: String,
    ) -> anyhow::Result<()> {
        let score = self.scores.entry(peer_id.clone()).or_insert(DEFAULT_SCORE);
        *score += delta;

        if *score <= BAN_THRESHOLD && self.banned_peers.insert(peer_id.clone()) {
            self.event_tx
                .send(PeerScoreEvent::PeerBanned { peer_id, reason })
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emits_ban_event_when_score_crosses_threshold() {
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let actor = PeerScoreActor::new(command_rx, event_tx);
        let cancellation_token = CancellationToken::new();
        let actor_token = cancellation_token.child_token();
        let actor_task = tokio::spawn(actor.run(actor_token));
        let handle = PeerScoreHandle::new(command_tx);

        for _ in 0..5 {
            handle
                .bad_message(PeerId::from("peer-a"), "invalid message")
                .await
                .unwrap();
        }

        assert_eq!(
            event_rx.recv().await,
            Some(PeerScoreEvent::PeerBanned {
                peer_id: PeerId::from("peer-a"),
                reason: "invalid message".to_string(),
            })
        );

        cancellation_token.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn emits_ban_event_only_once() {
        let (command_tx, command_rx) = mpsc::channel(16);
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let actor = PeerScoreActor::new(command_rx, event_tx);
        let cancellation_token = CancellationToken::new();
        let actor_token = cancellation_token.child_token();
        let actor_task = tokio::spawn(actor.run(actor_token));
        let handle = PeerScoreHandle::new(command_tx);

        for _ in 0..6 {
            handle
                .bad_message(PeerId::from("peer-a"), "invalid message")
                .await
                .unwrap();
        }

        assert!(event_rx.recv().await.is_some());
        assert!(event_rx.try_recv().is_err());

        cancellation_token.cancel();
        actor_task.await.unwrap().unwrap();
    }
}
