use crate::PeerId;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerScoreCommand {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    BadMessage { peer_id: PeerId, reason: String },
    FailedRequest { peer_id: PeerId, reason: String },
    UsefulResponse(PeerId),
}

#[derive(Debug, Clone)]
pub struct PeerReputationHandle {
    command_tx: mpsc::Sender<PeerScoreCommand>,
}

impl PeerReputationHandle {
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
        match self.command_tx.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                tracing::warn!(
                    ?command,
                    "dropping peer score command because queue is full"
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                // The scorer is a best-effort telemetry worker. During shutdown its
                // cancellation token can win the race with final connection
                // events, so losing this update must not keep the network task alive.
                tracing::debug!(
                    ?command,
                    "dropping peer score command because actor stopped"
                );
                Ok(())
            }
        }
    }
    pub async fn report_good_peer(&self, peer_id: PeerId, _reason: String) -> anyhow::Result<()> {
        self.useful_response(peer_id).await
    }

    pub async fn report_bad_peer(&self, peer_id: PeerId, reason: String) -> anyhow::Result<()> {
        self.bad_message(peer_id, reason).await
    }
}
