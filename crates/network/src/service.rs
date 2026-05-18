use std::{collections::HashMap, future::Future};

use anyhow::Context;
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder, futures::StreamExt, gossipsub, identity::Keypair,
    request_response, swarm::SwarmEvent,
};
use libp2p_mini_chain_chain::block::{Block, BlockHash};
use libp2p_mini_chain_protocol::{
    codec,
    messages::{BlockAnnouncement, BlockRequest, BlockResponse, Transaction},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    PeerAddress, PeerId,
    behaviour::{BLOCK_ANNOUNCEMENT_TOPIC, NetworkBehaviour, NetworkBehaviourEvent, TX_TOPIC},
    command::NetworkCommand,
    event::NetworkEvent,
    peer_score::{PeerScoreActor, PeerScoreEvent, PeerScoreHandle},
};

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
}

#[derive(Debug, Clone)]
pub struct NetworkHandle {
    command_tx: mpsc::Sender<NetworkCommand>,
    listen_addr: PeerAddress,
}

pub trait NetworkStatusPort: Clone + Send + Sync + 'static {
    fn listen_addr(&self) -> &str;
}

pub trait BlockRequestPort: Clone + Send + Sync + 'static {
    fn request_block(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_;
}

pub trait BlockSendPort: Clone + Send + Sync + 'static {
    fn send_block(
        &self,
        peer_id: PeerId,
        block: Block,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_;
}

impl NetworkHandle {
    pub fn new(command_tx: mpsc::Sender<NetworkCommand>) -> Self {
        Self::with_listen_addr(command_tx, PeerAddress::from("/ip4/127.0.0.1/tcp/0"))
    }

    pub fn with_listen_addr(
        command_tx: mpsc::Sender<NetworkCommand>,
        listen_addr: PeerAddress,
    ) -> Self {
        Self {
            command_tx,
            listen_addr,
        }
    }

    pub fn listen_addr(&self) -> &str {
        self.listen_addr.as_str()
    }

    pub async fn send(&self, command: NetworkCommand) -> anyhow::Result<()> {
        self.command_tx.send(command).await?;
        Ok(())
    }
}

impl NetworkStatusPort for NetworkHandle {
    fn listen_addr(&self) -> &str {
        self.listen_addr()
    }
}

impl BlockRequestPort for NetworkHandle {
    fn request_block(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_ {
        self.send(NetworkCommand::RequestBlock {
            peer_id,
            block_hash,
        })
    }
}

impl BlockSendPort for NetworkHandle {
    fn send_block(
        &self,
        peer_id: PeerId,
        block: Block,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + '_ {
        self.send(NetworkCommand::SendBlock { peer_id, block })
    }
}

pub struct NetworkService {
    config: NetworkConfig,
    command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
}

type BlockResponseChannel = request_response::ResponseChannel<BlockResponse>;

impl NetworkService {
    pub fn new(
        config: NetworkConfig,
        command_rx: mpsc::Receiver<NetworkCommand>,
        event_tx: mpsc::Sender<NetworkEvent>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            command_rx,
            event_tx,
        })
    }

    pub async fn run(mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut swarm = self.build_swarm().await?;
        let mut pending_block_responses = HashMap::new();
        let (peer_score_command_tx, peer_score_command_rx) = mpsc::channel(1024);
        let (peer_score_event_tx, mut peer_score_event_rx) = mpsc::channel(1024);
        let peer_score = PeerScoreHandle::new(peer_score_command_tx);
        let peer_score_cancellation_token = cancellation_token.child_token();
        let peer_score_task = tokio::spawn(
            PeerScoreActor::new(peer_score_command_rx, peer_score_event_tx)
                .run(peer_score_cancellation_token),
        );

        tracing::info!(
            listen_addr = %self.config.listen_addr,
            "network service started"
        );

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::info!("network service shutdown requested");
                    break;
                }
                command = self.command_rx.recv() => {
                    match command {
                        Some(command) => {
                            self.handle_command(
                                &mut swarm,
                                &mut pending_block_responses,
                                command,
                            ).await?;
                        }
                        None => break,
                    }
                }
                event = swarm.select_next_some() => {
                    self.handle_swarm_event(
                        &mut swarm,
                        &mut pending_block_responses,
                        &peer_score,
                        event,
                    ).await?;
                }
                event = peer_score_event_rx.recv() => {
                    match event {
                        Some(event) => self.handle_peer_score_event(&mut swarm, event).await?,
                        None => break,
                    }
                }
            }
        }

        drop(self.event_tx);
        cancellation_token.cancel();
        peer_score_task.await??;
        tracing::info!("network service stopped");
        Ok(())
    }

    async fn build_swarm(&self) -> anyhow::Result<Swarm<NetworkBehaviour>> {
        let listen_addr: Multiaddr = self
            .config
            .listen_addr
            .as_str()
            .parse()
            .context("invalid network listen address")?;

        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_behaviour(|keypair: &Keypair| NetworkBehaviour::new(keypair.clone()))?
            .build();

        swarm.listen_on(listen_addr)?;

        for peer in &self.config.bootstrap_peers {
            let peer_addr: Multiaddr = peer
                .as_str()
                .parse()
                .with_context(|| format!("invalid bootstrap peer address: {peer}"))?;
            swarm.dial(peer_addr)?;
        }

        Ok(swarm)
    }

    async fn handle_command(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_responses: &mut HashMap<(PeerId, BlockHash), BlockResponseChannel>,
        command: NetworkCommand,
    ) -> anyhow::Result<()> {
        match command {
            NetworkCommand::BroadcastTransaction(tx) => {
                let payload = Self::encode_transaction_broadcast(&tx)?;
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(TX_TOPIC), payload)?;
            }
            NetworkCommand::BroadcastBlock(block) => {
                let payload = Self::encode_block_announcement(&block)?;
                swarm.behaviour_mut().gossipsub.publish(
                    gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC),
                    payload,
                )?;
            }
            NetworkCommand::RequestBlock {
                peer_id,
                block_hash,
            } => {
                let libp2p_peer_id = peer_id
                    .as_str()
                    .parse()
                    .with_context(|| format!("invalid peer id for block request: {peer_id}"))?;
                swarm
                    .behaviour_mut()
                    .block_request_response
                    .send_request(&libp2p_peer_id, BlockRequest { block_hash });
            }
            NetworkCommand::SendBlock { peer_id, block } => {
                let Some(channel) =
                    pending_block_responses.remove(&(peer_id.clone(), block.hash.clone()))
                else {
                    tracing::warn!(%peer_id, block_hash = ?block.hash, "no pending block response channel");
                    return Ok(());
                };

                let response = BlockResponse { block };
                if swarm
                    .behaviour_mut()
                    .block_request_response
                    .send_response(channel, response)
                    .is_err()
                {
                    tracing::warn!(%peer_id, "failed to queue block response");
                }
            }
        }

        Ok(())
    }

    async fn handle_swarm_event(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_responses: &mut HashMap<(PeerId, BlockHash), BlockResponseChannel>,
        peer_score: &PeerScoreHandle,
        event: SwarmEvent<NetworkBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "network listening");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                let peer_id = PeerId::from(peer_id.to_string());
                peer_score.connected(peer_id.clone()).await?;
                self.event_tx
                    .send(NetworkEvent::PeerConnected(peer_id))
                    .await?;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                let peer_id = PeerId::from(peer_id.to_string());
                peer_score.disconnected(peer_id.clone()).await?;
                self.event_tx
                    .send(NetworkEvent::PeerDisconnected(peer_id))
                    .await?;
            }
            SwarmEvent::ConnectionClosed { .. } => {
                // Other connections to the same peer are still open.
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::warn!(?peer_id, ?error, "outgoing connection failed");
            }
            SwarmEvent::IncomingConnectionError { peer_id, error, .. } => {
                tracing::warn!(?peer_id, ?error, "incoming connection failed");
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Gossipsub(event)) => {
                self.handle_gossipsub_event(peer_score, event).await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::BlockRequestResponse(event)) => {
                self.handle_block_request_response_event(
                    swarm,
                    pending_block_responses,
                    peer_score,
                    event,
                )
                .await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Ping(event)) => {
                tracing::debug!(?event, "ping event");
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_peer_score_event(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        event: PeerScoreEvent,
    ) -> anyhow::Result<()> {
        match event {
            PeerScoreEvent::PeerBanned { peer_id, reason } => {
                tracing::warn!(%peer_id, %reason, "peer banned");
                if let Ok(libp2p_peer_id) = peer_id.as_str().parse()
                    && let Err(error) = swarm.disconnect_peer_id(libp2p_peer_id)
                {
                    tracing::debug!(%peer_id, ?error, "failed to disconnect banned peer");
                }
                self.event_tx
                    .send(NetworkEvent::PeerMisbehaved { peer_id, reason })
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_gossipsub_event(
        &self,
        peer_score: &PeerScoreHandle,
        event: gossipsub::Event,
    ) -> anyhow::Result<()> {
        let gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        } = event
        else {
            return Ok(());
        };

        let peer_id = PeerId::from(propagation_source.to_string());
        if message.topic == gossipsub::IdentTopic::new(TX_TOPIC).hash() {
            let tx = match codec::decode::<Transaction>(&message.data) {
                Ok(tx) => tx,
                Err(error) => {
                    peer_score
                        .bad_message(peer_id, format!("invalid transaction payload: {error}"))
                        .await?;
                    return Ok(());
                }
            };
            self.event_tx
                .send(NetworkEvent::TransactionReceived { peer_id, tx })
                .await?;
        } else if message.topic == gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC).hash() {
            let announcement = match codec::decode::<BlockAnnouncement>(&message.data) {
                Ok(announcement) => announcement,
                Err(error) => {
                    peer_score
                        .bad_message(
                            peer_id,
                            format!("invalid block announcement payload: {error}"),
                        )
                        .await?;
                    return Ok(());
                }
            };
            self.event_tx
                .send(NetworkEvent::BlockAnnouncementReceived {
                    peer_id,
                    announcement,
                })
                .await?;
        }

        Ok(())
    }

    async fn handle_block_request_response_event(
        &self,
        _swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_responses: &mut HashMap<(PeerId, BlockHash), BlockResponseChannel>,
        peer_score: &PeerScoreHandle,
        event: request_response::Event<BlockRequest, BlockResponse>,
    ) -> anyhow::Result<()> {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let peer_id = PeerId::from(peer.to_string());
                    pending_block_responses
                        .insert((peer_id.clone(), request.block_hash.clone()), channel);
                    self.event_tx
                        .send(NetworkEvent::BlockRequestReceived {
                            peer_id,
                            block_hash: request.block_hash,
                        })
                        .await?;
                }
                request_response::Message::Response { response, .. } => {
                    peer_score
                        .useful_response(PeerId::from(peer.to_string()))
                        .await?;
                    self.event_tx
                        .send(NetworkEvent::BlockReceived {
                            peer_id: PeerId::from(peer.to_string()),
                            block: response.block,
                        })
                        .await?;
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, ?error, "block request failed");
                peer_score
                    .failed_request(
                        PeerId::from(peer.to_string()),
                        format!("block request failed: {error}"),
                    )
                    .await?;
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, ?error, "inbound block request failed");
                peer_score
                    .failed_request(
                        PeerId::from(peer.to_string()),
                        format!("inbound block request failed: {error}"),
                    )
                    .await?;
            }
            request_response::Event::ResponseSent { peer, .. } => {
                tracing::debug!(peer_id = %peer, "block response sent");
            }
        }

        Ok(())
    }

    fn encode_transaction_broadcast(tx: &Transaction) -> anyhow::Result<Vec<u8>> {
        codec::encode(tx)
    }

    fn encode_block_announcement(block: &Block) -> anyhow::Result<Vec<u8>> {
        codec::encode(&BlockAnnouncement {
            block_hash: block.hash.clone(),
            height: block.height,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_mini_chain_chain::block::{Block, BlockHash};
    use libp2p_mini_chain_protocol::messages::{BlockAnnouncement, Transaction, TransactionId};

    #[test]
    fn broadcast_transaction_encodes_protocol_payload() {
        let tx = Transaction {
            id: TransactionId::from_u8(1),
            payload: vec![1, 2, 3],
        };

        let payload = NetworkService::encode_transaction_broadcast(&tx).unwrap();
        let decoded: Transaction = codec::decode(&payload).unwrap();

        assert_eq!(decoded, tx);
    }

    #[test]
    fn broadcast_block_encodes_block_announcement_payload() {
        let block = Block {
            hash: BlockHash::from_u8(1),
            parent_hash: None,
            height: 10,
            body: vec![1, 2, 3],
        };

        let payload = NetworkService::encode_block_announcement(&block).unwrap();
        let decoded: BlockAnnouncement = codec::decode(&payload).unwrap();

        assert_eq!(decoded.block_hash, BlockHash::from_u8(1));
        assert_eq!(decoded.height, 10);
    }
}
