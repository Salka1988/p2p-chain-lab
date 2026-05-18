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
    PeerId,
    behaviour::{BLOCK_ANNOUNCEMENT_TOPIC, NetworkBehaviour, NetworkBehaviourEvent, TX_TOPIC},
    command::NetworkCommand,
    event::NetworkEvent,
};

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub listen_addr: String,
    pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkHandle {
    command_tx: mpsc::Sender<NetworkCommand>,
    listen_addr: String,
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
        Self::with_listen_addr(command_tx, "/ip4/127.0.0.1/tcp/0".to_string())
    }

    pub fn with_listen_addr(command_tx: mpsc::Sender<NetworkCommand>, listen_addr: String) -> Self {
        Self {
            command_tx,
            listen_addr,
        }
    }

    pub fn listen_addr(&self) -> &str {
        &self.listen_addr
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
                        event,
                    ).await?;
                }
            }
        }

        drop(self.event_tx);
        tracing::info!("network service stopped");
        Ok(())
    }

    async fn build_swarm(&self) -> anyhow::Result<Swarm<NetworkBehaviour>> {
        let listen_addr: Multiaddr = self
            .config
            .listen_addr
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
        event: SwarmEvent<NetworkBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "network listening");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                let peer_id = peer_id.to_string();
                self.event_tx
                    .send(NetworkEvent::PeerConnected(peer_id))
                    .await?;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                self.event_tx
                    .send(NetworkEvent::PeerDisconnected(peer_id.to_string()))
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
                self.handle_gossipsub_event(event).await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::BlockRequestResponse(event)) => {
                self.handle_block_request_response_event(swarm, pending_block_responses, event)
                    .await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Ping(event)) => {
                tracing::debug!(?event, "ping event");
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_gossipsub_event(&self, event: gossipsub::Event) -> anyhow::Result<()> {
        let gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        } = event
        else {
            return Ok(());
        };

        let peer_id = propagation_source.to_string();
        if message.topic == gossipsub::IdentTopic::new(TX_TOPIC).hash() {
            let tx = codec::decode::<Transaction>(&message.data)?;
            self.event_tx
                .send(NetworkEvent::TransactionReceived { peer_id, tx })
                .await?;
        } else if message.topic == gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC).hash() {
            let announcement = codec::decode::<BlockAnnouncement>(&message.data)?;
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
        event: request_response::Event<BlockRequest, BlockResponse>,
    ) -> anyhow::Result<()> {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let peer_id = peer.to_string();
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
                    self.event_tx
                        .send(NetworkEvent::BlockReceived {
                            peer_id: peer.to_string(),
                            block: response.block,
                        })
                        .await?;
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, ?error, "block request failed");
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, ?error, "inbound block request failed");
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
