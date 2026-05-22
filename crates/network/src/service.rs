use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder, futures::StreamExt, gossipsub, identity::Keypair, kad,
    multiaddr, request_response, swarm::SwarmEvent,
};
use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_ports::{
    BlockBroadcastPort, BlockRequestPort, BlockSendPort, NetworkEvent, NetworkStatusPort, PeerId,
};
use p2p_chain_lib_protocol::{
    codec,
    messages::{BlockAnnouncement, BlockRequest, BlockResponse, Transaction},
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    PeerAddress,
    behaviour::{BLOCK_ANNOUNCEMENT_TOPIC, NetworkBehaviour, NetworkBehaviourEvent, TX_TOPIC},
    command::NetworkCommand,
    peer_score::{PeerScoreEvent, PeerScoreHandle},
};

const DEFAULT_COMMAND_BUFFER: usize = 1024;

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
    pub node_key_seed: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkHandle {
    command_tx: mpsc::Sender<NetworkCommand>,
    listen_addr: PeerAddress,
}

impl NetworkHandle {
    fn with_listen_addr(
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

    async fn send(&self, command: NetworkCommand) -> anyhow::Result<()> {
        self.command_tx.send(command).await?;
        Ok(())
    }

    pub async fn broadcast_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastTransaction(tx)).await
    }

    pub async fn broadcast_block(&self, block: Block) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastBlock(block)).await
    }

    pub async fn request_block(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        self.send(NetworkCommand::RequestBlock {
            peer_id,
            block_hash,
        })
        .await
    }

    pub async fn send_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()> {
        self.send(NetworkCommand::SendBlock { peer_id, block })
            .await
    }
}

impl NetworkStatusPort for NetworkHandle {
    fn listen_addr(&self) -> &str {
        self.listen_addr()
    }
}

#[async_trait]
impl BlockRequestPort for NetworkHandle {
    async fn request_block(&self, peer_id: PeerId, block_hash: BlockHash) -> anyhow::Result<()> {
        self.request_block(peer_id, block_hash).await
    }
}

#[async_trait]
impl BlockSendPort for NetworkHandle {
    async fn send_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()> {
        self.send_block(peer_id, block).await
    }
}

#[async_trait]
impl BlockBroadcastPort for NetworkHandle {
    async fn broadcast_block(&self, block: Block) -> anyhow::Result<()> {
        self.broadcast_block(block).await
    }
}

pub struct NetworkService {
    config: NetworkConfig,
    command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    peer_score: PeerScoreHandle,
    peer_score_event_rx: mpsc::Receiver<PeerScoreEvent>,
}

type BlockResponseChannel = request_response::ResponseChannel<BlockResponse>;

impl NetworkService {
    pub fn new(
        config: NetworkConfig,
        event_tx: mpsc::Sender<NetworkEvent>,
        peer_score: PeerScoreHandle,
        peer_score_event_rx: mpsc::Receiver<PeerScoreEvent>,
    ) -> anyhow::Result<(Self, NetworkHandle)> {
        let (command_tx, command_rx) = mpsc::channel(DEFAULT_COMMAND_BUFFER);
        let handle = NetworkHandle::with_listen_addr(command_tx, config.listen_addr.clone());
        let service = Self {
            config,
            command_rx,
            event_tx,
            peer_score,
            peer_score_event_rx,
        };

        Ok((service, handle))
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
                event = self.peer_score_event_rx.recv() => {
                    match event {
                        Some(event) => self.handle_peer_score_event(&mut swarm, event).await?,
                        None => break,
                    }
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
            .as_str()
            .parse()
            .context("invalid network listen address")?;

        let identity = self.build_identity()?;
        let local_peer_id = identity.public().to_peer_id();

        let mut swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_dns()?
            .with_behaviour(
                |keypair: &Keypair| -> Result<
                    NetworkBehaviour,
                    Box<dyn std::error::Error + Send + Sync>,
                > {
                    NetworkBehaviour::new(keypair.clone(), keypair.public().to_peer_id())
                        .map_err(Into::into)
                },
            )?
            .build();

        tracing::info!(%local_peer_id, "local peer id");
        swarm.listen_on(listen_addr)?;

        for peer in &self.config.bootstrap_peers {
            let peer_addr: Multiaddr = peer
                .as_str()
                .parse()
                .with_context(|| format!("invalid bootstrap peer address: {peer}"))?;
            if let Some(peer_id) = Self::peer_id_from_addr(&peer_addr) {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr.clone());
            } else {
                tracing::debug!(
                    %peer,
                    "bootstrap peer address has no /p2p/<peer-id>; dialing without kademlia address registration"
                );
            }
            swarm.dial(peer_addr)?;
        }

        if !self.config.bootstrap_peers.is_empty() {
            match swarm.behaviour_mut().kademlia.bootstrap() {
                Ok(query_id) => tracing::info!(?query_id, "kademlia bootstrap started"),
                Err(error) => tracing::warn!(?error, "kademlia bootstrap could not start"),
            }
        }

        Ok(swarm)
    }

    fn peer_id_from_addr(addr: &Multiaddr) -> Option<libp2p::PeerId> {
        addr.iter().find_map(|protocol| match protocol {
            multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
    }

    fn build_identity(&self) -> anyhow::Result<Keypair> {
        let Some(seed) = &self.config.node_key_seed else {
            return Ok(Keypair::generate_ed25519());
        };

        let secret_key = Sha256::digest(seed.as_bytes());
        Keypair::ed25519_from_bytes(secret_key.to_vec())
            .context("failed to build deterministic ed25519 keypair from node key seed")
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
                if let Err(error) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(TX_TOPIC), payload)
                {
                    tracing::warn!(?error, "failed to publish transaction");
                }
            }
            NetworkCommand::BroadcastBlock(block) => {
                let payload = Self::encode_block_announcement(&block)?;
                if let Err(error) = swarm.behaviour_mut().gossipsub.publish(
                    gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC),
                    payload,
                ) {
                    tracing::warn!(?error, "failed to publish block announcement");
                }
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
        event: SwarmEvent<NetworkBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "network listening");
                self.event_tx
                    .send(NetworkEvent::Listening(PeerAddress::from(
                        address.to_string(),
                    )))
                    .await?;
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                let peer_id = PeerId::from(peer_id.to_string());
                self.peer_score.connected(peer_id.clone()).await?;
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
                self.peer_score.disconnected(peer_id.clone()).await?;
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
                self.handle_gossipsub_event(event).await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::BlockRequestResponse(event)) => {
                self.handle_block_request_response_event(swarm, pending_block_responses, event)
                    .await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Ping(event)) => {
                tracing::debug!(?event, "ping event");
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Kademlia(event)) => {
                self.handle_kademlia_event(event).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_kademlia_event(&self, event: kad::Event) -> anyhow::Result<()> {
        match event {
            kad::Event::RoutingUpdated {
                peer,
                is_new_peer,
                addresses,
                old_peer,
                ..
            } => {
                let peer_id = PeerId::from(peer.to_string());
                tracing::info!(
                    %peer_id,
                    is_new_peer,
                    address_count = addresses.iter().count(),
                    ?old_peer,
                    "kademlia routing updated"
                );
                self.event_tx
                    .send(NetworkEvent::PeerDiscovered(peer_id))
                    .await?;
            }
            kad::Event::OutboundQueryProgressed {
                result: kad::QueryResult::Bootstrap(result),
                step,
                ..
            } => match result {
                Ok(ok) => {
                    tracing::info!(
                        peer_id = %ok.peer,
                        num_remaining = ok.num_remaining,
                        step = step.count.get(),
                        last = step.last,
                        "kademlia bootstrap progressed"
                    );
                }
                Err(error) => {
                    tracing::warn!(?error, "kademlia bootstrap failed");
                }
            },
            kad::Event::RoutablePeer { peer, address } => {
                tracing::debug!(peer_id = %peer, %address, "kademlia peer routable");
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                tracing::debug!(peer_id = %peer, %address, "kademlia peer pending routable");
            }
            kad::Event::UnroutablePeer { peer } => {
                tracing::debug!(peer_id = %peer, "kademlia peer unroutable");
            }
            kad::Event::ModeChanged { new_mode } => {
                tracing::debug!(?new_mode, "kademlia mode changed");
            }
            kad::Event::OutboundQueryProgressed { result, .. } => {
                tracing::debug!(?result, "kademlia query progressed");
            }
            kad::Event::InboundRequest { request } => {
                tracing::debug!(?request, "kademlia inbound request");
            }
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

    async fn handle_gossipsub_event(&self, event: gossipsub::Event) -> anyhow::Result<()> {
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
                    self.peer_score
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
                    self.peer_score
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
                    self.peer_score
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
                self.peer_score
                    .failed_request(
                        PeerId::from(peer.to_string()),
                        format!("block request failed: {error}"),
                    )
                    .await?;
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, ?error, "inbound block request failed");
                self.peer_score
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

    fn service_with_seed(seed: Option<&str>) -> anyhow::Result<NetworkService> {
        let (event_tx, _event_rx) = mpsc::channel(4);
        let (peer_score_command_tx, _peer_score_command_rx) = mpsc::channel(4);
        let (_peer_score_event_tx, peer_score_event_rx) = mpsc::channel(4);
        let (service, _handle) = NetworkService::new(
            NetworkConfig {
                listen_addr: PeerAddress::from("/ip4/127.0.0.1/tcp/0"),
                bootstrap_peers: Vec::new(),
                node_key_seed: seed.map(ToOwned::to_owned),
            },
            event_tx,
            PeerScoreHandle::new(peer_score_command_tx),
            peer_score_event_rx,
        )?;

        Ok(service)
    }

    #[test]
    fn node_key_seed_builds_stable_peer_id() -> anyhow::Result<()> {
        let first = service_with_seed(Some("node-a"))?
            .build_identity()?
            .public()
            .to_peer_id();
        let second = service_with_seed(Some("node-a"))?
            .build_identity()?
            .public()
            .to_peer_id();
        let third = service_with_seed(Some("node-b"))?
            .build_identity()?
            .public()
            .to_peer_id();

        assert_eq!(first, second);
        assert_ne!(first, third);
        Ok(())
    }
}
