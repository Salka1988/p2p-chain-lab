use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder,
    futures::StreamExt,
    gossipsub, identify,
    identity::Keypair,
    kad::{self, Quorum},
    multiaddr, request_response,
    swarm::{
        SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
};
use p2p_chain_lib_chain::block::{Block, BlockHash};
use p2p_chain_lib_ports::{BlockNetworkPort, BlockRequestPort, NetworkEvent, PeerId};
use p2p_chain_lib_protocol::{
    codec,
    messages::{BlockAnnouncement, BlockRequest, BlockResponse, ChainTip, Transaction},
    validation::{validate_block, validate_chain_tip},
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    PeerAddress,
    behaviour::{
        BLOCK_ANNOUNCEMENT_TOPIC, BLOCK_TOPIC, CHAIN_TIP_TOPIC, NetworkBehaviour,
        NetworkBehaviourEvent, TX_TOPIC,
    },
    command::NetworkCommand,
    peer_score::{PeerScoreEvent, PeerScoreHandle},
};

const DEFAULT_COMMAND_BUFFER: usize = 1024;
const MAX_PENDING_CRITICAL_EVENTS: usize = 1024;
const MAX_PENDING_BEST_EFFORT_EVENTS: usize = 1024;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
pub const DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND: u16 = 64;
pub const DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND: u16 = 16;
const MAX_TRACKED_RATE_LIMIT_PEERS: usize = 4096;

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
    pub node_key_seed: Option<String>,
    pub validator_id: Option<String>,
    pub validators: Vec<String>,
    pub max_gossip_events_per_second: u16,
    pub max_block_requests_per_second: u16,
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

    pub async fn broadcast_chain_tip(&self, tip: ChainTip) -> anyhow::Result<()> {
        self.send(NetworkCommand::BroadcastChainTip(tip)).await
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
        self.send(NetworkCommand::SendBlock {
            peer_id,
            block_hash: block.hash.clone(),
            block: Some(block),
        })
        .await
    }

    pub async fn send_block_not_found(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        self.send(NetworkCommand::SendBlock {
            peer_id,
            block_hash,
            block: None,
        })
        .await
    }
}

#[async_trait]
impl BlockRequestPort for NetworkHandle {
    async fn request_block(&self, peer_id: PeerId, block_hash: BlockHash) -> anyhow::Result<()> {
        self.request_block(peer_id, block_hash).await
    }
}

#[async_trait]
impl BlockNetworkPort for NetworkHandle {
    async fn broadcast_block(&self, block: Block) -> anyhow::Result<()> {
        self.broadcast_block(block).await
    }

    async fn broadcast_chain_tip(&self, tip: ChainTip) -> anyhow::Result<()> {
        self.broadcast_chain_tip(tip).await
    }

    async fn send_block(&self, peer_id: PeerId, block: Block) -> anyhow::Result<()> {
        self.send_block(peer_id, block).await
    }

    async fn send_block_not_found(
        &self,
        peer_id: PeerId,
        block_hash: BlockHash,
    ) -> anyhow::Result<()> {
        self.send_block_not_found(peer_id, block_hash).await
    }
}

pub struct NetworkService {
    config: NetworkConfig,
    command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    peer_score: PeerScoreHandle,
    peer_score_event_rx: mpsc::Receiver<PeerScoreEvent>,
    pending_node_events: PendingNodeEvents,
    peer_rate_limiter: PeerRateLimiter,
    rejected_peers: HashSet<PeerId>,
}

type BlockResponseChannel = request_response::ResponseChannel<BlockResponse>;

#[derive(Default)]
struct PendingBlockRequests {
    responses: HashMap<(PeerId, BlockHash), BlockResponseChannel>,
    outbound: HashMap<request_response::OutboundRequestId, BlockHash>,
}

#[derive(Default)]
struct PendingNodeEvents {
    critical: VecDeque<NetworkEvent>,
    best_effort: VecDeque<NetworkEvent>,
}

#[derive(Default)]
struct PeerRateLimiter {
    peers: HashMap<PeerId, PeerRateWindow>,
    last_pruned_at: Option<tokio::time::Instant>,
}

struct PeerRateWindow {
    started_at: tokio::time::Instant,
    gossip_events: u16,
    block_requests: u16,
}

#[derive(Clone, Copy)]
enum IngressKind {
    Gossip,
    BlockRequest,
}

impl PeerRateLimiter {
    fn allow(
        &mut self,
        peer_id: PeerId,
        kind: IngressKind,
        max_gossip_events: u16,
        max_block_requests: u16,
    ) -> bool {
        let now = tokio::time::Instant::now();
        if self
            .last_pruned_at
            .is_none_or(|last_pruned_at| now.duration_since(last_pruned_at) >= RATE_LIMIT_WINDOW)
        {
            self.peers
                .retain(|_, window| now.duration_since(window.started_at) < RATE_LIMIT_WINDOW);
            self.last_pruned_at = Some(now);
        }
        if !self.peers.contains_key(&peer_id) && self.peers.len() == MAX_TRACKED_RATE_LIMIT_PEERS {
            return false;
        }
        let window = self.peers.entry(peer_id).or_insert(PeerRateWindow {
            started_at: now,
            gossip_events: 0,
            block_requests: 0,
        });
        let counter = match kind {
            IngressKind::Gossip => &mut window.gossip_events,
            IngressKind::BlockRequest => &mut window.block_requests,
        };
        let limit = match kind {
            IngressKind::Gossip => max_gossip_events,
            IngressKind::BlockRequest => max_block_requests,
        };
        if *counter == limit {
            return false;
        }
        *counter += 1;
        true
    }
}

impl PendingNodeEvents {
    fn push(&mut self, event: NetworkEvent) -> Option<NetworkEvent> {
        let (queue, limit) = if is_critical_event(&event) {
            (&mut self.critical, MAX_PENDING_CRITICAL_EVENTS)
        } else {
            (&mut self.best_effort, MAX_PENDING_BEST_EFFORT_EVENTS)
        };
        if queue.len() == limit {
            return Some(event);
        }
        queue.push_back(event);
        None
    }

    fn pop(&mut self) -> Option<NetworkEvent> {
        self.critical
            .pop_front()
            .or_else(|| self.best_effort.pop_front())
    }

    fn push_front(&mut self, event: NetworkEvent) {
        if is_critical_event(&event) {
            self.critical.push_front(event);
        } else {
            self.best_effort.push_front(event);
        }
    }
}

fn is_critical_event(event: &NetworkEvent) -> bool {
    matches!(
        event,
        NetworkEvent::BlockReceived { .. }
            | NetworkEvent::BlockNotFound { .. }
            | NetworkEvent::BlockRequestFailed { .. }
            | NetworkEvent::BlockRequestReceived { .. }
    )
}

fn event_peer_id(event: &NetworkEvent) -> Option<&PeerId> {
    match event {
        NetworkEvent::TransactionReceived { peer_id, .. }
        | NetworkEvent::BlockAnnouncementReceived { peer_id, .. }
        | NetworkEvent::ChainTipReceived { peer_id, .. }
        | NetworkEvent::BlockReceived { peer_id, .. }
        | NetworkEvent::BlockNotFound { peer_id, .. }
        | NetworkEvent::BlockRequestFailed { peer_id, .. }
        | NetworkEvent::BlockRequestReceived { peer_id, .. }
        | NetworkEvent::PeerMisbehaved { peer_id, .. } => Some(peer_id),
        NetworkEvent::PeerConnected(peer_id)
        | NetworkEvent::PeerDisconnected(peer_id)
        | NetworkEvent::PeerDiscovered(peer_id) => Some(peer_id),
        NetworkEvent::Listening(_) => None,
    }
}

fn block_response_matches_request(expected_hash: &BlockHash, response: &BlockResponse) -> bool {
    response.block_hash == *expected_hash
        && response
            .block
            .as_ref()
            .is_none_or(|block| block.hash == *expected_hash)
}

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
            pending_node_events: PendingNodeEvents::default(),
            peer_rate_limiter: PeerRateLimiter::default(),
            rejected_peers: HashSet::new(),
        };

        Ok((service, handle))
    }

    pub async fn run(mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut swarm = self.build_swarm().await?;
        let mut pending_block_requests = PendingBlockRequests::default();
        let mut banned_peers = HashSet::new();
        let mut local_validator_addresses = BTreeSet::new();
        let mut pending_validator_lookups = HashMap::new();
        let mut discovery_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let mut event_delivery_ticker = tokio::time::interval(Duration::from_millis(10));

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
                                &mut pending_block_requests,
                                command,
                            ).await?;
                        }
                        None => break,
                    }
                }
                event = swarm.select_next_some() => {
                    self.handle_swarm_event(
                        &mut swarm,
                        &mut pending_block_requests,
                        &mut local_validator_addresses,
                        &mut pending_validator_lookups,
                        &banned_peers,
                        event,
                    ).await?;
                }
                event = self.peer_score_event_rx.recv() => {
                    match event {
                        Some(event) => self.handle_peer_score_event(&mut swarm, &mut banned_peers, event).await?,
                        None => break,
                    }
                }
                _ = discovery_ticker.tick() => {
                    self.trigger_random_walk(&mut swarm).await?;
                    self.refresh_validator_directory(
                        &mut swarm,
                        &local_validator_addresses,
                        &mut pending_validator_lookups,
                    ).await?;
                }
                _ = event_delivery_ticker.tick() => {
                    self.flush_node_events()?;
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
                swarm.dial(
                    DialOpts::peer_id(peer_id)
                        .condition(PeerCondition::Disconnected)
                        .addresses(vec![peer_addr])
                        // Reusing the listening port is useful for NAT traversal but
                        // fails for multiple localhost nodes on macOS.
                        .allocate_new_port()
                        .build(),
                )?;
            } else {
                tracing::debug!(
                    %peer,
                    "bootstrap peer address has no /p2p/<peer-id>; dialing without kademlia address registration"
                );
                swarm.dial(
                    DialOpts::unknown_peer_id()
                        .address(peer_addr)
                        .allocate_new_port()
                        .build(),
                )?;
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

    fn validator_signing_key(&self) -> anyhow::Result<SigningKey> {
        let seed = self
            .config
            .node_key_seed
            .as_deref()
            .context("node key seed is required to sign validator records")?;
        let bytes: [u8; 32] = Sha256::digest(seed.as_bytes()).into();
        Ok(SigningKey::from_bytes(&bytes))
    }

    async fn emit_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        source_peer_id: Option<PeerId>,
        event: NetworkEvent,
    ) -> anyhow::Result<()> {
        self.flush_node_events()?;
        match self.event_tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(event)) => {
                let peer_id = source_peer_id.or_else(|| event_peer_id(&event).cloned());
                if let Some(event) = self.pending_node_events.push(event) {
                    if let Some(peer_id) = peer_id {
                        tracing::warn!(%peer_id, ?event, "rejecting peer because node event queues are full");
                        self.reject_peer(swarm, peer_id, "node event queues are full")
                            .await?;
                    } else {
                        tracing::warn!(
                            ?event,
                            "dropping local network event because its priority queue is full"
                        );
                    }
                }
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("node event receiver closed")
            }
        }
    }

    fn can_accept_critical_event(&self) -> bool {
        self.event_tx.capacity() > 0
            || self.pending_node_events.critical.len() < MAX_PENDING_CRITICAL_EVENTS
    }

    async fn reject_peer(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        peer_id: PeerId,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.rejected_peers.insert(peer_id.clone());
        if let Ok(libp2p_peer_id) = peer_id.as_str().parse() {
            let _ = swarm.disconnect_peer_id(libp2p_peer_id);
        }
        self.peer_score
            .failed_request(peer_id, reason.to_string())
            .await
    }

    async fn admit_peer_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        peer_id: PeerId,
        kind: IngressKind,
    ) -> anyhow::Result<bool> {
        if self.peer_rate_limiter.allow(
            peer_id.clone(),
            kind,
            self.config.max_gossip_events_per_second,
            self.config.max_block_requests_per_second,
        ) {
            return Ok(true);
        }
        tracing::warn!(%peer_id, "rejecting peer because ingress rate limit was exceeded");
        self.reject_peer(swarm, peer_id, "ingress rate limit exceeded")
            .await?;
        Ok(false)
    }

    fn flush_node_events(&mut self) -> anyhow::Result<()> {
        while let Some(event) = self.pending_node_events.pop() {
            match self.event_tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending_node_events.push_front(event);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    anyhow::bail!("node event receiver closed");
                }
            }
        }
        Ok(())
    }

    async fn handle_command(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_requests: &mut PendingBlockRequests,
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
                let payload = codec::encode(&block)?;
                if let Err(error) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(BLOCK_TOPIC), payload)
                {
                    tracing::warn!(?error, "failed to publish block");
                }
                let tip = ChainTip {
                    block_hash: block.hash,
                    height: block.height,
                };
                self.publish_chain_tip(swarm, &tip);
            }
            NetworkCommand::BroadcastChainTip(tip) => {
                self.publish_chain_tip(swarm, &tip);
            }
            NetworkCommand::RequestBlock {
                peer_id,
                block_hash,
            } => {
                let libp2p_peer_id = peer_id
                    .as_str()
                    .parse()
                    .with_context(|| format!("invalid peer id for block request: {peer_id}"))?;
                let request_id = swarm.behaviour_mut().block_request_response.send_request(
                    &libp2p_peer_id,
                    BlockRequest {
                        block_hash: block_hash.clone(),
                    },
                );
                pending_block_requests
                    .outbound
                    .insert(request_id, block_hash);
            }
            NetworkCommand::SendBlock {
                peer_id,
                block_hash,
                block,
            } => {
                let Some(channel) = pending_block_requests
                    .responses
                    .remove(&(peer_id.clone(), block_hash.clone()))
                else {
                    tracing::warn!(
                        %peer_id,
                        %block_hash,
                        "no pending block response channel",
                    );
                    return Ok(());
                };

                let response = BlockResponse { block_hash, block };
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

    fn publish_chain_tip(&self, swarm: &mut Swarm<NetworkBehaviour>, tip: &ChainTip) {
        let payload = match codec::encode(tip) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(?error, "failed to encode chain tip");
                return;
            }
        };
        if let Err(error) = swarm
            .behaviour_mut()
            .gossipsub
            .publish(gossipsub::IdentTopic::new(CHAIN_TIP_TOPIC), payload)
        {
            tracing::warn!(?error, "failed to publish chain tip");
        }
    }

    async fn handle_swarm_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_requests: &mut PendingBlockRequests,
        local_validator_addresses: &mut BTreeSet<String>,
        pending_validator_lookups: &mut HashMap<kad::QueryId, String>,
        banned_peers: &HashSet<PeerId>,
        event: SwarmEvent<NetworkBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "network listening");
                if self.track_local_validator_address(local_validator_addresses, &address) {
                    self.publish_validator_record(swarm, local_validator_addresses)
                        .await?;
                }
                self.emit_event(
                    swarm,
                    None,
                    NetworkEvent::Listening(PeerAddress::from(address.to_string())),
                )
                .await?;
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                let peer_id_string = PeerId::from(peer_id.to_string());
                if banned_peers.contains(&peer_id_string)
                    || self.rejected_peers.contains(&peer_id_string)
                {
                    tracing::warn!(%peer_id_string, "rejecting banned peer connection");
                    let _ = swarm.disconnect_peer_id(peer_id);
                    return Ok(());
                }
                // On an inbound connection the remote address is the peer's
                // ephemeral source port, not a dialable listener. Identify
                // supplies the peer's actual listen addresses shortly after.
                if endpoint.is_dialer() {
                    let address = endpoint.get_remote_address().clone();
                    swarm.add_peer_address(peer_id, address.clone());
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, address);
                }
                self.peer_score.connected(peer_id_string.clone()).await?;
                self.emit_event(swarm, None, NetworkEvent::PeerConnected(peer_id_string))
                    .await?;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                let peer_id = PeerId::from(peer_id.to_string());
                self.peer_score.disconnected(peer_id.clone()).await?;
                self.emit_event(swarm, None, NetworkEvent::PeerDisconnected(peer_id))
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
                self.handle_gossipsub_event(swarm, event).await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Identify(event)) => {
                self.handle_identify_event(swarm, event).await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::BlockRequestResponse(event)) => {
                self.handle_block_request_response_event(swarm, pending_block_requests, event)
                    .await?;
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Ping(event)) => {
                tracing::debug!(?event, "ping event");
            }
            SwarmEvent::Behaviour(NetworkBehaviourEvent::Kademlia(event)) => {
                self.handle_kademlia_event(swarm, event, pending_validator_lookups)
                    .await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_identify_event(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        event: identify::Event,
    ) -> anyhow::Result<()> {
        match event {
            identify::Event::Received {
                peer_id,
                connection_id: _,
                info,
            } => {
                let peer_id_str = PeerId::from(peer_id.to_string());
                tracing::info!(
                    %peer_id_str,
                    address_count = info.listen_addrs.len(),
                    "identify received"
                );
                for address in info.listen_addrs {
                    swarm.add_peer_address(peer_id, address.clone());
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, address);
                }
            }
            identify::Event::Sent { peer_id, .. } => {
                tracing::debug!(peer_id = %peer_id, "identify sent");
            }
            identify::Event::Pushed { peer_id, info, .. } => {
                tracing::debug!(
                    peer_id = %peer_id,
                    address_count = info.listen_addrs.len(),
                    "identify pushed"
                );
            }
            identify::Event::Error { peer_id, error, .. } => {
                tracing::warn!(peer_id = %peer_id, ?error, "identify error");
            }
        }

        Ok(())
    }

    async fn handle_kademlia_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        event: kad::Event,
        pending_validator_lookups: &mut HashMap<kad::QueryId, String>,
    ) -> anyhow::Result<()> {
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
                self.emit_event(swarm, None, NetworkEvent::PeerDiscovered(peer_id))
                    .await?;
            }
            kad::Event::OutboundQueryProgressed {
                id: _,
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
            kad::Event::OutboundQueryProgressed {
                id: _,
                result: kad::QueryResult::PutRecord(result),
                ..
            } => match result {
                Ok(ok) => {
                    let key = Self::validator_id_from_record_key(&ok.key)
                        .unwrap_or_else(|| String::from_utf8_lossy(ok.key.as_ref()).to_string());
                    tracing::info!(validator_id = %key, "validator record published");
                }
                Err(error) => {
                    tracing::warn!(?error, "validator record publish failed");
                }
            },
            kad::Event::OutboundQueryProgressed {
                id,
                result: kad::QueryResult::GetRecord(result),
                ..
            } => match result {
                Ok(kad::GetRecordOk::FoundRecord(record)) => {
                    let validator_id = pending_validator_lookups.remove(&id).unwrap_or_else(|| {
                        Self::validator_id_from_record_key(&record.record.key).unwrap_or_else(
                            || String::from_utf8_lossy(record.record.key.as_ref()).to_string(),
                        )
                    });
                    if let Err(error) = self
                        .handle_validator_record_lookup(swarm, &validator_id, record.record.value)
                        .await
                    {
                        tracing::warn!(?error, validator_id = %validator_id, "discarding invalid validator record");
                    }
                }
                Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. }) => {
                    if let Some(validator_id) = pending_validator_lookups.remove(&id) {
                        tracing::debug!(validator_id = %validator_id, "validator record lookup finished");
                    } else {
                        tracing::debug!("validator record lookup finished");
                    }
                }
                Err(error) => {
                    let validator_id = pending_validator_lookups.remove(&id).unwrap_or_else(|| {
                        Self::validator_id_from_record_key(error.key()).unwrap_or_else(|| {
                            String::from_utf8_lossy(error.key().as_ref()).to_string()
                        })
                    });
                    tracing::debug!(validator_id = %validator_id, ?error, "validator record lookup failed");
                }
            },
            kad::Event::OutboundQueryProgressed {
                id: _,
                result: kad::QueryResult::GetClosestPeers(result),
                ..
            } => match result {
                Ok(ok) => {
                    tracing::info!(peer_count = ok.peers.len(), "kademlia closest peers");
                    for peer in ok.peers {
                        let peer_id = peer.peer_id;
                        for address in peer.addrs {
                            swarm.add_peer_address(peer_id, address.clone());
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, address);
                        }

                        if !swarm.is_connected(&peer_id) {
                            match swarm.dial(
                                DialOpts::peer_id(peer_id)
                                    .condition(PeerCondition::Disconnected)
                                    .allocate_new_port()
                                    .build(),
                            ) {
                                Ok(()) => {
                                    tracing::debug!(peer_id = %peer_id, "dial discovered peer");
                                }
                                Err(error) => {
                                    tracing::debug!(peer_id = %peer_id, ?error, "failed to dial discovered peer");
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "kademlia closest peers lookup failed");
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

    async fn trigger_random_walk(&self, swarm: &mut Swarm<NetworkBehaviour>) -> anyhow::Result<()> {
        let query_id = swarm
            .behaviour_mut()
            .kademlia
            .get_closest_peers(libp2p::identity::PeerId::random());
        tracing::debug!(?query_id, "kademlia random walk started");
        Ok(())
    }

    async fn publish_validator_record(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        local_validator_addresses: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        let Some(validator_id) = &self.config.validator_id else {
            return Ok(());
        };

        if local_validator_addresses.is_empty() {
            return Ok(());
        }

        let addresses = local_validator_addresses
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .collect::<Vec<Multiaddr>>();

        if addresses.is_empty() {
            return Ok(());
        }

        let record = crate::validator_directory::ValidatorRecord::new(
            validator_id.clone(),
            *swarm.local_peer_id(),
            addresses,
            &self.validator_signing_key()?,
        )?;
        let value = crate::validator_directory::encode_record(&record)?;
        let record = kad::Record::new(crate::validator_directory::record_key(validator_id), value);
        match swarm
            .behaviour_mut()
            .kademlia
            .put_record(record, Quorum::One)
        {
            Ok(query_id) => {
                tracing::debug!(?query_id, validator_id = %validator_id, "validator record publish started");
            }
            Err(error) => {
                tracing::warn!(validator_id = %validator_id, ?error, "validator record publish failed to start");
            }
        }

        Ok(())
    }

    async fn refresh_validator_directory(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        local_validator_addresses: &BTreeSet<String>,
        pending_validator_lookups: &mut HashMap<kad::QueryId, String>,
    ) -> anyhow::Result<()> {
        self.publish_validator_record(swarm, local_validator_addresses)
            .await?;

        for validator_id in self.remote_validator_ids() {
            if pending_validator_lookups
                .values()
                .any(|pending| pending == &validator_id)
            {
                continue;
            }

            let query_id = swarm
                .behaviour_mut()
                .kademlia
                .get_record(crate::validator_directory::record_key(&validator_id));
            pending_validator_lookups.insert(query_id, validator_id.clone());
            tracing::debug!(
                ?query_id,
                validator_id = %validator_id,
                "validator record lookup started"
            );
        }

        Ok(())
    }

    async fn handle_validator_record_lookup(
        &self,
        swarm: &mut Swarm<NetworkBehaviour>,
        validator_id: &str,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        let (peer_id, addresses) = Self::parse_validator_record_lookup(validator_id, &value)?;

        tracing::info!(
            validator_id = %validator_id,
            peer_id = %peer_id,
            address_count = addresses.len(),
            "validator record received"
        );

        for address in addresses {
            swarm.add_peer_address(peer_id, address.clone());
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, address);
        }

        if !swarm.is_connected(&peer_id) {
            match swarm.dial(
                DialOpts::peer_id(peer_id)
                    .condition(PeerCondition::Disconnected)
                    .allocate_new_port()
                    .build(),
            ) {
                Ok(()) => {
                    tracing::debug!(validator_id = %validator_id, peer_id = %peer_id, "dial validator from dht record");
                }
                Err(error) => {
                    tracing::debug!(validator_id = %validator_id, peer_id = %peer_id, ?error, "failed to dial validator from dht record");
                }
            }
        }

        Ok(())
    }

    fn parse_validator_record_lookup(
        validator_id: &str,
        value: &[u8],
    ) -> anyhow::Result<(libp2p::PeerId, Vec<Multiaddr>)> {
        let record = crate::validator_directory::decode_record(value)
            .with_context(|| format!("invalid validator record for {validator_id}"))?;
        anyhow::ensure!(
            record.validator_id == validator_id,
            "validator record identity does not match its lookup key"
        );
        crate::validator_directory::verify_record(&record)
            .with_context(|| format!("invalid validator record signature for {validator_id}"))?;

        let peer_id: libp2p::PeerId = record
            .peer_id
            .parse()
            .with_context(|| format!("invalid peer id in validator record for {validator_id}"))?;
        let addresses = record
            .listen_addrs
            .into_iter()
            .map(|address| {
                address
                    .parse()
                    .with_context(|| format!("invalid validator address for {validator_id}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok((peer_id, addresses))
    }

    fn remote_validator_ids(&self) -> impl Iterator<Item = String> + '_ {
        self.config
            .validators
            .iter()
            .filter(|validator_id| {
                Some(validator_id.as_str()) != self.config.validator_id.as_deref()
            })
            .cloned()
    }

    fn track_local_validator_address(
        &self,
        local_validator_addresses: &mut BTreeSet<String>,
        address: &Multiaddr,
    ) -> bool {
        local_validator_addresses.insert(address.to_string())
    }

    fn validator_id_from_record_key(key: &kad::RecordKey) -> Option<String> {
        let key = std::str::from_utf8(key.as_ref()).ok()?;
        key.strip_prefix(crate::validator_directory::VALIDATOR_RECORD_PREFIX)
            .map(ToOwned::to_owned)
    }

    async fn handle_peer_score_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        banned_peers: &mut HashSet<PeerId>,
        event: PeerScoreEvent,
    ) -> anyhow::Result<()> {
        match event {
            PeerScoreEvent::PeerBanned { peer_id, reason } => {
                tracing::warn!(%peer_id, %reason, "peer banned");
                banned_peers.insert(peer_id.clone());
                if let Ok(libp2p_peer_id) = peer_id.as_str().parse()
                    && let Err(error) = swarm.disconnect_peer_id(libp2p_peer_id)
                {
                    tracing::debug!(%peer_id, ?error, "failed to disconnect banned peer");
                }
                self.emit_event(
                    swarm,
                    None,
                    NetworkEvent::PeerMisbehaved { peer_id, reason },
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_gossipsub_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
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

        let message_source = message.source.unwrap_or(propagation_source);
        let peer_id = PeerId::from(message_source.to_string());
        let ingress_peer_id = PeerId::from(propagation_source.to_string());
        if message.topic == gossipsub::IdentTopic::new(TX_TOPIC).hash() {
            if !self
                .admit_peer_event(swarm, ingress_peer_id.clone(), IngressKind::Gossip)
                .await?
            {
                return Ok(());
            }
            let tx = match codec::decode::<Transaction>(&message.data) {
                Ok(tx) => tx,
                Err(error) => {
                    self.peer_score
                        .bad_message(peer_id, format!("invalid transaction payload: {error}"))
                        .await?;
                    return Ok(());
                }
            };
            self.emit_event(
                swarm,
                Some(ingress_peer_id),
                NetworkEvent::TransactionReceived { peer_id, tx },
            )
            .await?;
        } else if message.topic == gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC).hash() {
            if !self
                .admit_peer_event(swarm, ingress_peer_id.clone(), IngressKind::Gossip)
                .await?
            {
                return Ok(());
            }
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
            self.emit_event(
                swarm,
                Some(ingress_peer_id),
                NetworkEvent::BlockAnnouncementReceived {
                    peer_id,
                    announcement,
                },
            )
            .await?;
        } else if message.topic == gossipsub::IdentTopic::new(BLOCK_TOPIC).hash() {
            if !self
                .admit_peer_event(swarm, ingress_peer_id.clone(), IngressKind::Gossip)
                .await?
            {
                return Ok(());
            }
            let block = match codec::decode::<Block>(&message.data).and_then(|block| {
                validate_block(&block)?;
                Ok(block)
            }) {
                Ok(block) => block,
                Err(error) => {
                    self.peer_score
                        .bad_message(peer_id, format!("invalid block payload: {error}"))
                        .await?;
                    return Ok(());
                }
            };
            self.emit_event(
                swarm,
                Some(ingress_peer_id),
                NetworkEvent::BlockReceived { peer_id, block },
            )
            .await?;
        } else if message.topic == gossipsub::IdentTopic::new(CHAIN_TIP_TOPIC).hash() {
            if !self
                .admit_peer_event(swarm, ingress_peer_id.clone(), IngressKind::Gossip)
                .await?
            {
                return Ok(());
            }
            let tip = match codec::decode::<ChainTip>(&message.data).and_then(|tip| {
                validate_chain_tip(&tip)?;
                Ok(tip)
            }) {
                Ok(tip) => tip,
                Err(error) => {
                    self.peer_score
                        .bad_message(peer_id, format!("invalid chain tip payload: {error}"))
                        .await?;
                    return Ok(());
                }
            };
            self.emit_event(
                swarm,
                Some(ingress_peer_id),
                NetworkEvent::ChainTipReceived { peer_id, tip },
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_block_request_response_event(
        &mut self,
        swarm: &mut Swarm<NetworkBehaviour>,
        pending_block_requests: &mut PendingBlockRequests,
        event: request_response::Event<BlockRequest, BlockResponse>,
    ) -> anyhow::Result<()> {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let peer_id = PeerId::from(peer.to_string());
                    if !self
                        .admit_peer_event(swarm, peer_id.clone(), IngressKind::BlockRequest)
                        .await?
                    {
                        return Ok(());
                    }
                    if !self.can_accept_critical_event() {
                        tracing::warn!(%peer_id, block_hash = %request.block_hash, "rejecting block request because node event queues are full");
                        self.reject_peer(swarm, peer_id, "node event queues are full")
                            .await?;
                        return Ok(());
                    }
                    pending_block_requests
                        .responses
                        .insert((peer_id.clone(), request.block_hash.clone()), channel);
                    self.emit_event(
                        swarm,
                        None,
                        NetworkEvent::BlockRequestReceived {
                            peer_id,
                            block_hash: request.block_hash,
                        },
                    )
                    .await?;
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let Some(expected_hash) = pending_block_requests.outbound.remove(&request_id)
                    else {
                        tracing::warn!(peer_id = %peer, ?request_id, "received response for an unknown block request");
                        return Ok(());
                    };
                    if !block_response_matches_request(&expected_hash, &response) {
                        self.peer_score
                            .bad_message(
                                PeerId::from(peer.to_string()),
                                "block response does not match its request",
                            )
                            .await?;
                        self.emit_event(
                            swarm,
                            None,
                            NetworkEvent::BlockRequestFailed {
                                peer_id: PeerId::from(peer.to_string()),
                                block_hash: expected_hash,
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                    let Some(block) = response.block else {
                        self.emit_event(
                            swarm,
                            None,
                            NetworkEvent::BlockNotFound {
                                peer_id: PeerId::from(peer.to_string()),
                                block_hash: expected_hash,
                            },
                        )
                        .await?;
                        return Ok(());
                    };
                    self.peer_score
                        .useful_response(PeerId::from(peer.to_string()))
                        .await?;
                    self.emit_event(
                        swarm,
                        None,
                        NetworkEvent::BlockReceived {
                            peer_id: PeerId::from(peer.to_string()),
                            block,
                        },
                    )
                    .await?;
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(peer_id = %peer, ?error, "block request failed");
                self.peer_score
                    .failed_request(
                        PeerId::from(peer.to_string()),
                        format!("block request failed: {error}"),
                    )
                    .await?;
                if let Some(block_hash) = pending_block_requests.outbound.remove(&request_id) {
                    self.emit_event(
                        swarm,
                        None,
                        NetworkEvent::BlockRequestFailed {
                            peer_id: PeerId::from(peer.to_string()),
                            block_hash,
                        },
                    )
                    .await?;
                }
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
                validator_id: None,
                validators: Vec::new(),
                max_gossip_events_per_second: DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
                max_block_requests_per_second: DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND,
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

    #[test]
    fn malformed_validator_record_is_rejected_without_panicking() {
        assert!(NetworkService::parse_validator_record_lookup("validator-a", b"not cbor").is_err());
    }

    #[test]
    fn rejects_block_response_for_a_different_hash() {
        let expected_hash = BlockHash::from_u8(1);
        let response = BlockResponse {
            block_hash: BlockHash::from_u8(2),
            block: None,
        };

        assert!(!block_response_matches_request(&expected_hash, &response));
    }

    #[test]
    fn prioritizes_critical_events_over_best_effort_events() {
        let mut events = PendingNodeEvents::default();
        for index in 0..MAX_PENDING_BEST_EFFORT_EVENTS {
            assert!(
                events
                    .push(NetworkEvent::PeerConnected(PeerId::from(format!(
                        "peer-{index}"
                    ))))
                    .is_none()
            );
        }
        assert!(
            events
                .push(NetworkEvent::BlockRequestFailed {
                    peer_id: PeerId::from("peer-b"),
                    block_hash: BlockHash::from_u8(1),
                })
                .is_none()
        );
        assert!(matches!(
            events.pop(),
            Some(NetworkEvent::BlockRequestFailed { peer_id, block_hash })
                if peer_id == PeerId::from("peer-b") && block_hash == BlockHash::from_u8(1)
        ));
    }

    #[test]
    fn rate_limits_each_peer_and_ingress_kind_independently() {
        let mut limiter = PeerRateLimiter::default();
        let peer_a = PeerId::from("peer-a");

        for _ in 0..DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND {
            assert!(limiter.allow(
                peer_a.clone(),
                IngressKind::Gossip,
                DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
                DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND,
            ));
        }
        assert!(!limiter.allow(
            peer_a.clone(),
            IngressKind::Gossip,
            DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
            DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND,
        ));
        assert!(limiter.allow(
            peer_a.clone(),
            IngressKind::BlockRequest,
            DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
            DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND,
        ));
        assert!(limiter.allow(
            PeerId::from("peer-b"),
            IngressKind::Gossip,
            DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
            DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND,
        ));
    }
}
