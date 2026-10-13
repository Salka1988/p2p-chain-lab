use anyhow::Context;
use libp2p::{
    StreamProtocol, gossipsub, identify,
    identity::Keypair,
    kad::{self, store::MemoryStore},
    ping,
    request_response::{self, ProtocolSupport, cbor},
};
use p2p_chain_lib_protocol::messages::{BlockRequest, BlockResponse};

pub(crate) const TX_TOPIC: &str = "p2p-chain-lib/tx/1";
pub(crate) const BLOCK_ANNOUNCEMENT_TOPIC: &str = "p2p-chain-lib/block-announcement/1";
pub(crate) const BLOCK_TOPIC: &str = "p2p-chain-lib/block/1";
pub(crate) const CHAIN_TIP_TOPIC: &str = "p2p-chain-lib/chain-tip/1";
pub(crate) const BLOCK_REQUEST_PROTOCOL: &str = "/p2p-chain-lib/block-request/1";
pub(crate) const IDENTIFY_PROTOCOL_VERSION: &str = "p2p-chain-lib/1.0.0";
pub(crate) const KAD_PROTOCOL_NAME: &str = "/p2p-chain-lib/kad/1.0.0";

#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct NetworkBehaviour {
    pub(crate) ping: ping::Behaviour,
    pub(crate) kademlia: kad::Behaviour<MemoryStore>,
    pub(crate) identify: identify::Behaviour,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) block_request_response: cbor::Behaviour<BlockRequest, BlockResponse>,
}

impl NetworkBehaviour {
    pub(crate) fn new(keypair: Keypair, local_peer_id: libp2p::PeerId) -> anyhow::Result<Self> {
        let identify = identify::Behaviour::new(
            identify::Config::new_with_signed_peer_record(
                IDENTIFY_PROTOCOL_VERSION.to_string(),
                &keypair,
            )
            .with_push_listen_addr_updates(true),
        );

        let mut gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair),
            gossipsub::Config::default(),
        )
        .map_err(anyhow::Error::msg)
        .context("failed to create gossipsub behaviour")?;
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TX_TOPIC))
            .context("failed to subscribe to transaction topic")?;
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC))
            .context("failed to subscribe to block announcement topic")?;
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(BLOCK_TOPIC))
            .context("failed to subscribe to block topic")?;
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(CHAIN_TIP_TOPIC))
            .context("failed to subscribe to chain tip topic")?;

        let block_request_response = cbor::Behaviour::new(
            [(
                StreamProtocol::new(BLOCK_REQUEST_PROTOCOL),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        let mut kademlia = kad::Behaviour::with_config(
            local_peer_id,
            MemoryStore::new(local_peer_id),
            kad::Config::new(StreamProtocol::new(KAD_PROTOCOL_NAME)),
        );
        kademlia.set_mode(Some(kad::Mode::Server));

        Ok(Self {
            ping: ping::Behaviour::new(ping::Config::default()),
            kademlia,
            identify,
            gossipsub,
            block_request_response,
        })
    }
}
