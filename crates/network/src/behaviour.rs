use anyhow::Context;
use libp2p::{
    StreamProtocol, gossipsub,
    identity::Keypair,
    kad::{self, store::MemoryStore},
    ping,
    request_response::{self, ProtocolSupport, cbor},
};
use p2p_chain_lib_protocol::messages::{BlockRequest, BlockResponse};

pub const TX_TOPIC: &str = "p2p-chain-lib/tx/1";
pub const BLOCK_ANNOUNCEMENT_TOPIC: &str = "p2p-chain-lib/block-announcement/1";
pub const BLOCK_REQUEST_PROTOCOL: &str = "/p2p-chain-lib/block-request/1";

#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct NetworkBehaviour {
    pub ping: ping::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub block_request_response: cbor::Behaviour<BlockRequest, BlockResponse>,
}

impl NetworkBehaviour {
    pub fn new(keypair: Keypair, local_peer_id: libp2p::PeerId) -> anyhow::Result<Self> {
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

        let block_request_response = cbor::Behaviour::new(
            [(
                StreamProtocol::new(BLOCK_REQUEST_PROTOCOL),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        let kademlia = kad::Behaviour::new(local_peer_id, MemoryStore::new(local_peer_id));

        Ok(Self {
            ping: ping::Behaviour::new(ping::Config::default()),
            kademlia,
            gossipsub,
            block_request_response,
        })
    }
}
