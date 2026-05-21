use libp2p::{
    StreamProtocol, gossipsub,
    identity::Keypair,
    kad::{self, store::MemoryStore},
    ping,
    request_response::{self, ProtocolSupport, cbor},
};
use p2p_chain_lab_protocol::messages::{BlockRequest, BlockResponse};

pub const TX_TOPIC: &str = "p2p-chain-lab/tx/1";
pub const BLOCK_ANNOUNCEMENT_TOPIC: &str = "p2p-chain-lab/block-announcement/1";
pub const BLOCK_REQUEST_PROTOCOL: &str = "/p2p-chain-lab/block-request/1";

#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct NetworkBehaviour {
    pub ping: ping::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub block_request_response: cbor::Behaviour<BlockRequest, BlockResponse>,
}

impl NetworkBehaviour {
    pub fn new(keypair: Keypair, local_peer_id: libp2p::PeerId) -> Self {
        let mut gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair),
            gossipsub::Config::default(),
        )
        .expect("gossipsub signed authentication should accept libp2p keypair");
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TX_TOPIC))
            .expect("static transaction topic should be valid");
        gossipsub
            .subscribe(&gossipsub::IdentTopic::new(BLOCK_ANNOUNCEMENT_TOPIC))
            .expect("static block announcement topic should be valid");

        let block_request_response = cbor::Behaviour::new(
            [(
                StreamProtocol::new(BLOCK_REQUEST_PROTOCOL),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        let kademlia = kad::Behaviour::new(local_peer_id, MemoryStore::new(local_peer_id));

        Self {
            ping: ping::Behaviour::new(ping::Config::default()),
            kademlia,
            gossipsub,
            block_request_response,
        }
    }
}
