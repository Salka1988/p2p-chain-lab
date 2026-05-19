use libp2p::{
    StreamProtocol, gossipsub,
    identity::Keypair,
    ping,
    request_response::{self, ProtocolSupport, cbor},
};
use libp2p_mini_chain_protocol::messages::{BlockRequest, BlockResponse};

pub const TX_TOPIC: &str = "libp2p-mini-chain/tx/1";
pub const BLOCK_ANNOUNCEMENT_TOPIC: &str = "libp2p-mini-chain/block-announcement/1";
pub const BLOCK_REQUEST_PROTOCOL: &str = "/libp2p-mini-chain/block-request/1";

#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct NetworkBehaviour {
    pub ping: ping::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub block_request_response: cbor::Behaviour<BlockRequest, BlockResponse>,
}

impl NetworkBehaviour {
    pub fn new(keypair: Keypair) -> Self {
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

        Self {
            ping: ping::Behaviour::new(ping::Config::default()),
            gossipsub,
            block_request_response,
        }
    }
}
