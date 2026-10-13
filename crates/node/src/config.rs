use std::{path::PathBuf, time::Duration};

use p2p_chain_lib_ports::PeerAddress;

const DEFAULT_LISTEN_ADDR: &str = "/ip4/127.0.0.1/tcp/0";

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
    pub node_key_seed: Option<String>,
    pub chain_path: PathBuf,
    pub block_production_interval: Option<Duration>,
    pub genesis: GenesisConfig,
    pub max_gossip_events_per_second: u16,
    pub max_block_requests_per_second: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: PeerAddress::from(DEFAULT_LISTEN_ADDR),
            bootstrap_peers: Vec::new(),
            node_key_seed: None,
            chain_path: PathBuf::from("chain.bin"),
            block_production_interval: None,
            genesis: GenesisConfig::default(),
            max_gossip_events_per_second: 64,
            max_block_requests_per_second: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GenesisConfig {
    pub chain_id: String,
    pub validators: Vec<String>,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            chain_id: "local-devnet".into(),
            validators: Vec::new(),
        }
    }
}
