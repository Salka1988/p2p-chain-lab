use std::time::Duration;

use clap::{Parser, builder::NonEmptyStringValueParser};
use p2p_chain_lib_network::service::{
    DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND, DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND,
};
use p2p_chain_lib_node::config::Config;
use p2p_chain_lib_ports::PeerAddress;

const DEFAULT_LISTEN_ADDR: &str = "/ip4/127.0.0.1/tcp/0";

#[derive(Debug, Parser)]
#[command(
    name = "p2p-chain-lib-cli",
    about = "P2P blockchain node",
    args_override_self = true
)]
struct Cli {
    #[arg(long, default_value = DEFAULT_LISTEN_ADDR, value_parser = NonEmptyStringValueParser::new())]
    listen: String,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    bootstrap: Vec<String>,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    node_key_seed: Option<String>,

    #[arg(
        long,
        value_name = "ED25519_PUBLIC_KEY_HEX",
        help = "scheduled validator Ed25519 public key, encoded as 64 hexadecimal characters",
        value_parser = NonEmptyStringValueParser::new()
    )]
    validator: Vec<String>,

    #[arg(long = "produce-block-interval-ms", value_parser = clap::value_parser!(u64).range(1..))]
    produce_block_interval_ms: Option<u64>,

    #[arg(long, default_value_t = DEFAULT_MAX_GOSSIP_EVENTS_PER_SECOND, value_parser = clap::value_parser!(u16).range(1..))]
    max_gossip_events_per_second: u16,

    #[arg(long, default_value_t = DEFAULT_MAX_BLOCK_REQUESTS_PER_SECOND, value_parser = clap::value_parser!(u16).range(1..))]
    max_block_requests_per_second: u16,
}

pub fn from_args() -> anyhow::Result<Config> {
    Ok(Cli::try_parse()?.into())
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        Self {
            listen_addr: PeerAddress::from(cli.listen),
            bootstrap_peers: cli.bootstrap.into_iter().map(PeerAddress::from).collect(),
            node_key_seed: cli.node_key_seed,
            block_production_interval: cli.produce_block_interval_ms.map(Duration::from_millis),
            validators: cli.validator,
            max_gossip_events_per_second: cli.max_gossip_events_per_second,
            max_block_requests_per_second: cli.max_block_requests_per_second,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_values() -> anyhow::Result<()> {
        let config: Config = Cli::try_parse_from([
            "p2p-chain-lib-cli",
            "--listen=/ip4/127.0.0.1/tcp/4001",
            "--bootstrap=/ip4/127.0.0.1/tcp/4002",
            "--bootstrap=/ip4/127.0.0.1/tcp/4003",
            "--node-key-seed=node-a",
            "--validator=node-a",
            "--validator=node-b",
            "--produce-block-interval-ms=250",
            "--max-gossip-events-per-second=80",
            "--max-block-requests-per-second=20",
        ])?
        .into();

        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4001");
        assert_eq!(
            config
                .bootstrap_peers
                .iter()
                .map(PeerAddress::as_str)
                .collect::<Vec<_>>(),
            vec!["/ip4/127.0.0.1/tcp/4002", "/ip4/127.0.0.1/tcp/4003"]
        );
        assert_eq!(config.node_key_seed.as_deref(), Some("node-a"));
        assert_eq!(config.validators, vec!["node-a", "node-b"]);
        assert_eq!(
            config.block_production_interval,
            Some(Duration::from_millis(250))
        );
        assert_eq!(config.max_gossip_events_per_second, 80);
        assert_eq!(config.max_block_requests_per_second, 20);
        Ok(())
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(Cli::try_parse_from(["p2p-chain-lib-cli", "--listen="]).is_err());
        assert!(
            Cli::try_parse_from(["p2p-chain-lib-cli", "--produce-block-interval-ms=0"]).is_err()
        );
        assert!(Cli::try_parse_from(["p2p-chain-lib-cli", "--bad"]).is_err());
    }

    #[test]
    fn last_single_value_wins() -> anyhow::Result<()> {
        let config: Config = Cli::try_parse_from([
            "p2p-chain-lib-cli",
            "--listen=/ip4/127.0.0.1/tcp/4001",
            "--listen=/ip4/127.0.0.1/tcp/4002",
        ])?
        .into();

        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4002");
        Ok(())
    }
}
