use std::{collections::HashMap, path::PathBuf, time::Duration};

use anyhow::{Context, ensure};
use clap::{Parser, builder::NonEmptyStringValueParser};
use p2p_chain_lib_node::config::{Config, GenesisConfig};
use p2p_chain_lib_ports::PeerAddress;
use serde::Deserialize;

const ENV_FIELDS: &[&str] = &[
    "LISTEN",
    "BOOTSTRAP",
    "NODE_KEY_SEED",
    "CHAIN_PATH",
    "VALIDATOR",
    "GENESIS__CHAIN_ID",
    "GENESIS__VALIDATORS",
    "PRODUCE_BLOCK_INTERVAL_MS",
    "MAX_GOSSIP_EVENTS_PER_SECOND",
    "MAX_BLOCK_REQUESTS_PER_SECOND",
];

#[derive(Debug, Parser, Deserialize)]
#[serde(deny_unknown_fields)]
#[command(
    name = "p2p-chain-lib-cli",
    about = "P2P blockchain node",
    args_override_self = true
)]
struct Cli {
    #[arg(skip)]
    genesis: Option<GenesisSettings>,

    #[arg(long)]
    #[serde(skip)]
    chain_id: Option<String>,
    /// Optional TOML configuration file. Explicit flags override environment and file values.
    #[arg(long, value_name = "FILE")]
    #[serde(skip)]
    config: Option<PathBuf>,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    listen: Option<String>,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    bootstrap: Option<Vec<String>>,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    node_key_seed: Option<String>,

    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    /// Block storage directory (one committed file per block).
    chain_path: Option<String>,

    #[arg(
        long,
        value_name = "ED25519_PUBLIC_KEY_HEX",
        help = "scheduled validator Ed25519 public key, encoded as 64 hexadecimal characters",
        value_parser = NonEmptyStringValueParser::new()
    )]
    validator: Option<Vec<String>>,

    #[arg(long = "produce-block-interval-ms", value_parser = clap::value_parser!(u64).range(1..))]
    produce_block_interval_ms: Option<u64>,

    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    max_gossip_events_per_second: Option<u16>,

    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    max_block_requests_per_second: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenesisSettings {
    chain_id: Option<String>,
    validators: Option<Vec<String>>,
}

pub fn from_args() -> anyhow::Result<Config> {
    Cli::try_parse()?.load(std::env::vars().collect())
}

impl Cli {
    fn load(self, mut env: HashMap<String, String>) -> anyhow::Result<Config> {
        // Only consume node settings, not script variables such as P2P_DATA_DIR.
        env.retain(|key, _| {
            key.strip_prefix("P2P_")
                .is_some_and(|key| ENV_FIELDS.contains(&key))
        });
        let mut builder = ::config::Config::builder();
        if let Some(path) = &self.config {
            builder = builder.add_source(
                ::config::File::from(path.as_path()).format(::config::FileFormat::Toml),
            );
        }
        // Normalize the legacy TOML spelling before applying higher-priority sources.
        let file = builder.build()?;
        let legacy = file.get::<Vec<String>>("validator").ok();
        ensure!(
            legacy.is_none() || file.get::<Vec<String>>("genesis.validators").is_err(),
            "use genesis.validators or validator, not both"
        );
        let mut builder = ::config::Config::builder()
            .add_source(file)
            .set_override_option("genesis.validators", legacy)?;
        let bootstrap = env.remove("P2P_BOOTSTRAP").map(parse_env_list);
        let legacy = env.remove("P2P_VALIDATOR");
        let nested = env.remove("P2P_GENESIS__VALIDATORS");
        ensure!(
            legacy.is_none() || nested.is_none(),
            "use only one validator environment spelling"
        );
        let validators = nested.or(legacy).map(parse_env_list);
        // Apply this after the TOML alias, before explicit CLI flags.
        if let Some(validators) = validators {
            builder = builder.set_override("genesis.validators", validators)?;
        }
        let settings: Self = builder
            .add_source(
                ::config::Environment::with_prefix("P2P")
                    .prefix_separator("_")
                    .separator("__")
                    .source(Some(env)),
            )
            .set_override_option("bootstrap", bootstrap)?
            .set_override_option("listen", self.listen)?
            .set_override_option("bootstrap", self.bootstrap)?
            .set_override_option("node_key_seed", self.node_key_seed)?
            .set_override_option("chain_path", self.chain_path)?
            .set_override_option("genesis.validators", self.validator)?
            .set_override_option("genesis.chain_id", self.chain_id)?
            .set_override_option("produce_block_interval_ms", self.produce_block_interval_ms)?
            .set_override_option(
                "max_gossip_events_per_second",
                self.max_gossip_events_per_second,
            )?
            .set_override_option(
                "max_block_requests_per_second",
                self.max_block_requests_per_second,
            )?
            .build()
            .context("failed to load node configuration")?
            .try_deserialize()
            .context("invalid node configuration")?;
        settings.finish()
    }

    fn finish(self) -> anyhow::Result<Config> {
        let defaults = Config::default();
        let genesis = self.genesis.unwrap_or(GenesisSettings {
            chain_id: None,
            validators: None,
        });
        let config = Config {
            listen_addr: self
                .listen
                .map(PeerAddress::from)
                .unwrap_or(defaults.listen_addr),
            bootstrap_peers: self
                .bootstrap
                .unwrap_or_default()
                .into_iter()
                .map(PeerAddress::from)
                .collect(),
            node_key_seed: self.node_key_seed,
            chain_path: self
                .chain_path
                .map(PathBuf::from)
                .unwrap_or(defaults.chain_path),
            block_production_interval: self.produce_block_interval_ms.map(Duration::from_millis),
            genesis: GenesisConfig {
                chain_id: genesis.chain_id.unwrap_or(defaults.genesis.chain_id),
                validators: genesis.validators.unwrap_or_default(),
            },
            max_gossip_events_per_second: self
                .max_gossip_events_per_second
                .unwrap_or(defaults.max_gossip_events_per_second),
            max_block_requests_per_second: self
                .max_block_requests_per_second
                .unwrap_or(defaults.max_block_requests_per_second),
        };
        ensure!(
            !config.listen_addr.as_str().is_empty(),
            "listen must not be empty"
        );
        ensure!(
            !config.chain_path.as_os_str().is_empty(),
            "chain_path must not be empty"
        );
        ensure!(
            config.node_key_seed.as_deref() != Some(""),
            "node_key_seed must not be empty"
        );
        ensure!(
            config
                .bootstrap_peers
                .iter()
                .all(|peer| !peer.as_str().is_empty()),
            "bootstrap entries must not be empty"
        );
        ensure!(
            config
                .genesis
                .validators
                .iter()
                .all(|validator| !validator.is_empty()),
            "validator entries must not be empty"
        );
        ensure!(
            !config.genesis.chain_id.trim().is_empty(),
            "genesis.chain_id must not be empty"
        );
        ensure!(
            config.block_production_interval != Some(Duration::ZERO),
            "produce_block_interval_ms must be greater than zero"
        );
        ensure!(
            config.max_gossip_events_per_second > 0,
            "max_gossip_events_per_second must be greater than zero"
        );
        ensure!(
            config.max_block_requests_per_second > 0,
            "max_block_requests_per_second must be greater than zero"
        );
        Ok(config)
    }
}

fn parse_env_list(value: String) -> Vec<String> {
    if value.trim().is_empty() {
        Vec::new()
    } else {
        value
            .split(',')
            .map(|item| item.trim().to_owned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_genesis_obeys_layer_precedence() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        std::fs::write(
            file.path(),
            "[genesis]\nchain_id = 'file'\nvalidators = ['file-key']\n",
        )?;
        let path = file.path().to_str().unwrap();
        let config = Cli::try_parse_from(["node", "--config", path])?.load(HashMap::new())?;
        assert_eq!(config.genesis.chain_id, "file");
        assert_eq!(config.genesis.validators, ["file-key"]);
        let environment = env(&[
            ("P2P_GENESIS__CHAIN_ID", "env"),
            ("P2P_GENESIS__VALIDATORS", "env-key"),
        ]);
        let config = Cli::try_parse_from(["node", "--config", path])?.load(environment.clone())?;
        assert_eq!(config.genesis.chain_id, "env");
        assert_eq!(config.genesis.validators, ["env-key"]);
        let config = Cli::try_parse_from([
            "node",
            "--config",
            path,
            "--chain-id=cli",
            "--validator=cli-key",
        ])?
        .load(environment)?;
        assert_eq!(config.genesis.chain_id, "cli");
        assert_eq!(config.genesis.validators, ["cli-key"]);
        Ok(())
    }

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn defaults_match_node_config() -> anyhow::Result<()> {
        let config = Cli::try_parse_from(["node"])?.load(HashMap::new())?;
        let defaults = Config::default();
        assert_eq!(config.listen_addr, defaults.listen_addr);
        assert_eq!(config.chain_path, defaults.chain_path);
        assert_eq!(
            config.max_gossip_events_per_second,
            defaults.max_gossip_events_per_second
        );
        assert_eq!(
            config.max_block_requests_per_second,
            defaults.max_block_requests_per_second
        );
        assert!(config.bootstrap_peers.is_empty());
        assert!(config.genesis.validators.is_empty());
        assert!(config.node_key_seed.is_none());
        assert!(config.block_production_interval.is_none());
        Ok(())
    }

    #[test]
    fn layers_replace_values_and_lists() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        std::fs::write(
            file.path(),
            r#"
listen = "/ip4/127.0.0.1/tcp/4001"
chain_path = "from-file-data"
bootstrap = ["file-peer"]
validator = ["file-validator"]
produce_block_interval_ms = 100
max_gossip_events_per_second = 70
"#,
        )?;
        let environment = env(&[
            ("P2P_LISTEN", "/ip4/127.0.0.1/tcp/4002"),
            ("P2P_BOOTSTRAP", "env-peer-a, env-peer-b"),
            ("P2P_VALIDATOR", "env-validator"),
            ("P2P_NODE_KEY_SEED", "00123"),
            ("P2P_PRODUCE_BLOCK_INTERVAL_MS", "200"),
            ("P2P_MAX_GOSSIP_EVENTS_PER_SECOND", "80"),
            ("P2P_MAX_BLOCK_REQUESTS_PER_SECOND", "22"),
            ("P2P_DATA_DIR", "/tmp/script-only"),
        ]);
        let path = file.path().to_str().unwrap();
        let config = Cli::try_parse_from(["node", "--config", path])?.load(environment.clone())?;
        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4002");
        assert_eq!(config.bootstrap_peers.len(), 2);
        assert_eq!(config.bootstrap_peers[1].as_str(), "env-peer-b");
        assert_eq!(config.genesis.validators, ["env-validator"]);
        assert_eq!(config.node_key_seed.as_deref(), Some("00123"));
        assert_eq!(
            config.block_production_interval,
            Some(Duration::from_millis(200))
        );
        assert_eq!(config.max_gossip_events_per_second, 80);
        assert_eq!(config.max_block_requests_per_second, 22);
        assert_eq!(config.chain_path, PathBuf::from("from-file-data"));
        let config = Cli::try_parse_from([
            "node",
            "--config",
            path,
            "--listen=/ip4/127.0.0.1/tcp/4003",
            "--bootstrap=cli-peer",
            "--validator=cli-validator",
            "--produce-block-interval-ms=300",
        ])?
        .load(environment)?;
        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4003");
        assert_eq!(config.bootstrap_peers, [PeerAddress::from("cli-peer")]);
        assert_eq!(config.genesis.validators, ["cli-validator"]);
        assert_eq!(
            config.block_production_interval,
            Some(Duration::from_millis(300))
        );
        let config = Cli::try_parse_from(["node", "--config", path])?
            .load(env(&[("P2P_BOOTSTRAP", ""), ("P2P_VALIDATOR", "")]))?;
        assert!(config.bootstrap_peers.is_empty());
        assert!(config.genesis.validators.is_empty());
        assert_eq!(config.max_gossip_events_per_second, 70);
        Ok(())
    }

    #[test]
    fn rejects_bad_file_and_environment_values() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        for contents in [
            "unknown = 1",
            "listen = [",
            "produce_block_interval_ms = 0",
            "chain_path = ''",
            "max_gossip_events_per_second = 0",
        ] {
            std::fs::write(file.path(), contents)?;
            assert!(
                Cli::try_parse_from(["node", "--config", file.path().to_str().unwrap()])?
                    .load(HashMap::new())
                    .is_err(),
                "{contents}"
            );
        }
        let missing = file.path().with_extension("missing");
        assert!(
            Cli::try_parse_from(["node", "--config", missing.to_str().unwrap()])?
                .load(HashMap::new())
                .is_err()
        );
        for (key, value) in [
            ("P2P_LISTEN", ""),
            ("P2P_NODE_KEY_SEED", ""),
            ("P2P_PRODUCE_BLOCK_INTERVAL_MS", "0"),
            ("P2P_MAX_BLOCK_REQUESTS_PER_SECOND", "65536"),
            ("P2P_MAX_GOSSIP_EVENTS_PER_SECOND", "oops"),
            ("P2P_BOOTSTRAP", "peer,,peer"),
        ] {
            assert!(
                Cli::try_parse_from(["node"])?
                    .load(env(&[(key, value)]))
                    .is_err(),
                "{key}"
            );
        }
        Ok(())
    }

    #[test]
    fn validates_the_winning_value() -> anyhow::Result<()> {
        let config = Cli::try_parse_from(["node", "--max-gossip-events-per-second=90"])?
            .load(env(&[("P2P_MAX_GOSSIP_EVENTS_PER_SECOND", "invalid")]))?;
        assert_eq!(config.max_gossip_events_per_second, 90);
        Ok(())
    }

    #[test]
    fn parses_values() -> anyhow::Result<()> {
        let config: Config = Cli::try_parse_from([
            "p2p-chain-lib-cli",
            "--listen=/ip4/127.0.0.1/tcp/4001",
            "--bootstrap=/ip4/127.0.0.1/tcp/4002",
            "--bootstrap=/ip4/127.0.0.1/tcp/4003",
            "--node-key-seed=node-a",
            "--chain-path=/tmp/chain-data",
            "--validator=node-a",
            "--validator=node-b",
            "--produce-block-interval-ms=250",
            "--max-gossip-events-per-second=80",
            "--max-block-requests-per-second=20",
        ])?
        .load(HashMap::new())?;

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
        assert_eq!(
            config.chain_path,
            std::path::PathBuf::from("/tmp/chain-data")
        );
        assert_eq!(config.genesis.validators, vec!["node-a", "node-b"]);
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
        .load(HashMap::new())?;

        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4002");
        Ok(())
    }
}
