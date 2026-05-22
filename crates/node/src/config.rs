use std::time::Duration;

use p2p_chain_lib_ports::PeerAddress;

const DEFAULT_LISTEN_ADDR: &str = "/ip4/127.0.0.1/tcp/0";

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
    pub node_key_seed: Option<String>,
    pub block_production_interval: Option<Duration>,
}

impl Config {
    pub fn from_args() -> anyhow::Result<Self> {
        Self::parse_from(std::env::args().skip(1))
    }

    pub fn parse_from<I, S>(args: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut config = Self::default();
        let mut args = args.into_iter().map(Into::into);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" => {
                    config.listen_addr = PeerAddress::from(next_value(&mut args, "--listen")?);
                }
                "--bootstrap" => {
                    config
                        .bootstrap_peers
                        .push(PeerAddress::from(next_value(&mut args, "--bootstrap")?));
                }
                "--node-key-seed" => {
                    config.node_key_seed = Some(next_value(&mut args, "--node-key-seed")?);
                }
                "--produce-block-interval-ms" => {
                    config.block_production_interval = Some(parse_duration_ms(next_value(
                        &mut args,
                        "--produce-block-interval-ms",
                    )?)?);
                }
                "--help" | "-h" => {
                    anyhow::bail!("{}", Self::usage());
                }
                _ if arg.starts_with("--listen=") => {
                    config.listen_addr = PeerAddress::from(split_value(&arg, "--listen=")?);
                }
                _ if arg.starts_with("--bootstrap=") => {
                    config
                        .bootstrap_peers
                        .push(PeerAddress::from(split_value(&arg, "--bootstrap=")?));
                }
                _ if arg.starts_with("--node-key-seed=") => {
                    config.node_key_seed = Some(split_value(&arg, "--node-key-seed=")?);
                }
                _ if arg.starts_with("--produce-block-interval-ms=") => {
                    config.block_production_interval = Some(parse_duration_ms(split_value(
                        &arg,
                        "--produce-block-interval-ms=",
                    )?)?);
                }
                _ => anyhow::bail!("unknown argument: {arg}\n\n{}", Self::usage()),
            }
        }

        anyhow::ensure!(
            !config.listen_addr.as_str().is_empty(),
            "listen address cannot be empty"
        );
        anyhow::ensure!(
            config
                .bootstrap_peers
                .iter()
                .all(|peer| !peer.as_str().is_empty()),
            "bootstrap peer cannot be empty"
        );

        Ok(config)
    }

    pub fn usage() -> &'static str {
        "usage: p2p-chain-lib-cli [--listen <addr>] [--bootstrap <addr>...] [--node-key-seed <seed>] [--produce-block-interval-ms <ms>]"
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: PeerAddress::from(DEFAULT_LISTEN_ADDR),
            bootstrap_peers: Vec::new(),
            node_key_seed: None,
            block_production_interval: None,
        }
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{flag} requires a non-empty value"))
}

fn split_value(arg: &str, prefix: &str) -> anyhow::Result<String> {
    let value = arg
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow::anyhow!("missing prefix: {prefix}"))?;

    anyhow::ensure!(!value.is_empty(), "{prefix} requires a non-empty value");

    Ok(value.to_string())
}

fn parse_duration_ms(value: String) -> anyhow::Result<Duration> {
    let milliseconds: u64 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("--produce-block-interval-ms requires a positive integer"))?;
    anyhow::ensure!(
        milliseconds > 0,
        "--produce-block-interval-ms requires a positive integer"
    );
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_defaults_without_args() -> anyhow::Result<()> {
        let config = Config::parse_from(Vec::<String>::new())?;

        assert_eq!(config.listen_addr.as_str(), DEFAULT_LISTEN_ADDR);
        assert!(config.bootstrap_peers.is_empty());
        assert_eq!(config.node_key_seed, None);
        Ok(())
    }

    #[test]
    fn parses_listen_and_bootstrap_values() -> anyhow::Result<()> {
        let config = Config::parse_from([
            "--listen",
            "/ip4/127.0.0.1/tcp/4001",
            "--bootstrap",
            "/ip4/127.0.0.1/tcp/4002",
            "--bootstrap=/ip4/127.0.0.1/tcp/4003",
            "--node-key-seed",
            "node-a",
        ])?;

        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4001");
        assert_eq!(
            config
                .bootstrap_peers
                .iter()
                .map(PeerAddress::as_str)
                .collect::<Vec<_>>(),
            vec!["/ip4/127.0.0.1/tcp/4002", "/ip4/127.0.0.1/tcp/4003"]
        );
        assert_eq!(config.node_key_seed, Some("node-a".to_string()));
        Ok(())
    }

    #[test]
    fn parses_equals_node_key_seed() -> anyhow::Result<()> {
        let config = Config::parse_from(["--node-key-seed=node-a"])?;

        assert_eq!(config.node_key_seed, Some("node-a".to_string()));
        Ok(())
    }

    #[test]
    fn parses_block_production_interval() -> anyhow::Result<()> {
        let config = Config::parse_from(["--produce-block-interval-ms=250"])?;

        assert_eq!(
            config.block_production_interval,
            Some(Duration::from_millis(250))
        );
        Ok(())
    }

    #[test]
    fn rejects_missing_flag_value() -> anyhow::Result<()> {
        let error = match Config::parse_from(["--listen"]) {
            Ok(_) => anyhow::bail!("expected --listen without value to fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("--listen requires"));
        Ok(())
    }

    #[test]
    fn rejects_unknown_arg() -> anyhow::Result<()> {
        let error = match Config::parse_from(["--bad"]) {
            Ok(_) => anyhow::bail!("expected unknown argument to fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("unknown argument"));
        Ok(())
    }
}
