use libp2p_mini_chain_network::{PeerAddress, service::NetworkConfig};

const DEFAULT_LISTEN_ADDR: &str = "/ip4/127.0.0.1/tcp/0";

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: PeerAddress,
    pub bootstrap_peers: Vec<PeerAddress>,
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
        "usage: libp2p-mini-chain-cli [--listen <addr>] [--bootstrap <addr>...]"
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: PeerAddress::from(DEFAULT_LISTEN_ADDR),
            bootstrap_peers: Vec::new(),
        }
    }
}

impl From<Config> for NetworkConfig {
    fn from(config: Config) -> Self {
        Self {
            listen_addr: config.listen_addr,
            bootstrap_peers: config.bootstrap_peers,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_defaults_without_args() {
        let config = Config::parse_from(Vec::<String>::new()).unwrap();

        assert_eq!(config.listen_addr.as_str(), DEFAULT_LISTEN_ADDR);
        assert!(config.bootstrap_peers.is_empty());
    }

    #[test]
    fn parses_listen_and_bootstrap_values() {
        let config = Config::parse_from([
            "--listen",
            "/ip4/127.0.0.1/tcp/4001",
            "--bootstrap",
            "/ip4/127.0.0.1/tcp/4002",
            "--bootstrap=/ip4/127.0.0.1/tcp/4003",
        ])
        .unwrap();

        assert_eq!(config.listen_addr.as_str(), "/ip4/127.0.0.1/tcp/4001");
        assert_eq!(
            config
                .bootstrap_peers
                .iter()
                .map(PeerAddress::as_str)
                .collect::<Vec<_>>(),
            vec!["/ip4/127.0.0.1/tcp/4002", "/ip4/127.0.0.1/tcp/4003"]
        );
    }

    #[test]
    fn rejects_missing_flag_value() {
        let error = Config::parse_from(["--listen"]).unwrap_err();

        assert!(error.to_string().contains("--listen requires"));
    }

    #[test]
    fn rejects_unknown_arg() {
        let error = Config::parse_from(["--bad"]).unwrap_err();

        assert!(error.to_string().contains("unknown argument"));
    }
}
