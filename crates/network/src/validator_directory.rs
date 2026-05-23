use std::time::{SystemTime, UNIX_EPOCH};

use libp2p::{Multiaddr, PeerId, kad::RecordKey};
use serde::{Deserialize, Serialize};

use p2p_chain_lib_protocol::codec;

pub(crate) const VALIDATOR_RECORD_PREFIX: &str = "/p2p-chain-lib/validator/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatorRecord {
    pub validator_id: String,
    pub peer_id: String,
    pub listen_addrs: Vec<String>,
    pub updated_at_ms: u64,
}

impl ValidatorRecord {
    pub(crate) fn new(
        validator_id: impl Into<String>,
        peer_id: PeerId,
        listen_addrs: impl IntoIterator<Item = Multiaddr>,
    ) -> Self {
        Self {
            validator_id: validator_id.into(),
            peer_id: peer_id.to_string(),
            listen_addrs: listen_addrs
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            updated_at_ms: current_time_ms(),
        }
    }
}

pub(crate) fn record_key(validator_id: &str) -> RecordKey {
    RecordKey::new(&format!("{VALIDATOR_RECORD_PREFIX}{validator_id}"))
}

pub(crate) fn encode_record(record: &ValidatorRecord) -> anyhow::Result<Vec<u8>> {
    codec::encode(record)
}

pub(crate) fn decode_record(bytes: &[u8]) -> anyhow::Result<ValidatorRecord> {
    codec::decode(bytes)
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validator_record_round_trips() -> anyhow::Result<()> {
        let peer_id = PeerId::random();
        let record = ValidatorRecord::new("node-a", peer_id, ["/ip4/127.0.0.1/tcp/4001".parse()?]);

        let encoded = encode_record(&record)?;
        let decoded = decode_record(&encoded)?;

        assert_eq!(decoded.validator_id, "node-a");
        assert_eq!(decoded.peer_id, peer_id.to_string());
        assert_eq!(decoded.listen_addrs, vec!["/ip4/127.0.0.1/tcp/4001"]);
        Ok(())
    }

    #[test]
    fn record_key_is_namespaced() {
        let key = record_key("node-a");

        assert!(
            String::from_utf8(key.to_vec())
                .unwrap()
                .starts_with(VALIDATOR_RECORD_PREFIX)
        );
    }
}
