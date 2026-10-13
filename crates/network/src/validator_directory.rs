use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use libp2p::{Multiaddr, PeerId, kad::RecordKey};
use serde::{Deserialize, Serialize};

use p2p_chain_lib_protocol::codec;

pub(crate) const VALIDATOR_RECORD_PREFIX: &str = "/p2p-chain-lib/validator/";
const MAX_RECORD_AGE_MS: u64 = 5 * 60 * 1000;
const MAX_FUTURE_SKEW_MS: u64 = 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatorRecord {
    pub validator_id: String,
    pub peer_id: String,
    pub listen_addrs: Vec<String>,
    pub updated_at_ms: u64,
    pub signature: Vec<u8>,
}

impl ValidatorRecord {
    pub(crate) fn new(
        validator_id: impl Into<String>,
        peer_id: PeerId,
        listen_addrs: impl IntoIterator<Item = Multiaddr>,
        signing_key: &SigningKey,
    ) -> anyhow::Result<Self> {
        let mut record = Self {
            validator_id: validator_id.into(),
            peer_id: peer_id.to_string(),
            listen_addrs: listen_addrs
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            updated_at_ms: current_time_ms(),
            signature: Vec::new(),
        };
        anyhow::ensure!(
            record.validator_id == validator_id_for_key(&signing_key.verifying_key()),
            "validator record signer does not match validator id"
        );
        record.signature = signing_key
            .sign(&signing_bytes(&record))
            .to_bytes()
            .to_vec();
        Ok(record)
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

pub(crate) fn verify_record(record: &ValidatorRecord) -> anyhow::Result<()> {
    let now = current_time_ms();
    anyhow::ensure!(
        record.updated_at_ms <= now.saturating_add(MAX_FUTURE_SKEW_MS),
        "validator record timestamp is too far in the future"
    );
    anyhow::ensure!(
        now.saturating_sub(record.updated_at_ms) <= MAX_RECORD_AGE_MS,
        "validator record has expired"
    );
    let key = verifying_key(&record.validator_id)?;
    let signature = Signature::from_slice(&record.signature)
        .map_err(|_| anyhow::anyhow!("validator record signature has invalid length"))?;
    key.verify(&signing_bytes(record), &signature)
        .map_err(|_| anyhow::anyhow!("validator record signature is invalid"))
}

pub(crate) fn validator_id_for_key(key: &VerifyingKey) -> String {
    key.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verifying_key(value: &str) -> anyhow::Result<VerifyingKey> {
    anyhow::ensure!(
        value.len() == 64,
        "validator id must be a 32-byte public key encoded as hex"
    );
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("validator id must be hexadecimal"))?;
    }
    VerifyingKey::from_bytes(&bytes)
        .map_err(|_| anyhow::anyhow!("validator id is not a valid Ed25519 public key"))
}

fn signing_bytes(record: &ValidatorRecord) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in std::iter::once(&record.validator_id)
        .chain(std::iter::once(&record.peer_id))
        .chain(record.listen_addrs.iter())
    {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes.extend_from_slice(&record.updated_at_ms.to_le_bytes());
    bytes
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn signing_key(seed: &str) -> SigningKey {
        let bytes: [u8; 32] = Sha256::digest(seed.as_bytes()).into();
        SigningKey::from_bytes(&bytes)
    }

    #[test]
    fn validator_record_round_trips() -> anyhow::Result<()> {
        let peer_id = PeerId::random();
        let key = signing_key("node-a");
        let record = ValidatorRecord::new(
            validator_id_for_key(&key.verifying_key()),
            peer_id,
            ["/ip4/127.0.0.1/tcp/4001".parse()?],
            &key,
        )?;

        let encoded = encode_record(&record)?;
        let decoded = decode_record(&encoded)?;

        assert_eq!(
            decoded.validator_id,
            validator_id_for_key(&key.verifying_key())
        );
        assert_eq!(decoded.peer_id, peer_id.to_string());
        assert_eq!(decoded.listen_addrs, vec!["/ip4/127.0.0.1/tcp/4001"]);
        verify_record(&decoded)?;
        Ok(())
    }

    #[test]
    fn rejects_tampered_or_wrongly_signed_records() -> anyhow::Result<()> {
        let key = signing_key("node-a");
        let mut record = ValidatorRecord::new(
            validator_id_for_key(&key.verifying_key()),
            PeerId::random(),
            ["/ip4/127.0.0.1/tcp/4001".parse()?],
            &key,
        )?;
        record.peer_id = PeerId::random().to_string();
        assert!(verify_record(&record).is_err());
        Ok(())
    }

    #[test]
    fn rejects_expired_record() -> anyhow::Result<()> {
        let key = signing_key("node-a");
        let mut record = ValidatorRecord::new(
            validator_id_for_key(&key.verifying_key()),
            PeerId::random(),
            ["/ip4/127.0.0.1/tcp/4001".parse()?],
            &key,
        )?;
        record.updated_at_ms = current_time_ms() - MAX_RECORD_AGE_MS - 1;
        record.signature = key.sign(&signing_bytes(&record)).to_bytes().to_vec();

        assert!(verify_record(&record).is_err());
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
