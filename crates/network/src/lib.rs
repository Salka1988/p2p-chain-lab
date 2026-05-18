pub mod behaviour;
pub mod command;
pub mod event;
pub mod peer_score;
pub mod service;

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for PeerId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for PeerId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerAddress(String);

impl PeerAddress {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for PeerAddress {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for PeerAddress {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}
