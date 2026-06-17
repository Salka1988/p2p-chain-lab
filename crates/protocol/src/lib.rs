//! Simple test protocol messages for the p2p chain playground.
//!
//! This is not a real blockchain protocol yet.
//!
//! This crate only has simple message types, encoding/decoding, and basic validation. It is mainly
//! here so we can test p2p communication between nodes.
//!
//! Things still missing for a real protocol:
//!
//! - signed transactions
//! - signed blocks
//! - real block hash verification
//! - validator checks
//! - parent/chain validation
//! - fork choice
//! - proper sync rules
//!
//! For now, treat this as a test protocol, not as a secure chain.

pub mod codec;
pub mod messages;
pub mod validation;
