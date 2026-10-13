# P2P Node Audit

The workspace builds cleanly with Clippy, and all unit tests pass. The real
libp2p integration tests also pass when allowed to bind localhost sockets.

## Current status

The earlier findings below are resolved. A follow-up audit found four release
blockers, which are addressed by the current working tree:

- Docker Compose now uses the deterministic validator public keys for its
  configured node seeds and persists each node's chain data in a named volume.
- Block responses must match the outbound request hash before reaching the
  node; mismatches become retryable request failures.
- The mempool evicts its oldest transaction when its byte or entry limit is
  reached instead of remaining permanently full.
- `ChainStore` persists blocks and consensus restores its next height and tip
  from the stored chain on startup.
- Nodes that receive a valid descendant before its parent cache it within fixed
  count and byte limits, request missing parents, and replay cached children
  after the parent is accepted.

Verification completed:

```text
cargo check --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test -p p2p-chain-lib-network --test network_integration --locked
```

## Good foundations

- Crate boundaries and port traits make node logic testable.
- Task shutdown handles cancellation, timeouts, panics, and cleanup.
- Network integration tests use real localhost libp2p services.
- Invalid encoded gossip is rejected and penalized.

## Historical findings (resolved)

The sections below record the pre-fix risks and the remediation that was
applied; they are retained as audit history.

## Fix first

### 1. Invalid DHT validator records can stop the node

`crates/network/src/service.rs` propagates invalid validator-record decoding,
peer ID parsing, and address parsing errors. Because DHT data is untrusted,
a malformed record can stop `NetworkService` and trigger whole-process
shutdown.

Fix: log and discard invalid records instead of returning an error. Add a test
that supplies invalid record bytes.

### 2. Failed block fetches remain pending forever

`SyncService` marks a missing block pending before issuing a request, but a
network request failure or timeout does not remove that hash. A later
announcement from a healthy peer is then ignored as already pending.

Fix: associate outbound request IDs with block hashes, emit a request-failed
event, and remove the matching pending hash. Add a retry test.

### 3. Invalid block responses prevent a retry

`Node` clears a sync pending hash before consensus validates and accepts the
received block. A peer can answer with an invalid block, and the node then
will not request the real block again.

Fix: clear pending only after the block passes validation and is accepted for
storage. Add an invalid-response-then-valid-response test.

### 4. Peer bans do not survive reconnects

Disconnecting a peer removes its score. A banned peer reconnects with score
zero; since it remains in `banned_peers`, another threshold crossing does not
emit a new ban event or disconnect it.

Fix: maintain a denylist and disconnect/reject banned peers on every
connection, or deliberately expire bans after a configured duration.

### 5. Consensus accepts arbitrary well-shaped blocks

Block validation only checks basic shape. It does not verify block hash,
parent existence, sequential height, expected producer, or a producer
signature. A valid-looking high-height block can advance local producer
state.

Fix: before treating this as more than a toy protocol, define authenticated
block contents and chain-selection rules, then verify them before storage.

### 6. Transactions are neither validated nor bounded

`validate_transaction` exists but received transactions are inserted directly
into the mempool. The mempool also has no count, byte, or age limit, allowing
a peer to accumulate arbitrary memory use with unique transaction IDs.

Fix: validate before insertion and set a fixed mempool capacity/eviction rule.

## Follow-up fixes

### Missing-block request channels can accumulate

When a requested block is absent, the node sends no response and the network
service retains the response channel.

Fix: model `BlockResponse` as either a block or `NotFound`, respond in both
cases, and remove the channel.

### Validator DHT records are not authenticated

Anyone can publish a record for a predictable validator key and direct nodes
to arbitrary addresses.

Fix: sign validator records, verify the signature against the configured
validator identity, and check that the record validator ID matches the
requested validator ID.

### Duplicate log line

`RoutablePeer` logs the same debug event twice in
`crates/network/src/service.rs`.

Fix: remove one line.

## Overload follow-up

### Peer penalties must not be the only overload defense

Peer-score commands can be dropped when their queue is full, so rate-limit
rejection must also update a local denylist that is enforced on reconnect.

Fix: add the rejected peer to the network service denylist before disconnecting
it; peer scoring remains supplementary telemetry.

### Reject the forwarding peer, not the gossip author

Gossipsub distinguishes the connected peer that forwarded a message from the
original author. Queue overload must reject the forwarding peer, because that
is the connection consuming local capacity.

Fix: retain the propagation source through event delivery and use it for
overload rejection.

### Rate-limit cleanup must not scan all peers per message

Pruning every tracked peer on each inbound event is O(number of peers) in the
hot path.

Fix: prune stale rate-limit entries at most once per rate-limit window.

### Signed validator records need expiry

A valid signature alone permits replaying an old address record indefinitely.

Fix: reject records older than five minutes and records more than one minute
ahead of local time.

## Verification run

```text
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p p2p-chain-lib-network --test network_integration -- --nocapture
```

The integration command needs permission to open localhost TCP sockets.
