# Architecture Migration Status

This document tracks the migration from one broad core crate into small workspace crates under `crates/`.

Goal:

- `bin/cli` is the composition root.
- `bin/cli` owns process startup and concrete service construction.
- Each reusable domain area lives in its own crate under `crates/`.
- Crate boundaries make dependencies and review scope explicit.
- Inner application code receives dependencies instead of constructing concrete adapters.
- Cross-crate behavior is exposed through port traits.
- Concrete structs implement port traits in their owning crates.
- Simple implementations come first so behavior can be tested and replaced later.

## How To Run

Build-check the workspace:

```sh
cargo check
```

Run tests:

```sh
cargo test
```

Run the node:

```sh
RUST_LOG=debug cargo run -p libp2p-mini-chain-cli
```

Run with explicit P2P config:

```sh
RUST_LOG=debug cargo run -p libp2p-mini-chain-cli -- \
  --listen /ip4/127.0.0.1/tcp/4001 \
  --bootstrap /ip4/127.0.0.1/tcp/4002
```

Current runtime behavior is intentionally small but real. The CLI builds concrete services, starts the libp2p-backed network service, starts the node, then waits for `Ctrl-C`. On shutdown, a root `CancellationToken` is cancelled and child tokens stop the node event loop and network service.

Expected default listen address:

```text
/ip4/127.0.0.1/tcp/0
```

## Current Layout

```text
libp2p-mini-chain/
├── Cargo.toml
├── bin/
│   └── cli/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
└── crates/
    ├── chain/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── block.rs
    │       └── store.rs
    ├── mempool/
    │   ├── Cargo.toml
    │   └── src/
    │       └── lib.rs
    ├── network/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── behaviour.rs
    │       ├── command.rs
    │       ├── event.rs
    │       ├── peer_score.rs
    │       └── service.rs
    ├── node/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── config.rs
    │       └── node.rs
    ├── protocol/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── codec.rs
    │       ├── messages.rs
    │       └── validation.rs
    └── sync/
        ├── Cargo.toml
        └── src/
            └── lib.rs
```

## Crate Responsibilities

### `bin/cli`

Owns:

- tracing setup
- config construction trigger
- concrete service construction
- channel creation
- root cancellation token creation
- task spawning
- node construction
- node startup/run
- process exit result

Composition root builds:

- `NetworkService`
- `NetworkHandle`
- `ChainStore`
- `Mempool`
- `SyncService`
- command/event channels
- root `CancellationToken`

Does not own domain behavior:

- networking behavior
- chain rules
- mempool rules
- sync rules
- protocol definitions

Review files:

- `bin/cli/Cargo.toml`
- `bin/cli/src/main.rs`

### `crates/node`

Owns:

- app-level config parsing
- network event routing in `node.rs`
- shutdown wait in `node.rs`
- cancellation-aware event loop stop

Uses port traits:

- `BlockStorePort`
- `MempoolPort`
- `NetworkStatusPort`
- `BlockSendPort`
- `SyncPort`

Review files:

- `crates/node/Cargo.toml`
- `crates/node/src/config.rs`
- `crates/node/src/node.rs`

Design note:

- `bin/cli` creates concrete services such as `NetworkService`.
- `Node` is generic over port traits and receives dependencies from the composition root.
- Current concrete CLI dependencies are `NetworkHandle`, `ChainStore`, `Mempool`, and `SyncService`.
- This keeps `Node` out of concrete adapter construction and preserves the composition-root boundary.

### `crates/network`

Owns:

- app-to-network command types
- network-to-node event types
- network handle
- network port traits
- libp2p network service
- peer score
- libp2p behavior type

Current simple behavior:

- starts a real libp2p swarm
- listens on configured `--listen` multiaddr
- dials configured `--bootstrap` multiaddrs
- wraps peer identities in `PeerId`
- wraps dial/listen multiaddrs in `PeerAddress`
- emits peer connect/disconnect events from real swarm events
- publishes transactions over gossipsub
- publishes block announcements over gossipsub
- receives gossipsub transactions and block announcements
- sends block requests over libp2p request-response
- receives block requests and forwards them to `node`
- sends block responses back through request-response channels
- stops when its `CancellationToken` is cancelled
- `NetworkBehaviour` derives libp2p `NetworkBehaviour` and contains ping, gossipsub, and request-response behavior.

Current public ports:

- `NetworkStatusPort`
- `BlockRequestPort`
- `BlockSendPort`

Public network identity/address types:

- `PeerId` is the app-level peer identity wrapper.
- `PeerAddress` is the app-level multiaddr wrapper used for listen/bootstrap addresses.
- libp2p `PeerId` and `Multiaddr` stay inside `network` adapter code.

Not implemented yet:

- peer score enforcement

Review files:

- `crates/network/Cargo.toml`
- `crates/network/src/command.rs`
- `crates/network/src/event.rs`
- `crates/network/src/service.rs`
- `crates/network/src/peer_score.rs`
- `crates/network/src/behaviour.rs`

### `crates/protocol`

Owns:

- wire/domain message structs
- binary message encoding/decoding
- basic message validation

Current messages:

- `TransactionId`
- `Transaction`
- `BlockAnnouncement`
- `BlockRequest`
- `BlockResponse`

Current simple behavior:

- `codec::encode` serializes messages with `bincode`.
- `codec::decode` deserializes messages with `bincode`.
- `TransactionId` uses private fixed-size bytes: `[u8; 32]`.
- transaction validation rejects zero transaction ids.
- block announcement validation rejects zero block hashes.
- block validation rejects zero hashes, empty bodies, and invalid genesis/parent shape.
- Round-trip tests cover each current protocol message.

Not implemented yet:

- message versioning
- consensus-grade block validation

Review files:

- `crates/protocol/Cargo.toml`
- `crates/protocol/src/codec.rs`
- `crates/protocol/src/messages.rs`
- `crates/protocol/src/validation.rs`

### `crates/chain`

Owns:

- fixed-size `BlockHash` bytes: private `[u8; 32]` with constructors/getters
- `Block`
- in-memory `ChainStore`
- chain storage port trait

Current simple behavior:

- `ChainStore::insert` returns `true` for new blocks and `false` for replacement.
- `ChainStore::get` fetches by hash.
- `ChainStore::len` reports block count.

Current public port:

- `BlockStorePort`

Not implemented yet:

- parent validation
- height validation
- best chain selection
- persistence

Review files:

- `crates/chain/Cargo.toml`
- `crates/chain/src/block.rs`
- `crates/chain/src/store.rs`

### `crates/mempool`

Owns:

- pending transaction storage
- mempool port trait

Current simple behavior:

- stores transactions by fixed-size `TransactionId`
- `Mempool::insert` returns `true` for new transactions and `false` for duplicates
- `Mempool::contains` checks id presence
- `Mempool::len` reports transaction count

Current public port:

- `MempoolPort`

Not implemented yet:

- transaction expiry
- transaction selection
- size limits
- fee or priority ordering

Review files:

- `crates/mempool/Cargo.toml`
- `crates/mempool/src/lib.rs`

### `crates/sync`

Owns:

- block announcement handling
- sync decisions
- block request decisions
- sync port trait

Current simple behavior:

- if an announced block is missing, requests it from the announcing peer
- if an announced block is already known, sends no command

Current public port:

- `SyncPort`

Design note:

- `SyncService` is generic over `BlockRequestPort` and `BlockStorePort`.
- It does not require concrete `NetworkHandle` or `ChainStore`.

Not implemented yet:

- range sync
- peer selection

Review files:

- `crates/sync/Cargo.toml`
- `crates/sync/src/lib.rs`

## Dependency Direction

Current intended dependency graph:

```text
bin/cli
  -> node
  -> chain
  -> mempool -> protocol -> chain
  -> network -> protocol -> chain
  -> sync -> network
          -> protocol
          -> chain

node
  -> chain
  -> mempool -> protocol -> chain
  -> network -> protocol -> chain
  -> sync -> network
          -> protocol
          -> chain
```

Important rule:

- Lower-level crates must not depend on `node`.
- `bin/cli` is allowed to depend on concrete crates because it is the composition root.
- Cross-crate behavior should be expressed through public port traits.
- `protocol` may depend on `chain` because block messages contain `Block` and `BlockHash`.
- `network` moves messages and exposes commands/events. It should not decide sync policy.
- `sync` decides what blocks to request. It should not own libp2p behavior.
- `sync` depends on `BlockRequestPort` and `BlockStorePort`, not concrete network/store structs.
- `node` receives already-built dependencies and routes events through port traits.
- `node` should not construct concrete adapters.

## Implemented

- Workspace split into separate crates under `crates/`.
- CLI is the composition root and depends on concrete crates.
- Old `crates/core` source files removed from workspace.
- `bin/cli` wires concrete services together.
- `node` is generic over port traits and routes events.
- `sync` is generic over port traits and decides block requests.
- `network` owns command/event/service boundaries.
- `network` exposes `NetworkStatusPort` and `BlockRequestPort`.
- `network` exposes `BlockSendPort`.
- `network` handles `BroadcastTransaction` by publishing a gossipsub payload.
- `network` handles `BroadcastBlock` by publishing a gossipsub block announcement.
- `network` handles `RequestBlock` by sending a libp2p request-response request to the chosen peer.
- `network` handles `SendBlock` by answering the stored request-response channel.
- `network` has a real libp2p `NetworkBehaviour` with ping, gossipsub, and request-response.
- `node` validates received blocks before storing them.
- `node` answers inbound block requests through `BlockSendPort`.
- service task failures propagate to process exit status.
- `bin/cli` creates a root `CancellationToken`.
- `NetworkService` and the node event loop stop through child cancellation tokens.
- CLI awaits the network task after shutdown.
- `Node::run` awaits its event-loop task after shutdown.
- `protocol` owns message definitions and basic validation.
- `protocol` owns binary encode/decode helpers and round-trip tests.
- `chain` owns block/store types.
- `chain` exposes `BlockStorePort`.
- `mempool` owns transaction storage.
- `mempool` exposes `MempoolPort`.
- `sync` owns block announcement decisions.
- `sync` exposes `SyncPort`.
- `node` parses `--listen` and repeated `--bootstrap` arguments.
- Focused tests exist for current simple behavior.
- `cargo check` passes after split.

## Tests

Run:

```sh
cargo test
```

Current focused tests cover:

- config defaults and explicit CLI argument parsing
- mempool rejects duplicate transaction ids
- chain store inserts and fetches blocks
- protocol validation accepts/rejects basic message shapes
- protocol validation accepts/rejects basic block shapes
- protocol serialization round-trips each current message
- sync requests missing announced blocks
- sync skips already-known announced blocks
- network service encodes transaction broadcast payloads
- network service encodes block announcement payloads
- network behaviour builds with libp2p ping, gossipsub, and request-response

## Review Order

Review in this order to track the migration cleanly:

1. `Cargo.toml`
2. `bin/cli/Cargo.toml`
3. `bin/cli/src/main.rs`
4. `crates/node/Cargo.toml`
5. `crates/node/src/config.rs`
6. `crates/node/src/node.rs`
7. `crates/chain/Cargo.toml`
8. `crates/chain/src/block.rs`
9. `crates/chain/src/store.rs`
10. `crates/protocol/Cargo.toml`
11. `crates/protocol/src/codec.rs`
12. `crates/protocol/src/messages.rs`
13. `crates/protocol/src/validation.rs`
14. `crates/mempool/Cargo.toml`
15. `crates/mempool/src/lib.rs`
16. `crates/network/Cargo.toml`
17. `crates/network/src/command.rs`
18. `crates/network/src/event.rs`
19. `crates/network/src/service.rs`
20. `crates/network/src/peer_score.rs`
21. `crates/network/src/behaviour.rs`
22. `crates/sync/Cargo.toml`
23. `crates/sync/src/lib.rs`

## Next Small Steps

Keep each next step small and testable:

1. Add an integration test that starts two local nodes and verifies peer connection.
2. Add an integration test for gossipsub transaction/block-announcement flow.
3. Add an integration test for request-response block fetching.
4. Add peer score enforcement and simple bad-message penalties.
5. Add block parent/height validation in `chain` or `protocol`.

## Acceptance Checklist

Current status:

- [x] `bin/cli/src/main.rs` is the composition root.
- [x] Each main domain area has its own crate under `crates/`.
- [x] CLI wires concrete services together.
- [x] `node` parses explicit listen/bootstrap config.
- [x] `node` receives generic port dependencies instead of concrete service types.
- [x] `node` owns shutdown waiting.
- [x] Cancellation token stops network service and node event loop.
- [x] Spawned network and node event-loop tasks are joined during shutdown.
- [x] Message types stay inside `protocol`.
- [x] Protocol messages have binary serialization and round-trip tests.
- [x] Chain state stays inside `chain`.
- [x] Pending transactions stay inside `mempool`.
- [x] Sync decisions stay inside `sync`.
- [x] Network command/event boundaries stay inside `network`.
- [x] `BroadcastTransaction` is handled and published inside `network`.
- [x] `BroadcastBlock` is handled as a published block announcement inside `network`.
- [x] `RequestBlock` is handled as a request-response block request inside `network`.
- [x] `SendBlock` is handled as a request-response block response inside `network`.
- [x] Received blocks are validated before being stored.
- [x] Real libp2p ping/gossipsub/request-response behavior stays inside `network`.
- [x] Service task failures are propagated into process exit status.
- [x] Project builds with `cargo check`.
- [x] Focused tests exist for simple domain behavior.
- [x] Real libp2p swarm event loop exists.
- [x] Real block request/response flow exists.
- [x] Real transaction broadcast flow exists.
- [ ] Two-node integration tests exist.
- [ ] Peer score enforcement exists.
