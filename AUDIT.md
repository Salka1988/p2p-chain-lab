# P2P Node Audit

## Current status — 2026-10-14

This is a small, undeployed P2P blockchain node with persistent blocks,
scheduled validators, and late-join synchronization. The current architecture
keeps `SyncService`, `SimpleConsensus`, and `NodeRuntime` concrete; only
durable storage remains behind `BlockStorePort`.

### Good foundations

- The CLI is the composition root and supervises node, network, and peer-score
  workers with cancellation.
- Network I/O is translated through transport-neutral events and bounded channel
  handles; application code has no libp2p dependency.
- Consensus emits explicit outcomes and commands. Accepted blocks are persisted
  before broadcast.
- Sync is event-driven, bounded, deduplicated, paced, and cancellation-safe.
- Storage writes one block at a time, caches the selected tip, and validates the
  recovered selected chain before starting production.
- The mempool and orphan cache have fixed limits.

### Remaining design limitation

`SimpleConsensus` has a fixed round-robin producer schedule. If the validator
scheduled for the next height is offline, the chain stops. This is documented in
CLI help and [the architecture guide](ARCHITECTURE_MIGRATION.md).

Do not implement local leader skipping: it creates forks. Fault tolerance needs
a designed protocol with rounds, timeouts, votes, and a quorum rule.

### Verification

```text
cargo fmt --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo test -p p2p-chain-lib-network --test network_integration --offline
```

All unit tests and all nine localhost network integration tests passed. The
six-node late-join test is intentionally separate and was not rerun in this
audit pass.

## Resolved audit history

- Persistence now commits disk before memory; post-rename directory-sync errors
  no longer split running memory from the visible block file.
- Node event-loop and sync-worker failures are supervised and propagated.
- Sync no longer creates unbounded tasks; failed requests are cleared for retry.
- Consensus distinguishes accepted, known, missing-parent, rejected, and
  no-change outcomes.
- Storage is an adapter; node and sync depend on storage through
  `BlockStorePort`.
- `selected_tip` is required by the storage port and cached by `ChainStore`.
- Recovered non-genesis blocks are checked for hash, producer, and signature.
- Network request responses include not-found, block responses are matched to
  requests, and validator-directory records are authenticated and expiring.
- Mempool and orphan storage are bounded.
- Architecture documentation now matches the concrete service and channel-based
  boundaries.
