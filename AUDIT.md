# P2P Node Audit

## Architecture and Rust audit — 2026-10-13

What this repo does: a small P2P blockchain node with shared genesis, scheduled
validators, persistent blocks, and late-join synchronization. This review assumes
an undeployed development network, not a production consensus system.

The structure is a reasonable start. The main problems are ownership and failure
handling, not missing generics.

### What is good

- The CLI is the composition root: it constructs services and connects dependencies.
- SyncService uses meaningful ports for block requests, storage, and reputation,
  allowing tests without a running network.
- Passing SyncService directly is fine. An application service does not need a
  forwarding trait just because another service calls it.
- Consensus returns explicit commands rather than broadcasting through libp2p.
- Typed hashes, enums, bounded channels, cancellation tokens, and bounded
  mempool/orphan storage are useful Rust choices.
- Tests cover shared genesis, invalid signatures, reverse-order history,
  persistence reopening, and task shutdown.

### Must fix

#### 1. Consensus and storage advance before persistence succeeds

Status: fixed in the working tree. ChainStore persists the candidate snapshot
before inserting into memory. Consensus now derives its next position from the
stored tip instead of maintaining a second, prematurely advanced producer state.
Node still executes storage before broadcast and stops command execution on error.

Regression coverage forces a real filesystem write failure for both production
and received-block paths, checks unchanged memory and disk, then retries the same
block and verifies persistence after reopening.

Fix verification: 72 library/binary tests, workspace Clippy with warnings denied,
formatting checks, and the six-node late-join integration test all passed.
This fixes write-error ordering; power-loss durability and incremental storage
remain outside this change.

Sources: [consensus](crates/consensus/src/lib.rs),
[storage](crates/storage/src/lib.rs).

Consensus updates its next height before StoreBlock executes. ChainStore inserts
into memory before writing the file. A failed write leaves memory ahead of disk;
retrying the same block skips persistence because it already exists.

Fix: persist successfully before committing the in-memory chain transition, then
broadcast. Add a failing-write regression test.

If skipped: restart can lose blocks the running node treated as accepted.

#### 2. The node's real task is not properly supervised

Status: fixed in the working tree. Node::new only stores its runtime. Node::run
owns the event loop directly and uses a single cancellation token; a closed
network event stream returns an error immediately, and panics reach the supervising
task. Delayed sync tasks are owned by a JoinSet and aborted when the loop is
dropped, rather than detached.

Verification: 76 library/binary tests and workspace Clippy with warnings denied
passed. New regressions cover construction without a Tokio runtime, early stream
closure, cancellation, and propagation of event-loop panics. Live TCP/six-node
tests were not rerun for this lifecycle change.

Source: [node lifecycle](crates/node/src/node.rs).

Node::new starts the event loop, but Node::run waits for cancellation before
observing its result. An event-loop panic is not immediately reported through
the CLI supervisor.

Fix: make new construct only, and let run own the event loop and its cancellation.

If skipped: the process can remain alive with its node loop dead.

#### 3. Sync work can grow beyond the bounded queues

Status: fixed in the working tree. One node-owned, supervised worker replaces
per-tip tasks. Sync handlers enqueue work without waiting for request pacing or
transport capacity. The concrete SyncService and its existing I/O ports remain;
no new forwarding trait or generic NodeRuntime was added.

Queued and in-flight requests share a 128-entry limit, with at most 64 tip
requests, parent priority, hash deduplication, and ten-second expiry. Dispatch
retains the 250ms tip grace period and 125ms pacing. A stalled transport send
times out without penalizing the peer. Full queues drop new work and rely on
later announcements; expired requests can likewise be retried on reannouncement.
Cached orphan ancestry is followed to the missing block rather than downloaded
again. Dropping the node loop aborts its worker.

The worker is event-driven: enqueue, parent promotion, and request removal notify
it to recompute its next deadline. With no pending work it waits without a timer;
otherwise it waits for readiness/pacing or the earliest expiry. Sync activity is
triggered by service calls, not a periodic polling loop. Regression tests check
idle wake-ups, waking for new work, expiry back to idle, and parent promotion
before the original tip deadline.

Regression tests cover capacity, reserved parent slots, duplicate requests,
expiry, cached-ancestry retries, pacing across clones, worker cancellation, and
stalled transport. The first live run exposed repeated orphan downloads; that
regression was fixed before the final verification.

Verification: workspace library/binary tests, all nine network integration tests,
and the six-node late-join test passed (22.57s on the event-driven worker). Workspace Clippy
with warnings denied, formatting, and diff whitespace checks also passed.

Sources: [tip handling](crates/node/src/node.rs),
[request pacing](crates/sync/src/lib.rs).

Original issue: every received tip spawned a task. Request pacing serialized those tasks
at eight requests per second. Distinct unknown tips arriving faster than that
accumulate waiting tasks; parent requests also wait behind them while the node
is processing an event.

Fix: use bounded, deduplicated scheduling owned by the node, with cancellation
and expiry.

If skipped: sustained traffic can increase memory use and delay synchronization.

### Should fix

#### 4. Consensus outcomes are too ambiguous

Status: fixed in the working tree. Consensus returns a ConsensusDecision with
explicit Accepted, AlreadyKnown, MissingParent, Rejected, or NoChange outcome
and the commands to execute. Accepted describes validation, not a committed write:
the node returns it only after command execution succeeds. Storage still precedes
broadcast, and execution failures remain errors.

The node requests ancestry only for MissingParent, clears sync state for Accepted
or AlreadyKnown, and does not repeat candidate validation. AlreadyKnown no longer
emits a redundant StoreBlock command. Consensus validates a received candidate
once, then separately checks its chain position. Tests assert received acceptance,
duplicate handling, missing ancestry without penalties, and rejection of invalid
orphans and wrong-height blocks.

Verification: workspace library/binary tests, Clippy with warnings denied,
formatting, and the six-node late-join regression passed (22.58s).

Sources: [block handling](crates/node/src/node.rs),
[consensus commands](crates/consensus/src/lib.rs).

Node interprets a boolean and validates the candidate again to decide whether
sync should act. Already-known blocks generate StoreBlock just to signal success.

Fix: return explicit outcomes such as Accepted, AlreadyKnown, MissingParent, and
Rejected. Tie peer reporting and sync decisions to those outcomes.

If skipped: future changes can confuse “not yet applicable” with “invalid,”
the distinction behind the earlier sync bug.

#### 5. Domain and infrastructure responsibilities still overlap

Status: fixed. The chain crate retains
Block, BlockHash, and BlockStorePort. The new storage adapter crate owns
ChainStore, its memory cache, filesystem I/O, snapshot serialization, and storage
tests. The domain no longer depends on Tokio or bincode. Consensus and sync use
the storage adapter only as a dev-dependency for tests.

The ports crate owns concrete NetworkHandle and PeerReputationHandle channel
senders and their transport-neutral commands. The network adapter consumes these
commands; libp2p and scoring policy stay in that adapter. The CLI wires the
workers and handles. NodeRuntime and SyncService are non-generic, with only
storage behind Arc<dyn BlockStorePort>; consensus remains concrete.
The node has no network adapter dependency, even in tests. Storage is only a
dev-dependency. Channel receivers let tests inspect commands without fake trait
implementations or a running transport. Network commands retain bounded waiting;
reputation updates retain their existing best-effort full/closed-queue behavior.
These channel-based ports intentionally depend on Tokio, not libp2p.

At the completion of #5, persistence format and write-before-memory ordering were unchanged. Existing
reopen, failed-write/retry, and genesis-mismatch tests exercise the moved adapter.

Verification: workspace library/binary tests, workspace Clippy with warnings
denied, formatting, all nine network integration tests, and the six-node late-join
test passed. A recording transport test
checks known-block and not-found responses through the node's network port;
the reverse-history test checks every recovered block through the storage port.

Sources: [storage adapter](crates/storage/src/lib.rs),
[storage port](crates/chain/src/store.rs),
[node dependencies](crates/node/src/node.rs).

Original finding: chain storage combined a port, memory storage, filesystem
operations, and serialization. Node named concrete network and storage adapters.

Fix: move filesystem persistence into a storage adapter. If node is intended as
the inner application layer, give its external I/O dependencies narrow contracts.
Keep concrete application services like SyncService.

If skipped: replacing storage or transport requires changing application wiring
inside that layer.

Hexagonal architecture means separating external I/O from application policy,
not putting a trait around every struct.

#### 6. The storage port has a misleading default

Status: fixed in the working tree. selected_tip is now required, so adapters
cannot silently inherit an empty-chain result. ChainStore, the only current
implementation, already implements it explicitly; runtime behavior is unchanged.

Verification: workspace library/binary tests, Clippy with warnings denied, and
formatting checks passed. Live TCP tests were not rerun for this contract-only
change. Fallible database reads remain future work, not part of this fix.

Source: [BlockStorePort](crates/chain/src/store.rs).

selected_tip defaults to None. An implementation that forgets this method silently
looks like an empty chain, affecting genesis initialization and production.

Fix: make selected_tip required. When adding fallible database reads, distinguish
“not found” from “read failed” with Result<Option<_>, _>.

If skipped: a new adapter can compile while supplying incorrect chain state.

#### 7. Persistence rewrites the whole chain for every block

Status: fixed. ChainStore writes one serialized block to a temporary file, syncs
it, renames it into place, and syncs the block directory before updating memory.
Existing block files are not rewritten. Opening and writing run on Tokio blocking
workers; reads can use the committed memory cache while a disk write is pending.
A shared writer gate serializes inserts, and the worker owns the memory update
and gate so cancelling its caller cannot split the disk and memory commit.

chain_path now names a directory (default chain-data), not a snapshot file.
Old snapshot files are rejected without modification; no migration is provided
for this undeployed format. Reopen ignores unfinished .tmp files and rejects
corrupt committed files or filename/hash mismatches. Use one writer per directory
and clone its handle; independent opens are intended for read-only inspection.

Tests cover incremental writes, concurrent duplicate inserts, reopen with an
unfinished file, corrupt files, old-format rejection, cancellation during commit,
and consensus persistence failure/retry. Full-history loading and memory usage
remain proportional to chain size; this is not an indexed database or pruning.

Verification: workspace library/binary tests, Clippy with warnings denied,
formatting, shell syntax, and the six-node late-join test passed (19.66s).

Source: [persistence implementation](crates/storage/src/lib.rs).

Original finding: each insertion clones and serializes all blocks, then performs synchronous
filesystem operations while holding the store lock. Each insertion becomes more
expensive as history grows and blocks other store operations.

Fix: move blocking I/O off async workers; use incremental storage before testing
substantial histories.

If skipped: short sync tests hide increasingly slow catch-up.

#### 8. Architecture documentation describes code that no longer exists

Source: [architecture document](ARCHITECTURE_MIGRATION.md).

The document lists removed ports and says Node is generic. It also describes
persistence and other implemented features as missing.

Fix: document current boundaries and clearly separate planned changes.

If skipped: it encourages reintroducing abstractions deliberately removed.

### Verdict and verification

Keep the concrete SyncService. Findings 1–4 are fixed. Improve adapter
boundaries afterward, without a broad “make everything generic” refactor.

Checked: 71 library/binary tests passed with:

```text
cargo test --workspace --lib --bins --locked
```

No code changed during this review.

Not checked in this review: live TCP/six-node tests, load benchmarks, or a full
cryptographic/security audit.

## Earlier audit record

The following notes and verification results are retained as history. Their
resolved status does not supersede the open findings above.

The workspace builds cleanly with Clippy, and all unit tests pass. The real
libp2p integration tests also pass when allowed to bind localhost sockets.

## Earlier status

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
