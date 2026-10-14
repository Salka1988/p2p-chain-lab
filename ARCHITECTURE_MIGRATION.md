# Architecture

## Purpose

`p2p-chain-lib` is a small, undeployed P2P blockchain node. A configured
validator set shares a deterministic genesis block, produces signed blocks in a
fixed schedule, and serves history to late-joining observers.

The CLI is the composition root. It constructs adapters and application
services, connects their channels, and supervises their lifetimes.

## Current boundaries

```text
CLI / composition root
  ├── NetworkService (libp2p adapter)
  │     ├── NetworkHandle ───────────────┐
  │     └── PeerReputationHandle ────────┤
  ├── PeerScoreActor                     │
  ├── ChainStore (filesystem adapter) ───┤
  └── Node                               │
        ├── SimpleConsensus              │
        ├── SyncService                  │
        ├── Mempool                      │
        └── BlockStorePort ◀─────────────┘
```

- `chain` owns blocks, hashes, and the `BlockStorePort` storage boundary.
- `storage` owns `ChainStore`: the in-memory cache and incremental filesystem
  persistence. Storage stays abstract in the application layer because it is
  durable external I/O.
- `ports` owns transport-neutral network events plus bounded channel handles
  and commands. It does not depend on libp2p.
- `network` is the libp2p adapter. It translates network traffic to events and
  consumes network commands. Peer scoring and validator-directory discovery
  live here.
- `consensus` validates signed blocks and turns ticks or received data into
  explicit decisions and commands. `SimpleConsensus` is deliberately concrete.
- `sync` is a concrete application service. It schedules bounded, deduplicated
  missing-block requests and replays bounded orphan blocks after their parents
  arrive.
- `node` coordinates consensus, sync, storage, network commands, and the
  mempool. `NodeRuntime` is concrete; it stores only the storage dependency as
  `Arc<dyn BlockStorePort>`.

This is hexagonal architecture where it matters: policy depends on a storage
boundary and transport-neutral commands, while adapters depend inward. It does
not use traits or generics merely to make concrete application services
replaceable.

## Runtime flow

1. The CLI opens `ChainStore`, configures `SimpleConsensus` from genesis, and
   starts the network, peer-score actor, and node under one cancellation tree.
2. Network traffic becomes `NetworkEvent` values. The node validates and
   processes them through consensus.
3. Accepted blocks are stored before they are broadcast. The store writes a
   temporary block file, syncs it, renames it, then updates its memory cache.
4. Missing parents or advertised tips call `SyncService`. Its single worker
   requests blocks with bounded capacity and pacing; it does not poll.
5. A late node starts from the same configured genesis, obtains a tip, and
   requests ancestors until it reaches known history. Orphans are cached and
   replayed when possible.

## Persistence and recovery

`chain_path` is a directory containing one `<hash>.block` file per committed
block. Temporary files are ignored during recovery. On startup, the selected
chain is walked back to configured genesis, then each recovered non-genesis
block is checked for hash, scheduled producer, and signature validity.

The in-memory selected tip is cached; opening a store computes it once.

Use one `ChainStore` writer per directory. Multiple independent writer
processes for the same directory are unsupported.

## Consensus limits

`SimpleConsensus` is a fixed round-robin schedule: the validator at
`height % validator_count` is the only producer for that height. Every
scheduled validator must be online. There is no quorum, vote, timeout, or
leader-failover protocol. Adding one requires a designed consensus protocol,
not a local fallback that skips a validator.

## Verification

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p p2p-chain-lib-network --test network_integration
scripts/test-late-join-sync.sh
```

The final command starts real localhost nodes and is intentionally separate
from the normal unit-test suite.
