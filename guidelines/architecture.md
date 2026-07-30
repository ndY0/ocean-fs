# OceanFS — Architectural Rules

**Version:** 0.2.0 — Draft
**Date:** 2026-07-30

---

## 1. Crate Layout

OceanFS is a Cargo workspace. Every major subsystem is its own crate.
This enforces compilation boundaries, keeps iteration fast, and
prevents accidental coupling.

### 1.1 Crate Dependency Graph (DAG)

```
                       oceanfs-core
                            |
         +-------+------+--+---+-------+---------+
         |       |      |      |       |         |
       hash     ec     |   storage  routing   membership
         |       |     |      |       |         |
         +---+---+      |      |       |         |
             |          |      |       |         |
           accel---------      |       |         |
             |                 |       |         |
           cache        network |       |         |
             |             |    |       |         |
             +------+------+----+-------+---------+
                         |
                  oceanfs-server
                         |
                    oceanfs-node
                         |
                    oceanfs (binary)
```

### 1.2 Crate Responsibilities

| Crate | Depends On (internal) | Public API |
|---|---|---|
| `oceanfs-core` | — | `Config`, `BucketPolicy`, `Hlc`, `Error`, `Result`, shared protobuf message types, version types |
| `oceanfs-hash` | `core` | `Blake3Hasher` (streaming), `BatchHasher` (multi-chunk), `HashOutput` |
| `oceanfs-ec` | `core` | `trait Encoder`, `trait Decoder`, `StripeLayout`, `CodecConfig`, `ShardData` |
| `oceanfs-accel` | `core`, `ec` | `AccelTier`, `AccelDispatcher`, feature-gated `CudaBackend` |
| `oceanfs-storage` | `core`, `hash`, `ec`, `accel` | `trait SegmentStore`, `trait MetadataStore`, `trait WalWriter`, `SegmentHandle`, `SegmentIndex`, `BufferPool` |
| `oceanfs-routing` | `core` | `Ring`, `RingCache`, `RouteRequest`, `RouteResponse`, `RoutingError` |
| `oceanfs-membership` | `core` | `Membership`, `NodeState`, `FailureDetector`, `GossipConfig` |
| `oceanfs-network` | `core` | `ConnectionPool`, `trait RpcClient`, `RpcConfig` |
| `oceanfs-cache` | `core` | `ObjectCache`, `MetadataCache`, `NegativeCache`, `CacheStats` |
| `oceanfs-server` | `core`, `storage`, `routing`, `membership`, `network`, `cache` | `S3Handler`, `WriteCoordinator`, `ReadCoordinator`, `AdminHandler` |
| `oceanfs-node` | `core`, `server` | `Node`, `NodeConfig`, `BackgroundTasks` (heal, scrub, gc) |
| `oceanfs` | `core`, `node` | Binary entrypoint only: `main`, CLI args, signal handling |

### 1.3 Dependency Enforcement

All internal dependencies form a directed acyclic graph (DAG).
Circular dependencies are forbidden.

**CI check:**
```bash
# Crates with circular internal deps will show bidirectional edges
cargo tree --edges normal --depth 1 -p oceanfs-node | grep oceanfs
```

Any `oceanfs-*` crate appearing in both "depends on" and "depended by"
positions for the same pair is a CI failure.

**`oceanfs-core` purity check:**
```bash
# core must have no internal dependencies
cargo tree --edges normal -p oceanfs-core | grep oceanfs-
# Expected output: (none)
```

---

## 2. Cross-Crate Coupling

### 2.1 Traits in the Consuming Crate

Trait definitions live in the crate that **consumes** the trait — not
the crate that provides the implementation.

Example: `oceanfs-server` needs to write segments. It defines:

```rust
// oceanfs-server/src/segment_store.rs
pub trait SegmentStore: Send + Sync {
    async fn append(&self, key: &Key, data: Bytes) -> Result<SegmentHandle>;
    async fn seal(&self, handle: SegmentHandle) -> Result<SegmentMetadata>;
}
```

`oceanfs-storage` provides the RocksDB-backed implementation of this
trait. `oceanfs-server` never imports `oceanfs-storage::SegmentStore`;
dependency injection wires the concrete type at startup.

**Rationale:** Inverting the dependency so `server` does not depend on
`storage` (it depends on `core` for the types, and wire-up happens
in `oceanfs-node`). This prevents `server` from accidentally reaching
into `storage` internals.

**Exception:** Traits that are fundamental to the domain and consumed
by many crates may live in `oceanfs-core`. Example: `Encodable` (for
EC-aware types) is fine in `core` because it is a cross-cutting concern.

### 2.2 Public API = Facade Re-exports

Each crate's `src/lib.rs` is a facade that re-exports only the types
needed by dependent crates. All implementation modules are `pub(crate)`
or private.

```rust
// oceanfs-storage/src/lib.rs
mod segment;
mod wal;
mod metadata;
mod buffer_pool;

pub use segment::SegmentHandle;
pub use segment::SegmentMetadata;
pub use metadata::MetadataStore;
pub use wal::WalWriter;
pub use buffer_pool::BufferPool;
```

**Enforcement:** Grep for `pub (in crate` or `pub fn` in
`src/lib.rs` vs any internal module. Internal modules must not
contain `pub` items unless re-exported from `lib.rs`.

### 2.3 Feature Gates for Optional Subsystems

Optional functionality is gated behind Cargo features. Each feature
maps to a dedicated code path, not scattered `#[cfg]` on individual
functions.

```toml
# oceanfs-accel/Cargo.toml
[features]
default = []
cuda = ["dep:cudarc"]
isa-l = ["dep:isal-rs"]
```

```rust
// oceanfs-accel/src/lib.rs
#[cfg(feature = "cuda")]
mod cuda;
#[cfg(feature = "cuda")]
pub use cuda::CudaBackend;

#[cfg(feature = "isa-l")]
mod isal;
#[cfg(feature = "isa-l")]
pub use isal::IsalEncoder;
```

**Enforcement:** Feature-gated code lives in dedicated modules.
`#[cfg(feature = "...")]` at the function level is allowed only
when the alternative is duplicating an entire module.

### 2.4 Protobufs: Messages in Core, Services in Owners

Protobuf message types that cross crate boundaries are defined in
`oceanfs-core/proto/`. Each crate that provides RPC services defines
its own service definition.

```
oceanfs-core/proto/
  segment.proto        → SegmentAppendRequest, FetchShardRequest, ...
  membership.proto     → GossipMessage, MembershipState, ...
  common.proto         → BucketId, ObjectKey, HlcTimestamp, ...

oceanfs-storage/proto/
  storage.proto        → service SegmentRpc { ... }    ← service def
                        // imports oceanfs-core segment.proto for messages

oceanfs-membership/proto/
  gossip.proto         → service GossipRpc { ... }     ← service def
                        // imports oceanfs-core membership.proto for messages
```

**Rationale:** Messages are shared; services belong to the crate that
implements them. A dependent crate should not need to import the full
gRPC client/server stubs when it only needs the data types.

---

## 3. Module Rules (Within a Crate)

### 3.1 `lib.rs` Is a Facade

`src/lib.rs` contains only:
- `pub mod submodule;` declarations
- `pub use submodule::PublicType;` re-exports
- Crate-level attributes (`#![deny(...)]`, `#![doc = ...]`)
- Crate-level documentation (`//!`)

No `impl` blocks, no `fn` definitions, no `const` definitions in
`lib.rs`.

**Rationale:** Makes the crate's public surface auditable in a single
screen. A new developer reads `lib.rs` and knows exactly what the
crate provides.

### 3.2 Default Visibility: `pub(crate)`

All items default to `pub(crate)` unless they appear in the crate's
public API (re-exported from `lib.rs`). Items only used within their
module remain private.

```rust
// oceanfs-storage/src/segment.rs
pub(crate) struct ActiveSegment {   // visible to sibling modules
    buffer: BytesMut,
    cursor: u64,
}

struct StripeLayout {               // private to this module
    data_shards: u8,
    parity_shards: u8,
}

pub struct SegmentHandle {          // re-exported in lib.rs -> public API
    id: SegmentId,
    node_ids: Vec<NodeId>,
}
```

### 3.3 Module Files Match Type Ownership

Each public type gets its own file. The file name matches the type
name in `snake_case`. Private helper types live alongside the public
type they serve.

```
oceanfs-storage/src/
  segment.rs         → pub struct SegmentHandle, impl SegmentHandle
                        pub(crate) struct ActiveSegment
                        struct StripeLayout (private, used only by the above)
  segment_index.rs   → pub(crate) struct SegmentIndex
  metadata.rs        → pub struct MetadataStore
  wal.rs             → pub struct WalWriter
  buffer_pool.rs     → pub struct BufferPool
  error.rs           → pub enum Error
  lib.rs             → facade
```

**No `mod.rs` with implementation.** The only exception is a top-level
`lib.rs` (which acts as the crate root). Deeper directory structures
use `mod.rs` only when a group of related types genuinely belong
together and sharing a module makes sense for cohesion — not for
file-count reduction.

---

## 4. Start-up & Wiring

### 4.1 Construction Happens in `oceanfs-node`

`oceanfs-server` defines what the system does (S3 handlers,
coordinators). `oceanfs-node` wires the concrete implementations
together — it is the composition root.

```rust
// oceanfs-node/src/node.rs
pub struct Node {
    config: NodeConfig,
    server: Server,
    background: BackgroundTasks,
}

impl Node {
    pub async fn start(config: NodeConfig) -> Result<Self> {
        let metadata = RocksDbMetadataStore::open(&config.data_dir)?;
        let segment_store = SegmentStore::new(&config, metadata.clone());
        let encoder = AccelDispatcher::new(&config.acceleration);
        let ring = Ring::new(&config.ring);
        let membership = Membership::new(&config.gossip, ring.clone());
        let pool = ConnectionPool::new(&config.grpc);
        let server = Server::new(segment_store, ring, membership, pool, ...);
        // ...
    }
}
```

`oceanfs-server` never imports `oceanfs-storage`, `oceanfs-membership`,
or any concrete crate. It imports only `oceanfs-core` for types and
traits.

### 4.2 Async Runtime Is Owned by `oceanfs-node`

`tokio::main` lives in `oceanfs` (the binary). `oceanfs-node` starts
background tasks on the provided runtime handle.

Other crates spawn tasks (`tokio::spawn`) but never own the runtime.
They receive a `tokio::runtime::Handle` or `Arc<Runtime>` from the
composition root.

---

## 5. Testing Boundaries

### 5.1 Unit Tests (Colocated)

`#[cfg(test)] mod tests { }` at the bottom of each source file.
Tests the module's public and `pub(crate)` functions.

### 5.2 Crate Integration Tests

`tests/` at the crate root exercises **exactly one crate's public API**.
The test sets up minimal mocks or in-memory implementations for the
crate's dependencies.

```
oceanfs-storage/tests/
  segment_lifecycle.rs   → writes, seals, reads back
  wal_recovery.rs        → crash simulation, replay
  metadata_crud.rs       → put, get, list, delete
```

### 5.3 Cross-Crate Integration Tests

`oceanfs-node/tests/` exercises multi-crate scenarios: full write
path through HTTP → coordinator → storage → EC → read back.

```
oceanfs-node/tests/
  write_read_roundtrip.rs   → PUT → GET → hash matches
  multi_node_cluster.rs     → 3-node mini-cluster, kill one, read still works
  healing.rs                → node failure, data recovered
  cache_behavior.rs          → L1/L2/L3 hit/miss
```

### 5.4 Dependency Injection for Testing

All crates accept their dependencies as `Arc<dyn Trait>` (or equivalent)
at construction time. Tests inject mock or in-memory implementations.
Construction in `oceanfs-node` wires the real implementations.

```rust
// In oceanfs-server
pub struct WriteCoordinator {
    segment_store: Arc<dyn SegmentStore>,
    metadata: Arc<dyn MetadataStore>,
    ring: Arc<RingCache>,
    encoder: Arc<dyn Encoder>,
}
```

No crate hard-codes the concrete implementation of its dependency.

---

## 6. File Organization (Workspace)

```
ocean-fs/
├── Cargo.toml              # workspace root [workspace.members]
├── Cargo.lock
├── rust-toolchain.toml     # pinned nightly or stable
├── clippy.toml             # workspace-level clippy config
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── coverage.yml
│       └── benchmarks.yml
├── docs/
│   ├── spec.md
│   ├── adr/
│   │   ├── 0000-template.md
│   │   └── 0001-segment-packing.md
│   └── diagrams/           # architecture diagrams (source + rendered)
├── guidelines/
│   ├── performance.md
│   ├── architecture.md     # this file
│   └── coding.md
├── proto/
│   └── oceanfs/
│       ├── common.proto
│       ├── segment.proto
│       └── ...             # shared proto definitions used by multiple crates
├── crates/
│   ├── oceanfs-core/
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── config.rs
│   │   │   ├── error.rs
│   │   │   ├── hlc.rs
│   │   │   └── types.rs
│   │   └── proto/
│   │       ├── common.proto
│   │       ├── segment.proto
│   │       └── membership.proto
│   ├── oceanfs-hash/
│   ├── oceanfs-ec/
│   ├── oceanfs-accel/
│   ├── oceanfs-storage/
│   ├── oceanfs-routing/
│   ├── oceanfs-membership/
│   ├── oceanfs-network/
│   ├── oceanfs-cache/
│   ├── oceanfs-server/
│   │   └── proto/
│   │       └── storage.proto   # service defs only
│   ├── oceanfs-node/
│   └── oceanfs/
│       └── src/
│           └── main.rs
├── benches/
│   ├── ec_benchmark.rs
│   ├── hash_benchmark.rs
│   └── storage_benchmark.rs
└── scripts/
    ├── pgo.sh
    └── ci-checks.sh
```

---

## 7. Versioning & Compatibility

### 7.1 Crate Versioning

All crates in the workspace share the same version number (set in the
workspace `Cargo.toml`). Individual crate `version` fields reference
`version.workspace = true`.

**Rationale:** The crates evolve together. A breaking change in
`oceanfs-storage` is a breaking change for `oceanfs-server`. Shared
versioning reflects this.

### 7.2 Unsafe Code Policy

Unsafe code is permitted only in the following crates:
- `oceanfs-accel` (GPU FFI, SIMD intrinsics)
- `oceanfs-hash` (BLAKE3 implementation if not using the upstream crate)
- `oceanfs-ec` (SIMD-accelerated GF arithmetic)

All other crates are `#![forbid(unsafe_code)]`.

**Enforcement:** CI checks each crate's `lib.rs` for
`#![forbid(unsafe_code)]` (or the absence of it, for the three
permitted crates).

### 7.3 Panic Policy

- Libraries (`oceanfs-core` through `oceanfs-node`): **never panic**.
  All fallible operations return `Result`. Unwrap/expect are permitted
  only when the invariant can be proven from the surrounding code.

- Binary (`oceanfs`): may panic on irrecoverable startup errors
  (missing config, port already bound).

**Enforcement:** Clippy lint `clippy::unwrap_used` and
`clippy::expect_used` denied at workspace level. Individual allows
require `#[allow(...)]` with a `// SAFETY:`-style justification
comment proving the invariant.
