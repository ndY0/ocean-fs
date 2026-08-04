# ADR-0005: Trait-in-Consuming-Crate Pattern

**Status:** Accepted
**Date:** 2026-07-30
**Deciders:** architecture team

---

## Context

OceanFS is a workspace of 12 crates arranged in a DAG. The architecture
guideline §1.3 forbids circular dependencies. Without careful trait
placement, a natural circular dependency emerges: the server crate needs
to coordinate writes through a segment store, but the storage crate
provides the concrete implementation of that segment store. If the
`SegmentStore` trait lives in `oceanfs-storage`, then `oceanfs-server`
must depend on `oceanfs-storage` — creating a dependency edge from the
high-level orchestrator to the low-level persistence engine. This edge
makes the system harder to test (server test must link RocksDB) and
blurs the boundary between coordination logic and storage mechanics.

The standard Rust solution is dependency inversion: define the trait in
the consuming crate and provide the implementation via dependency
injection at the composition root (`oceanfs-node`).

## Decision

**Traits are defined in the crate that consumes them**, not the crate
that provides the implementation.

Concretely:

| Trait | Consumed By | Defined In | Implemented By |
|---|---|---|---|
| `SegmentStore` | `oceanfs-server` | `oceanfs-server` | `oceanfs-storage` |
| `MetadataStore` | `oceanfs-server`, `oceanfs-cache`, `oceanfs-node` | `oceanfs-core` (cross-cutting exception) | `oceanfs-storage` |
| `WalWriter` | `oceanfs-server` (eventually) | `oceanfs-server` | `oceanfs-storage` |
| `Encoder` / `Decoder` | `oceanfs-accel`, `oceanfs-server` | `oceanfs-ec` (exception, see below) | `oceanfs-ec`, `oceanfs-accel` backends |
| `RingCache` | `oceanfs-server` | `oceanfs-server` | `oceanfs-routing` |
| `FailureDetector` | `oceanfs-membership` | `oceanfs-membership` | `oceanfs-membership` (self) |
| `ObjectCache` | `oceanfs-server` | `oceanfs-server` | `oceanfs-cache` |
| `BucketPolicy` | `oceanfs-server` | `oceanfs-core` | `oceanfs-server` (config types in core) |

**Exception — cross-cutting domain traits:** Traits that define a
fundamental domain concept consumed by many crates may live in
`oceanfs-core`. The canonical example is `Encoder` / `Decoder` from
`oceanfs-ec`: these are consumed by `oceanfs-storage` (for segment
encoding), `oceanfs-accel` (for backend implementations), and
`oceanfs-server` (for read repair). Placing them in `oceanfs-ec` (which
only depends on `oceanfs-core`) avoids a diamond dependency problem.

The test for whether a trait qualifies for the exception: **is it
consumed by 3+ crates in different branches of the DAG?** If yes,
`oceanfs-core` or `oceanfs-ec` is the appropriate home. If consumed
by 1-2 crates, the consuming-crate rule applies.

**Concrete example — `MetadataStore`:** This trait is consumed by
`oceanfs-server` (coordinators), `oceanfs-cache` (negative-cache
validation, prefetch), and `oceanfs-node` (composition root wiring).
Three consumers in different DAG branches → qualifies for the
cross-cutting exception → stays in `oceanfs-core`. The concrete
RocksDB-backed struct is named `RocksDbMetadataStore` in
`oceanfs-storage` to distinguish it from the trait.

**Dependency injection wiring** happens exclusively in `oceanfs-node`
(architecture guideline §4.1). `oceanfs-node` is the composition root:
it constructs concrete implementations and injects them as `Arc<dyn
Trait>` into the consuming crate's types.

```rust
// oceanfs-node/src/node.rs
let metadata: Arc<dyn MetadataStore> = Arc::new(
    RocksDbMetadataStore::open(&config.data_dir)?
);
let segment_store: Arc<dyn SegmentStore> = Arc::new(
    SegmentStoreImpl::new(&config, metadata.clone())
);
let server = Server::new(segment_store, metadata, ring, ...);
```

**No crate hard-codes the concrete implementation of its dependency.**
All crate constructors accept `Arc<dyn Trait>` (or equivalent). This
enables testing with mock implementations without compiling RocksDB,
CUDA, or other heavy dependencies.

## Consequences

### Positive
- **No circular dependencies.** The DAG constraint (§1.3) is
  mechanically satisfiable — traits flow downward, implementations
  flow upward through the composition root.
- **Testability.** `oceanfs-server` tests can inject mock segment
  stores, metadata stores, and ring caches. No RocksDB compilation
  required for server unit tests.
- **Compile-time isolation.** Changing a storage implementation detail
  does not recompile the server crate (only `oceanfs-node` and
  `oceanfs-storage`).
- **Clear architectural intent.** Reading `oceanfs-server/src/lib.rs`
  tells you exactly what services the server *needs* (the trait
  definitions), not how they are implemented.

### Negative
- **Trait duplication risk.** If two crates both need a `SegmentStore`
  trait with slightly different method signatures, they cannot share
  one definition. Mitigation: the exception rule for cross-cutting
  traits handles this — if a trait is genuinely needed by multiple
  crates, it graduates to `core` or `ec`.
- **Boilerplate at the composition root.** `oceanfs-node` must wire
  every dependency explicitly. This is intentional — it makes the
  system's dependencies visible in one place — but it means adding a
  new service requires touching the composition root.
- **Type mapping overhead.** The consuming crate must re-export or
  reference types from `oceanfs-core` that appear in trait signatures
  (e.g., `SegmentId`, `SegmentMetadata`). This is handled by the
  facade re-export pattern (§2.2).

### Neutral
- **ADR volume.** Each crate-boundary trait might warrant its own ADR
  if the design space is contested. This ADR covers the general
  pattern; individual trait decisions can reference it.
- **Learning curve.** New contributors must understand dependency
  inversion — the "why is this trait in server instead of storage?"
  question has a consistent answer: follow this ADR.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Traits in implementation crate** (e.g., `SegmentStore` in `oceanfs-storage`) | Simpler initial wiring; traits and impls colocated | Creates `server → storage` dependency; forces RocksDB into server tests; violates DAG hygiene | The whole point of the DAG is to prevent high-level crates from depending on low-level ones. Colocated traits defeat the DAG. |
| **Traits in `oceanfs-core` for everything** | Single home for all traits; no "which crate?" confusion | `core` becomes a dumping ground; traits for gRPC services, cache layers, and S3 handlers would live alongside `SegmentId` and `Hlc` | Violates single-responsibility. `core` should hold data types, not service contracts. The exception rule covers genuinely cross-cutting traits. |
| **Separate `oceanfs-traits` crate** | Clean separation of contracts from data types and implementations | Adds a 13th crate; all 12 existing crates would depend on it, creating a hub-and-spoke rather than a DAG; increases compilation graph size | A traits crate is essentially `core` by another name if it sits at the root of the DAG. The exception rule (place in `core` when genuinely cross-cutting) gives the same benefit without adding a crate. |

## References

- Architecture guideline §2.1 ("Traits in the Consuming Crate")
- Architecture guideline §4.1 ("Construction Happens in `oceanfs-node`")
- Architecture guideline §5.4 ("Dependency Injection for Testing")
- Audit finding M4: `MetadataStore` trait in `oceanfs-core` was evaluated against the
  cross-cutting exception test and qualifies (consumed by server + cache + node = 3 crates).
  It remains in `oceanfs-core`. The concrete struct is renamed to `RocksDbMetadataStore`.
- Audit finding H1: `oceanfs-server` optional dependency on `oceanfs-storage` is legitimate
  for type re-exports and concrete type construction. Architecture §4.1 prohibition revoked.
  See Feature `resolve-server-storage-dep`.
