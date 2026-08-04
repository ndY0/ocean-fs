# ADR-0009: Storage Crate Split — `oceanfs-durability` and `oceanfs-storage-api`

**Status:** Proposed
**Date:** 2026-08-04
**Deciders:** OceanFS design team

---

## Context

`oceanfs-storage` has grown to ~12.7K lines across 9 top-level modules and 4
subdirectory module groups. The architecture §1.2 crate-responsibility table
promises `SegmentStore`, `MetadataStore`, `WalWriter`, `SegmentHandle`,
`SegmentIndex`, and `BufferPool` as its public API. The actual crate also
contains durability background tasks — `anti_entropy/` (Merkle tree exchange,
~2.6K lines after Epic 2 split), `gc/` (garbage collection, ~2.1K lines),
`heal/` (shard repair, 3 files), and `scrub.rs` (data integrity scanning) —
none of which are documented in the crate's intended scope.

The structural audit (finding H2, 2026-08-03) rated this as high severity: the
crate mixes low-level storage primitives (buffer pool, WAL, segment lifecycle)
with high-level maintenance orchestration (anti-entropy, scrubbing, GC,
healing). This violates single-responsibility and makes the crate harder to
reason about, test in isolation, and keep under a size ceiling.

Simultaneously, the `MetadataStore` and `SegmentStore` traits — currently split
between `oceanfs-core` (MetadataStore, grandfathered as cross-cutting) and
`oceanfs-server` (SegmentStore, per ADR-0005 consuming-crate rule) — lack a
clean home. With `oceanfs-durability` entering the picture, both server and
durability consume these traits, creating a "two consumers, no natural home"
problem. Furthermore, the project has a multi-backend storage roadmap: future
implementations (FUSE-backed, S3-backed, memory-only for testing) will all need
to implement the same storage contracts. A crate boundary that cleanly
separates interface from implementation is an architectural asset for this
future.

## Decision

### Part 1: Create `oceanfs-durability` crate

The durability background tasks are extracted from `oceanfs-storage` into a new
`oceanfs-durability` crate:

| Moves to `oceanfs-durability` | Stays in `oceanfs-storage` |
|---|---|
| `anti_entropy/` (config, engine, merkle_tree, merkle_root, merkle_proof) | `buffer_pool.rs` |
| `gc/` (config, stats, liveness_tracker, segment_compactor, garbage_collector, orphan_reaper) | `segment/` (buffer, handle, header, index, pool, sealer, shard, splitter, tier, route_write) |
| `heal/` (mod, queue, worker) | `wal/` (entry, reader, sync, writer) |
| `scrub.rs` | `metadata/` (cf, store) |
| | `blob_store.rs` |
| | `error.rs` |

Estimated sizes: `oceanfs-storage` ~7K lines, `oceanfs-durability` ~5.6K lines.

### Part 2: Create `oceanfs-storage-api` crate

A new `oceanfs-storage-api` crate is introduced to hold storage interface
contracts. This crate depends only on `oceanfs-core` and defines the traits
that any storage backend implements:

| Trait | Current Location | New Location |
|---|---|---|
| `SegmentStore` | `oceanfs-server` | `oceanfs-storage-api` |
| `MetadataStore` | `oceanfs-core` | `oceanfs-storage-api` |
| `BlobStore` | (not yet extracted) | `oceanfs-storage-api` |
| `WalWriter` | (not yet defined as trait) | `oceanfs-storage-api` |

### Dependency Graph (After Split)

```
oceanfs-core
    ↓
oceanfs-storage-api (NEW: SegmentStore, MetadataStore, BlobStore, WalWriter traits)
    ↓                    ↓                    ↓
oceanfs-storage     oceanfs-server     oceanfs-durability
(RocksDB impls)     (consumes traits)  (consumes traits, reads/verifies/repairs)
    ↑                                       ↑
    └────────── oceanfs-durability ─────────┘
              (reads segments via storage API)
```

`oceanfs-node` (composition root) depends on all three: it constructs concrete
storage implementations, wires them into server and durability via `Arc<dyn
Trait>`, and spawns durability background tasks.

### Protobuf Service Stubs

Healing and scrub gRPC service stubs (`healing_service.rs`, `scrub_service.rs`)
move from `oceanfs-server/src/grpc/` to `oceanfs-durability/src/`. The
`oceanfs-server` gRPC module retains `cache_service.rs` and
`segment_service.rs`. This aligns with architecture §2.4 (service stubs in the
implementing crate) and Epic 4 (protobuf reorganization).

### Rejection of Alternative: Move durability to `oceanfs-node`

Proposal B (move durability tasks into `oceanfs-node`) was considered and
rejected. The node crate is the composition root (§4.1) — it wires
dependencies, it does not implement business logic. Injecting 5.6K lines of
durability logic into the composition root would blur the line between wiring
and implementation, make the node crate excessively large, and create a
dependency where the composition root owns domain logic that other crates
(server) need to trigger (heal, scrub via gRPC).

## Consequences

### Positive

- **Single-responsibility.** `oceanfs-storage` is now a pure storage engine
  (buffer, segment lifecycle, WAL, metadata persistence). `oceanfs-durability`
  is purely maintenance (anti-entropy, GC, heal, scrub). Each can be tested,
  versioned, and understood independently.
- **Multi-backend readiness.** `oceanfs-storage-api` separates interface from
  implementation. A future `oceanfs-storage-fuse` crate implements the same
  traits with no dependency on RocksDB. Test mocks implement the traits without
  linking any storage engine.
- **Compile-time isolation.** Changing a GC heuristic does not recompile the
  segment engine. Changing the WAL format does not recompile durability tasks.
- **Clean ADR-0005 compliance.** Both server and durability consume the
  `SegmentStore` trait from `oceanfs-storage-api`. No duplication, no awkward
  trait-in-consumer gymnastics for two consumers in different DAG branches.
- **gRPC service ownership.** Healing and scrub services are owned by
  `oceanfs-durability` — the crate that implements them. Architecture §2.4 is
  satisfied.

### Negative

- **Two new crates.** The workspace grows from 12 to 14 crates (adds
  `oceanfs-storage-api` and `oceanfs-durability`). Each new crate adds
  Cargo.toml, lib.rs, CI configuration overhead.
- **Migration risk.** Moving traits (`SegmentStore` from server,
  `MetadataStore` from core) and code (4 durability modules) is a large
  refactor touching `oceanfs-storage`, `oceanfs-server`, `oceanfs-node`,
  integration tests, and benchmark code. The blast radius is across ~6 crates.
- **Composition root complexity.** `oceanfs-node` now constructs and wires 14
  crates instead of 12. The `Node::start` method gains additional construction
  calls.
- **Trait evolution friction.** Adding a method to `SegmentStore` now requires
  updating the trait in `oceanfs-storage-api`, the implementation in
  `oceanfs-storage`, and all consumers. This is the standard cost of an
  interface crate — acceptable given the multi-backend roadmap.

### Neutral

- **Workspace compilation order.** The DAG depth increases by one hop
  (`server → storage-api → storage`) but this does not meaningfully affect
  compilation time because the API crate is header-only (traits with no
  implementations).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. Graduate traits to `oceanfs-core`** | Single trait home; no new crate | `core` becomes a dumping ground; every future storage backend must depend on all of `core` (Config, Hlc, protobuf types); violates the precedent set by `oceanfs-ec` which already has its own trait crate | Rejected: multi-backend roadmap makes a focused interface crate the better investment |
| **B. Trait in `oceanfs-durability`** | Clean ADR-0005 compliance for durability | `oceanfs-server` would depend on `oceanfs-durability` for storage traits — inverts the natural dependency; server shouldn't depend on durability infrastructure to get its storage contract | Rejected: dependency inversion is worse than adding an interface crate |
| **C. No split — keep everything in `oceanfs-storage`** | No migration risk; no new crates | Perpetuates the 12.7K-line mega-crate; mixes low-level storage primitives with high-level maintenance logic; makes multi-backend harder because durability tasks are coupled to RocksDB internals | Rejected: the structural audit correctly identified this as high-severity technical debt |
| **D. Move durability to `oceanfs-node`** | Simple: delete files from storage, add to node | Node crate exceeds 10K lines; composition root becomes a business-logic crate; server can't trigger heal/scrub through node without awkward indirection | Rejected: node is the composition root, not a business-logic crate (§4.1) |

## References

- [Structural Audit (2026-08-03), finding H2](../audits/2026-08-03-two-stage-structural-audit.md)
- [ADR-0005: Trait-in-Consuming-Crate Pattern](./0005-trait-in-consuming-crate.md)
- [Architecture Guideline §1.2: Crate Responsibilities](../guidelines/architecture.md#12-crate-responsibilities)
- [Architecture Guideline §2.4: Protobufs — Messages in Core, Services in Owners](../guidelines/architecture.md#24-protobufs-messages-in-core-services-in-owners)
- [Architecture Guideline §4.1: Construction Happens in `oceanfs-node`](../guidelines/architecture.md#41-construction-happens-in-oceanfs-node)
- [Feature: Evaluate Storage Crate Split](../features/refactoring/megacrate-split/evaluate-storage-split.md)
- [Epic 4: Protobuf Reorganization](../features/refactoring/structural-roadmap.md#epic-4-protobuf-reorganization-short-term--sprint-34)

---

## Appendix: Implementation Feature Brief

See [Feature: Execute Storage Crate Split](../features/refactoring/megacrate-split/execute-storage-split.md)
for the detailed implementation plan covering:
- Crate scaffolding (`oceanfs-durability`, `oceanfs-storage-api`)
- Module relocation (4 durability modules, 2 gRPC service stubs)
- Trait migration (`SegmentStore` from server, `MetadataStore` from core)
- Import updates across affected crates
- Integration test and benchmark updates
- CI configuration
