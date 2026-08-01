---
feature: "HLC Versioning & Conflict Resolution"
epic: "phase-4-distributed-read-write"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: write-coordinator-quorum
    reason: Every write is timestamped with HLC
  - feature: read-coordinator-parallel
    reason: Reads compare HLC values across replicas for consistency
adr: []
perf:
  - "6.1: Cache-line alignment for mutable atomics"
created: 2026-07-30
updated: 2026-07-30
---

# HLC Versioning & Conflict Resolution

## Summary

Implement Hybrid Logical Clock (HLC) versioning and Last-Write-Wins (LWW)
conflict resolution in `oceanfs-core`. Every object write is timestamped with an
HLC that combines a physical wall clock component with a logical counter to
provide causally-consistent total ordering without reliance on synchronized
clocks. The LWW resolver compares HLC values; a pluggable `ConflictResolver`
trait allows per-bucket custom conflict resolution.

## Scope

### In Scope
- `Hlc` type: `(wall_time: u64, logical: u32)` — 96-bit hybrid timestamp
- HLC update rules: on local event (increment logical, update wall if needed); on receive (max wall, increment logical)
- `HlcClock`: thread-safe HLC generator with `AtomicU64` for wall time (cache-line aligned)
- `LwwResolver`: default conflict resolver — newer HLC wins; tie-break by node_id
- `trait ConflictResolver`: pluggable interface for per-bucket resolution strategies
- `VersionVector`: for concurrent multi-writer scenarios (reserved, not implemented here)
- Integration: write coordinator stamps every write; read coordinator compares replicas
- `#[repr(align(64))]` on `HlcClock` to prevent false sharing
- Unit tests for HLC monotonicity, receive-merging, tie-breaking

### Out of Scope
- Full CRDT or multi-version concurrency (future work)
- Vector clock implementation (reserved for later)
- S3-style object versioning (future work — spec §16)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New modules: `hlc.rs`, `conflict.rs` |
| `oceanfs-core` | Facade exports: `pub use hlc::Hlc`, `pub use hlc::HlcClock`, `pub use conflict::ConflictResolver` |

## Interface (Public API)

- `pub struct Hlc` — `pub fn new(wall_time: u64, logical: u32) -> Self`, `pub fn wall_time(&self) -> u64`, `pub fn logical(&self) -> u32`, `impl Ord for Hlc` (total order)
- `pub struct HlcClock` — `pub fn new() -> Self`, `pub fn now(&self) -> Hlc`, `pub fn update(&self, received: Hlc) -> Hlc`
- `pub trait ConflictResolver: Send + Sync` — `fn resolve(&self, local: &Hlc, remote: &Hlc) -> Resolution`
- `pub enum Resolution` — `AcceptLocal`, `AcceptRemote`, `Merge` (reserved)
- `pub struct LwwResolver` — default implementation: newer HLC wins

## Data Flow

```
Write timestamping:
  WriteCoordinator::put(req):
    hlc = hlc_clock.now()  → (wall: 1690000000000, logical: 0)
    object_metadata.hlc = hlc
    // Replicate with HLC to replicas

HLC update on receive:
  Node B receives write from Node A with hlc=(1690000000000, 3):
    local_clock.update(received_hlc):
      wall = max(local_wall, received.wall) = 1690000000000
      if received.wall > local_wall: logical = received.logical + 1
      else: logical = max(local_logical, received.logical) + 1
      → new HLC = (1690000000000, 4)

Conflict resolution on read (R > 1):
  ReadCoordinator queries 3 replicas:
    replica_a: hlc=(1690000000000, 3)
    replica_b: hlc=(1690000000000, 3)  // same
    replica_c: hlc=(1690000000001, 1)  // newer wall time
  LwwResolver::resolve:
    replica_c has newer HLC → accept replica_c's data
    (async) push corrected data to replica_a and replica_b
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core`
- [x] **Tests:** Unit tests: HLC monotonic (now() > previous now()), HLC ordering (newer wall > older, same wall → higher logical > lower), update merges correctly, clock does not go backward, concurrent updates (stress test), LwwResolver picks newer, tie-break by node_id
<!-- REVIEW: R3 — 17 HLC unit tests pass (hlc.rs: 17 tests), 4 conflict resolution tests pass (conflict.rs: 6 tests). All spec-required scenarios covered: monotonicity, ordering, merge, clock backward prevention, concurrent stress, LwwResolver newer/older/equal HLCs, cache-line alignment. Integration test hlc_ordering.rs: 5 tests pass (monotonic, merge, deterministic, equal-hlc, cache-line-alignment). ✅ All independent tests pass. -->
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-core`
<!-- REVIEW: R3 — oceanfs-core tarpaulin reports 34.21% (143/418 lines, +1.36% from R2). hlc.rs 37/46 (80.4% ✅), conflict.rs 9/9 (100% ✅), timeouts.rs 1/1 (100% ✅), types.rs 47/98 (48.0%), config.rs 11/16 (68.8%), error.rs 2/4 (50%). Still below 80% threshold. The +17 unit tests added only +1.36% coverage; types.rs (98 lines) is the main bottleneck. hlc.rs logical overflow path (lines 139-148) still not covered. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `Hlc` and `ConflictResolver` documented
- [x] **ADR:** N/A (spec §7.6 covers versioning)
- [x] **Perf:** Rule 6.1 (cache-line aligned HlcClock to prevent false sharing)
- [x] **Integration:** `tests/hlc_ordering.rs`: multi-node scenario: node A writes, node B writes concurrently, verify HLC ordering yields deterministic LWW outcome
<!-- REVIEW: R2 — Integration test exists at crates/oceanfs-core/tests/hlc_ordering.rs with 5 tests (monotonic, merge, deterministic, equal-hlc, cache-line-alignment). All 5 pass. ✅ -->
- [x] **Manual:** Example in `HlcClock` docs compiles and runs
