---
feature: "DHT Ring & Consistent Hashing"
epic: "phase-2-distributed-connectivity"
status: done
priority: critical
owner: ""
dependencies:
  - epic: phase-0-project-scaffold
    reason: Requires oceanfs-core types (NodeId, RingConfig) and crate layout
  - epic: phase-1-storage-engine
    reason: Ring routes blob keys to nodes that own segment shards
adr: []
perf:
  - "2.4: ArcSwap for read-mostly shared data"
  - "6.5: BTreeMap over HashMap for ordered access"
  - "7.2: RwLock when reads ≥ 10× writes"
created: 2026-07-30
updated: 2026-08-02
---

# DHT Ring & Consistent Hashing

## Summary

Implement the 256-bit consistent hashing ring in `oceanfs-routing`. The ring maps
blob keys (SHA-256) to node replica sets via virtual nodes (`vnodes_per_node`).
A `RingCache` wraps the topology with `ArcSwap` for wait-free reads. Routing
operations are O(log N) binary search over sorted vnode positions, returning
the N successors (replication factor) for any key.

## Scope

### In Scope
- `Ring` struct: owns the ring topology — sorted list of `(vnode_position, node_id)` entries
- Consistent hashing: hash key (SHA-256) → binary search → find N successors
- Virtual nodes: each physical node owns `vnodes_per_node` (default 256) positions
- `RingCache`: `ArcSwap<Arc<Ring>>` — wait-free reads, atomic swap on topology change
- Ring operations: `lookup(key: &[u8]) -> Vec<NodeId>` (N successors), `add_node(node_id)`, `remove_node(node_id)`
- Ring rebalancing: on add/remove, compute affected key ranges for data migration
- Ring serialization/deserialization for gossip exchange
- Unit tests for successor correctness, vnode distribution uniformity, add/remove rebalance

### Out of Scope
- Membership integration (gossip triggers ring update — separate feature)
- Data migration orchestration (Phase 4) — ring only identifies what moved
- Multi-region or latency-aware routing

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `NodeId`, `RingConfig`, `VnodePosition` (alias `[u8; 32]`) |
| `oceanfs-routing` | New crate; modules: `ring.rs`, `ring_cache.rs`, `hash.rs` |
| `oceanfs-routing` | Facade exports: `pub use ring::Ring`, `pub use ring_cache::RingCache` |

## Interface (Public API)

- `pub struct RingConfig` — `vnodes_per_node: u32`, `replication_factor: u8`
- `pub struct Ring` — `pub fn new(config: RingConfig) -> Self`, `pub fn lookup(&self, key_hash: &[u8; 32]) -> Vec<NodeId>`, `pub fn add_node(&mut self, node: NodeId) -> Vec<VnodeRange>`, `pub fn remove_node(&mut self, node: NodeId) -> Vec<VnodeRange>`, `pub fn node_count(&self) -> usize`
- `pub struct RingCache` — `pub fn new(ring: Ring) -> Self`, `pub fn lookup(&self, key_hash: &[u8; 32]) -> Vec<NodeId>`, `pub fn update(&self, ring: Ring)`, `pub fn snapshot(&self) -> Arc<Ring>`
- `pub struct VnodeRange` — `start: [u8; 32], end: [u8; 32]` — affected key range for data migration
- `pub fn hash_key(key: &[u8]) -> [u8; 32]` — SHA-256 hash of blob key

## Data Flow

```
Key routing:
  Object key "photos/cat.jpg"
    → SHA-256 → 32-byte hash
      → RingCache::lookup(hash)
        → binary search in sorted vnode positions
          → find immediate successor vnode
            → walk ring forward N steps (replication_factor)
              → return [node_a, node_b, node_c] (replica set)

Ring update (triggered by gossip):
  Membership detects node added/removed
    → Ring::add_node(new_node) or Ring::remove_node(dead_node)
      → recompute vnode assignments
        → RingCache::update(new_ring) → ArcSwap::store(Arc::new(ring))
          → all readers see new topology on next lookup (atomic, wait-free)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-routing`
- [x] **Tests:** Unit tests (16): lookup returns N distinct nodes, lookup determinism, add_node increases vnode count, remove_node rebalances without gaps, ring serialization round-trip, RingCache concurrent read/write. All pass.
- [x] **Docs:** `#![deny(missing_docs)]` passes; `Ring` and `RingCache` documented with usage examples. `RUSTDOCFLAGS="-D warnings" cargo doc` passes.
- [x] **ADR:** N/A (ADR-0002 forthcoming; consistent hashing rationale documented in spec §2.2)
- [x] **Perf:** Rule 2.4 (ArcSwap for ring topology), 6.5 (BTreeMap for sorted vnodes in ring.rs:42), 7.2 (ring reads dominate writes). All verified.
- [x] **Integration:** `tests/ring_lifecycle.rs`: 5 tests — 3-node distribution uniformity, add/remove rebalance, RingCache snapshot, serialization round-trip. All pass.
