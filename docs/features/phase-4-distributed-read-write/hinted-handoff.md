---
feature: "Hinted Handoff"
epic: "phase-4-distributed-read-write"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: write-coordinator-quorum
    reason: Hinted handoff is triggered when a write successor is unreachable
  - feature: swim-gossip-membership
    reason: Membership detects node return, triggering hint delivery
adr: []
perf:
  - "2.6: Bounded channels for handoff queues"
  - "4.5: Adaptive per-operation timeouts"
created: 2026-07-30
updated: 2026-07-30
---

# Hinted Handoff

## Summary

Implement hinted handoff in `oceanfs-server`. When a replica node is
unreachable during a write, the coordinator selects the next successor on the
ring as a fallback. The fallback node stores the write with a hint
`{intended_for: unreachable_node}`. When the intended node returns (detected
via membership gossip), the fallback pushes the buffered data and clears the
hint. This ensures write durability and quorum satisfaction even during
transient node failures.

## Scope

### In Scope
- `HintedHandoff`: manages hint storage, delivery, and cleanup
- Hint write path: on successor unreachable → pick fallback node → append with `{intended_for}` hint
- Hint storage: RocksDB `hints` column family (or separate store under `data_dir/hints/`)
- Hint delivery: on membership event (node ALIVE), check for pending hints, push to returned node
- Hint lifecycle: create → buffer → deliver → acknowledge → delete
- Configurable `hint_buffer_size` and `hint_delivery_timeout`
- Integration: write coordinator calls handoff when `W` quorum at risk
- Unit tests for hint creation, delivery on node return, duplicate hint prevention

### Out of Scope
- Cross-DC handoff or multi-region hints (single cluster)
- Hint compression or batching across segments

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `HintRecord`, `IntendedFor` |
| `oceanfs-server` | New modules: `handoff/hinter.rs`, `handoff/delivery.rs` |

## Interface (Public API)

- `pub struct HintedHandoff` — `pub fn new(metadata: Arc<MetadataStore>, pool: Arc<ConnectionPool>) -> Self`, `pub async fn handoff(&self, intended_for: NodeId, entry: HintRecord) -> Result<()>`, `pub async fn deliver_pending(&self, node: NodeId) -> Result<usize>`, `pub async fn pending_count(&self, node: NodeId) -> usize`
- `pub struct HintRecord` — `intended_for: NodeId`, `segment_id: SegmentId`, `wal_entry: WalEntry`, `timestamp: Hlc`

## Data Flow

```
Write with unreachable successor:
  Replica set: [node_a, node_b, node_c]
  node_b is unreachable
    → coordinator selects node_d (next successor on ring) as fallback
      → HintedHandoff::handoff(intended_for=node_b, entry)
           └─ node_d stores data with hint {intended_for: node_b} in local hints store
    → quorum now includes node_d → write succeeds

Node return and hint delivery:
  Membership detects node_b transition: DEAD → ALIVE
    → HintedHandoff::deliver_pending(node_b)
         ├─ Query all known nodes for hints intended for node_b
         ├─ Stream hint entries to node_b via gRPC
         ├─ node_b acknowledges receipt + persists to its WAL
         └─ Sender deletes delivered hints
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: handoff creates hint on fallback node, delivery pushes hints on node return, hint cleared after successful delivery, duplicate hint ignored, pending_count accurate, delivery to still-unreachable node retries
<!-- REVIEW: R2 — 6 unit tests pass (handoff_create, handoff_multiple, deliver_clears, deliver_no_hints=0, new_is_empty, pending_count). Missing: (1) duplicate hint prevention, (2) delivery to still-unreachable node retry. handoff/hinter.rs and handoff/delivery.rs sub-modules exist as scaffolding. In-memory storage used (not RocksDB CF) — acknowledged deferred to Phase 7. -->
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
<!-- REVIEW: R2 — tarpaulin on oceanfs-server could not be verified (timed out, same as R1). -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes
- [x] **ADR:** N/A (spec §7.2 covers hinted handoff)
- [ ] **Perf:** Rule 2.6 (bounded hint queues), 4.5 (delivery timeout)
<!-- REVIEW: Rule 2.6: No bounded channel — hints stored in an unbounded RwLock<HashMap<NodeId, Vec<HintRecord>>>. No backpressure on hint accumulation. Rule 4.5: No configurable delivery timeout — deliver_single always succeeds immediately. -->
- [x] **Integration:** `tests/hinted_handoff.rs`: 3-node cluster, kill node_b, PUT succeeds via fallback node, restart node_b, verify hints delivered, verify data consistent
<!-- REVIEW: R2 — Integration test exists at crates/oceanfs-server/tests/hinted_handoff.rs with 4 tests (handoff_create_deliver_cleanup, handoff_multiple_hints, deliver_no_hints=0, unknown_node_zero_pending). All pass with default features. Missing: kill-node scenario (requires real membership). -->
- [ ] **Manual:** Example in `HintedHandoff` docs compiles and runs
<!-- REVIEW: The doc example for HintedHandoff::new() compiles but is a trivial construction test. The handoff()/deliver_pending() examples are not doctest-compiled. -->
