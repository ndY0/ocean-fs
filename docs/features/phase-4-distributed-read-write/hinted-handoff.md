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

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: handoff creates hint on fallback node, delivery pushes hints on node return, hint cleared after successful delivery, duplicate hint ignored, pending_count accurate, delivery to still-unreachable node retries
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** N/A (spec §7.2 covers hinted handoff)
- [ ] **Perf:** Rule 2.6 (bounded hint queues), 4.5 (delivery timeout)
- [ ] **Integration:** `tests/hinted_handoff.rs`: 3-node cluster, kill node_b, PUT succeeds via fallback node, restart node_b, verify hints delivered, verify data consistent
- [ ] **Manual:** Example in `HintedHandoff` docs compiles and runs
