---
feature: "f1: PeerSelector / PartitionPlanner Trait + Node Manifest Implementations"
epic: "refactoring/manifest-aware-repair"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: The node-side constructors are wired where c2 (durability builder) and c3 (server builder) take over AntiEntropy/ScrubCoordinator/ScrubGrpcService construction from node.rs §7/§15
  - epic: refactoring/store-unification
    reason: The node-side selector is a peer (membership + manifest) concern, but the AE/scrub consumers read holder sets from the lifecycle registry Sealed entries, which wave 2 ② makes consistent with a single shared data store
adr:
  - 0033-manifest-aware-peer-selection
  - 0015-anti-entropy-merkle-protocol
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: PeerSelector / PartitionPlanner Trait + Node Manifest Implementations

## Summary

Defines the injection seam ADR-0033 D3 mandates: a `PeerSelector` trait
(which of a segment's `storage_locations` holders may act as a comparison
peer) and a `PartitionPlanner` trait (which holder scrubs each segment) in
`oceanfs-durability`, mirroring the shape of the existing
`RepairTargetSelector` (`crates/oceanfs-durability/src/repair.rs:110`).
`oceanfs-node` supplies the concrete implementations —
`ManifestPeerSelector` and `ManifestPartitionPlanner` — which filter the
holder set against membership state and the gossiped `NodeManifest`,
exactly like g5's `ManifestRepairTargetSelector`
(`crates/oceanfs-node/src/repair.rs:73`) and reusing g6's manifest
predicates (`crates/oceanfs-node/src/routing_cache.rs`). The `SegmentPartition`
record (`scrub.rs:276`) becomes `pub` so the trait surface can return it.

The durable engine keeps zero manifest logic; the node wires the concrete
selector once (composition root c2), and AE (`f2`) and scrub (`f3`)
consume the same trait object. This is the ADR-0005/ADR-0009 "trait in
consuming crate, impl in node" pattern already used for
`RepairTargetSelector`.

## Scope

### In Scope
- New module `oceanfs-durability/src/peer_selection.rs` (crate-root,
  beside `repair.rs`) defining `PeerSelector` and `PartitionPlanner` with
  rustdoc examples; re-exported from `lib.rs`.
- Promote `SegmentPartition` (`scrub.rs:276`) from `pub(crate)` to `pub`
  (fields stay as-is) and re-export it from `lib.rs` so the
  `PartitionPlanner` trait can return it and the node impl can construct
  it. (`#[doc(hidden)]` stays.)
- New module `oceanfs-node/src/peer_selection.rs` with
  `ManifestPeerSelector` (implements `PeerSelector`) and
  `ManifestPartitionPlanner` (implements `PartitionPlanner`), both built
  over `Arc<Membership>` + `self_id` (`ManifestRepairTargetSelector::new`
  shape). Add `pub mod peer_selection;` to `oceanfs-node/src/lib.rs`.
- Shared eligibility predicate helper in the node module (mirrors g6's
  `healthy_data_pools` filter used by `routing_cache.rs`): a holder is
  eligible when membership state is `Alive | Suspect` and, when a
  manifest is known, the manifest reports at least one Healthy data pool;
  a missing/stale manifest stays eligible (ADR-0029 §D5 — the I/O error
  path is the truth).
- Unit tests in both crates covering the pub trait surface, self/Dead
  exclusion, manifest-healthy filtering, missing-manifest eligibility, and
  deterministic planning.

### Out of Scope
- Any change to `AntiEntropy` cycles (f2) or the scrub coordinator/gRPC
  wiring (f3) — f1 only lands the trait, the node impls, and the
  `SegmentPartition` visibility change.
- Changing `RepairTargetSelector` / `ManifestRepairTargetSelector`
  (g5) — those stay as-is.
- No Merkle/protocol/config changes.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New module `src/peer_selection.rs`; `lib.rs` adds `pub mod peer_selection;` + re-exports `PeerSelector`, `PartitionPlanner`, `SegmentPartition`; `scrub.rs` changes `pub(crate) struct SegmentPartition` → `pub struct SegmentPartition` |
| `oceanfs-node` | New module `src/peer_selection.rs` with `ManifestPeerSelector`, `ManifestPartitionPlanner`; `lib.rs` adds `pub mod peer_selection;` |

No new crate dependencies: `oceanfs-durability` already depends on
`oceanfs-membership` (for `Membership::nodes_full`, the accessor that
carries the manifest); the manifest *interpretation* stays in
`oceanfs-node`, matching how `ManifestRepairTargetSelector` sits in
`oceanfs-node/src/repair.rs` today.

## Interface (Public API)

`oceanfs-durability/src/peer_selection.rs`:

```rust
/// Selects which of a segment's `storage_locations` holders may act as a
/// Merkle-comparison peer (ADR-0033 D1/D3).
///
/// Injected from the node layer. The durable crate never interprets
/// manifests; the node implementation filters holders against membership
/// state + the gossiped NodeManifest (g6 predicate).
pub trait PeerSelector: Send + Sync {
    /// Returns the subset of `holders` (a segment's `storage_locations`)
    /// eligible as comparison peers: not this node, membership
    /// `Alive | Suspect`, and — when a manifest is known — reporting at
    /// least one Healthy data pool. A holder whose manifest is
    /// missing/stale stays eligible (ADR-0029 §D5).
    fn eligible_holders(&self, segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId>;
}

/// Builds the per-node scrub work assignment (ADR-0033 D1 scrub half).
///
/// Injected from the node layer alongside `PeerSelector`. The concrete
/// planner assigns each segment to one of its eligible holders and never
/// assigns a segment to a node that does not hold it.
pub trait PartitionPlanner: Send + Sync {
    /// Plans a scrub assignment over the coordinator's sealed-segment
    /// inventory (`segments`). Each returned [`SegmentPartition`] lists,
    /// for its `node_id`, only segments that node holds; a segment whose
    /// only eligible holder is `self_id` lands in the self partition.
    fn plan_partitions(
        &self,
        segments: &[SegmentMetadata],
        self_id: &NodeId,
    ) -> Vec<SegmentPartition>;
}
```

`oceanfs-node/src/peer_selection.rs` (both implement the traits above):

```rust
pub struct ManifestPeerSelector { /* Arc<Membership>, self_id: NodeId */ }
impl ManifestPeerSelector { pub fn new(membership: Arc<Membership>, self_id: NodeId) -> Self }

pub struct ManifestPartitionPlanner { /* Arc<Membership>, self_id: NodeId */ }
impl ManifestPartitionPlanner { pub fn new(membership: Arc<Membership>, self_id: NodeId) -> Self }
```

`SegmentPartition` (promoted, `scrub.rs:276`):
- `pub struct SegmentPartition { pub node_id: NodeId, pub segment_ids: Vec<SegmentId> }`

## Data Flow

```
node.rs (composition root, c2/c3)
  → ManifestPeerSelector::new(membership.clone(), self_id)      // node-layer impl
  → ManifestPartitionPlanner::new(membership.clone(), self_id)
  → Arc<dyn PeerSelector>       → injected into AntiEntropy::new (f2)
  → Arc<dyn PartitionPlanner>   → injected into ScrubCoordinator (f3)

run-time call:
  AE/scrub engine (durability) calls selector.eligible_holders(seg, storage_locations)
    → ManifestPeerSelector iterates membership.nodes_full() once
    → filters: holder ≠ self ∧ state ∈ {Alive, Suspect}
                ∧ (manifest.is_none() ∨ manifest has ≥1 Healthy data pool)
    → Vec<NodeId> of eligible partners (never invented — subset of holders)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds for
      `oceanfs-durability` and `oceanfs-node`.
- [ ] **API:** `PeerSelector` + `PartitionPlanner` exported from
      `oceanfs-durability`; `ManifestPeerSelector` +
      `ManifestPartitionPlanner` exported from `oceanfs-node`;
      `SegmentPartition` is `pub` with `pub` fields.
- [ ] **Eligibility tests:** a holder that is Dead, or whose manifest
      reports zero Healthy data pools (or `write_degraded`), is excluded;
      self is excluded; a holder with no manifest remains eligible.
      Mirror the fixture style of
      `oceanfs-node/src/repair.rs::manifest_selector_excludes_degraded_nodes`
      (`membership.upsert_node` + `membership.set_peer_manifest`,
      `oceanfs-membership/src/membership/manager.rs:843`).
- [ ] **Planner tests:** for a segment list with mixed
      `storage_locations`, every returned partition's `segment_ids` are
      all ∈ that node's `storage_locations`; a local-only segment
      (`storage_locations == {self}`) lands in the self partition; no
      partition lists a non-holder.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` pass
      (RocksDB caveat, PIPELINE.md §4.6).
- [ ] **Docs:** every `pub` item in both new modules has `# Examples`;
      `#![deny(missing_docs)]` passes.
- [ ] **ADR:** ADR-0033 D3 satisfied (trait in durability, impl in node,
      no new manifest dependency in `oceanfs-durability`); ADR-0029 §D5
      hint discipline preserved (missing manifest ≠ excluded).
- [ ] **Integration:** a node-crate unit test constructs
      `ManifestPeerSelector` over a membership with two holders (one
      healthy, one Dead) and asserts only the healthy holder is returned.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (see
> `guidelines/coding.md` §9.2).
