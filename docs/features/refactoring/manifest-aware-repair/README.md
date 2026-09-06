---
feature: "Manifest-Aware Peer Selection for AE + Scrub (ADR-0033) — Program Coordination"
epic: "refactoring/manifest-aware-repair"
status: done
priority: high
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: Injection points land in the module builders — c2 (DurabilityModule constructs AntiEntropy/ScrubCoordinator, replacing node.rs §7) and c3 (ServerModule constructs the scrub gRPC service, replacing node.rs §15). c1 (single shared DiskSegmentStore in StorageModule) is the data-store precondition.
  - epic: refactoring/store-unification
    reason: ADR-0032 (wave 2 ②) gives AE + scrub one shared SegmentDataStore, so holder-set reads (registry Sealed entries + storage_locations) and the data-store reads a feature touches are consistent and single-writer.
  - epic: refactoring/durability-scheduler
    reason: "NOT a hard scheduling gate (the scheduler bounds per-cycle work; holder-aware grouping removes the fan-out it would otherwise bound) — BUT an interface constraint applies: this epic and durability-scheduler f4 both construct/register AntiEntropy in the same c2 builder. Manifest-aware f2 MUST keep AntiEntropy::new stable (inject peer_selector via with_peer_selector) or land before scheduler f4 wiring. See f2-ae-holder-aware-exchanges and roadmap §4."
adr:
  - 0033-manifest-aware-peer-selection
  - 0015-anti-entropy-merkle-protocol
  - 0029-storage-pools-disk-resilience
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-06
---

# Manifest-Aware Peer Selection for AE + Scrub — Program Coordination

> **EPIC COMPLETE (2026-09-06):** f1 (`b651d87`), f2 (`d50362a`), f3
> (`4bc0914`) all landed — implemented in one pass (per-feature reviewer
> gates intentionally skipped) and the single independent review returned
> **PASS (iteration 2)** after the review-gap fix `ca0f7cb` (epic wiring
> integration test + `#[doc(hidden)]` observability seams). The DoD items
> below are checked; accepted deviations D1-A–D4-A and the process deviation
> are recorded in Implementation notes (2026-09-06). This document remains
> the map.

> **This is the coordination document for the manifest-aware-repair epic
> (ADR-0033, review triage Theme 5, wave 2 ④).** If you are implementing
> any feature under `refactoring/manifest-aware-repair/`, read this first —
> it tells you where your work sits in the whole, what must exist before
> you start, and what must not regress while you work. The per-feature
> docs (`f1-*`, `f2-*`, `f3-*`) are the authority for each feature; this
> document is the map.

## Summary

Anti-entropy and scrub assume full replication between arbitrary peers.
Today AE picks **random alive members** (`AntiEntropy::select_alive_peers`,
`crates/oceanfs-durability/src/anti_entropy/engine.rs:863-878`) and sends
its **full sealed-segment list** to whatever peer was chosen
(`engine.rs:538-547`); scrub's `partition_for_current_nodes`
(`scrub.rs:612-623`) partitions the node's whole local sealed list across
alive peers and `ScrubGrpcService::assign_partition`
(`scrub_service.rs:43-57`) **acks without executing**. Under data-pool
partial replication (the in-flight healing epic) these assumptions are
false — peers no longer hold each other's data.

ADR-0033's decision: **the segment's replica set (`storage_locations`) is
the unit of comparison.** The manifest/holder set becomes the *entry
point* of both algorithms, never "all alive nodes", and selection is
**injected** from the node layer as a `PeerSelector`/`PartitionPlanner`
trait (the same shape as g5's `RepairTargetSelector`, ADR-0030) so
`oceanfs-durability` gains no manifest/membership-internals dependency.

The precedent machinery already exists: g4's `HolderIndex`
(`crates/oceanfs-durability/src/reconcile.rs:185`, built from
`storage_locations` stamps at the single choke point
`SegmentLifecycleCoordinator::set_storage_locations`), g5's
`ManifestRepairTargetSelector` (`crates/oceanfs-node/src/repair.rs:73`),
and g6's manifest filters (`crates/oceanfs-node/src/routing_cache.rs`,
`healthy_data_pools`/`is_write_degraded`/`can_accept_writes`). This epic
reuses that machinery — **no new protocol, no Merkle change**.

## What stays untouched (non-goals)

- The Merkle protocol itself, the incremental tree, sampling *rates*, and
  the gRPC `MerkleExchange` wire format (ADR-0015) — **unchanged**. Only
  *which segments go to which peer* changes.
- Scrub full-scan semantics (spec §7.5): a node still scans its local
  `.dat` set and verifies against the stored seal-time root. Only the
  *distribution/partition mechanism* changes.
- Replica *choice* (that is g5's re-replication job). This epic only
  *selects comparison/scrub partners* from existing holders.

## Feature DAG

```
refactoring/composition-root-decomposition   (c1/c2/c3, wave 2 ①)
refactoring/store-unification                (ADR-0032, wave 2 ②)
   └── f1-peer-selector-trait.md             trait(s) in oceanfs-durability
        │                                    + ManifestPeerSelector / ManifestPartitionPlanner
        │                                    in oceanfs-node; SegmentPartition becomes pub
        ├── f2-ae-holder-aware-exchanges.md  AntiEntropy entry point = segment→holders;
        │                                    per-segment eligible-holder exchange; local-only
        │                                    segments excluded from remote exchange
        └── f3-scrub-partition-by-holder.md  holder-aware partition planner replaces the
                                             alive-peer fan-out; assign_partition wired to
                                             execute a real partition scrub (no silent ack)
```

- **f1 → f2, f1 → f3**: the AE/scrub engines consume the injected trait
  and the node supplies the concrete selector.
- **f2 ∥ f3**: independent once f1 lands (AE engine vs scrub coordinator +
  gRPC service).
- Every feature depends on the composition-root decomposition (c2/c3 build
  the injection points) and the unified store (ADR-0032 / store-unification
  epic), because both engines read segment data and holder sets through the
  same store/registry after wave 2 ②. In practice f1's node-side
  constructors and f2/f3's `new()` call-site changes land inside the c2/c3
  module builders.

## Shared design (all features)

1. **Holder set is the entry point.** A node's own sealed segments come
   from its lifecycle registry (`SegmentLifecycleRegistry::for_each`,
   `SegmentState::Sealed`), exactly as today. What changes: each segment's
   comparison/scrub partners are derived from that segment's
   `metadata.storage_locations`, **never** from the set of all alive
   nodes.
2. **Eligible = alive + healthy.** A holder is an eligible peer when it is
   membership-`Alive`/`Suspect` and its gossiped `NodeManifest` shows at
   least one Healthy data pool. A stale/missing manifest stays eligible —
   the I/O error path is the truth (ADR-0029 §D5). This predicate lives in
   the node layer (g6 filters already encode it).
3. **Local-only segments have no remote partner.** Segments whose eligible
   holder set is empty (`storage_locations == {self}` or all other holders
   Dead) are *not* exchanged remotely; they are covered by the existing
   local verification fallback (`AntiEntropy::local_merkle_verify`) and by
   local scrub. This is the deliberate coverage shift ADR-0033 documents —
   AE cadence stays, scrub cadence carries the local-only guarantee.
4. **Injection, not dependency.** `oceanfs-durability` defines the trait;
   `oceanfs-node` implements it over `Membership` (which carries
   manifests) + the manifest filters. Node.rs constructs it once and hands
   it to the durability workers (c2) — the same wiring used for
   `RepairTargetSelector` → `ManifestRepairTargetSelector` today.

## Epic Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds for `oceanfs-durability`
      and `oceanfs-node`.
- [x] **AE is holder-driven:** no path in `AntiEntropy` (`run_cycle`,
      `run_continuous_cycle`, `run_sampling_cycle`) selects comparison
      peers from `Membership::nodes()` / `select_alive_peers`; all three
      start from the sealed segment → `storage_locations` map and exchange
      roots only with eligible holders. Review block `engine.rs:226`
      removed; the "send full sealed list to a random peer" path
      (`engine.rs:538-547`) is gone.
- [x] **Local-only segments:** a segment with no eligible remote holder is
      excluded from remote exchange in all three AE modes; the
      `local_merkle_verify` fallback and local scrub cover it.
- [x] **Scrub partitions by holder:** `ScrubCoordinator` no longer
      partitions the local set across all alive peers; partitions are
      computed per segment over `storage_locations` and no partition ever
      lists a segment for a node that does not hold it. Local-only
      segments stay in the local partition.
- [x] **No silent ack:** `ScrubGrpcService::assign_partition` executes the
      assigned partition through the local `ScrubWorker` (and reports a
      truthful result), or the scaffolding is deleted — there is no
      accept-and-ignore path left.
- [x] **Merkle + modes intact:** ADR-0015 protocol, incremental tree,
      sampling fraction, continuous/sampling triggers unchanged; the only
      delta is peer/partition selection.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` pass
      (RocksDB caveat, PIPELINE.md §4.6); new tests cover the pub trait
      surface, holder-aware grouping, and local-only handling.
- [x] **Docs:** every `pub` item in the new modules has `# Examples`;
      `#![deny(missing_docs)]` passes in both crates.
- [x] **ADRs:** ADR-0033 D1–D3 satisfied; no ADR-0015 / ADR-0029 §D5
      constraint violated.
- [x] **Integration:** a node-crate test exercises the wiring: a sealed
      segment whose `storage_locations` names one alive+healthy holder and
      one dead holder is exchanged/scrubbed only with the eligible holder.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (see
> `guidelines/coding.md` §9.2).

## Implementation notes (2026-09-06)

Implementation complete and independently reviewed — reviewer **PASS
(iteration 2)**, 0 gaps, after the review-gap fix `ca0f7cb`. The epic was
implemented in **one pass** (three code commits `b651d87` / `d50362a` /
`4bc0914`) with per-feature self-reports and a SINGLE independent review at
the end; per-feature reviewer gates were intentionally skipped (process
deviation). The accepted deviations below record what was actually built
against the prose above (implementer + reviewer agreed; user validated
D1-A / D2-A / D3-A / D4-A):

- **D1-A — AntiEntropy keeps a stable `new()`; the PeerSelector is injected
  via the builder.** durability-scheduler f4 already landed and
  constructs/registers `AntiEntropy` in the same c2 builder, so f2 added
  `with_peer_selector(Arc<dyn PeerSelector>)` (builder `with_*` style,
  stored as `Option`) rather than a constructor parameter. When unwired
  (unit tests / no selector), no remote exchange occurs — all segments are
  local-only. f2's "Interface (Public API)" section is amended to the
  builder form.
- **D2-A — `ScrubCoordinator::new(config, planner: Arc<dyn
  PartitionPlanner>, self_id: NodeId)`.** There is no planner-less
  coordinator state; the old `with_distributed` scaffolding was deleted.
- **D3-A — coordinator dispatch over gRPC is not wired, so
  `ScrubCoordinator::run_cycle` plans the sealed inventory via
  `plan_cycle_partitions` and executes the UNION of all planned partitions
  locally (== the inventory; each segment exactly once), preserving the spec
  §7.5 full-scan guarantee.** f3's DoD phrasing "self partition == sealed
  inventory" is imprecise under this semantics and is clarified in f3's
  Deviations.
- **D4-A — `ScrubGrpcService::new(registry, data_store)` builds its
  `ScrubWorker` internally** so `ScrubWorker` stays `pub(crate)`; the
  earlier f3 doc text implying the service ctor takes `Arc<ScrubWorker>` was
  inconsistent with that visibility — corrected in the f3 doc.
- **Test-observability seams** (needed for the epic DoD "Integration"
  node-crate wiring test, `ca0f7cb`): `AntiEntropy::holder_exchange_groups()`
  is `#[doc(hidden)] pub`; `ScrubCoordinator::plan_cycle_partitions` is
  `#[doc(hidden)] pub` (f3 Interface text had written `pub(crate)` —
  updated to match).
- **Optional LOW (noted, non-blocking):** f3's Data Flow/Scope text
  originally mentioned `spawn_blocking` for `assign_partition` while the
  code uses `tokio::task::spawn` over the async store reads (consistent
  with `run_cycle`); the f3 doc is corrected to match.

## References

- ADR-0033 (this epic's decision), ADR-0015 (AE protocol — unchanged),
  ADR-0029 §D2/D5 (NodeManifest, routing cache — hint discipline),
  ADR-0025 (lifecycle registry = sealed-set source), ADR-0030 (selector
  precedent)
- Review anchors: `anti_entropy/engine.rs:226` (+:184, :199 out of scope),
  `scrub.rs:601`
- Roadmap: `docs/features/refactoring/review-2026-09-roadmap.md` (Theme 5,
  wave 2 ④)
- Precedent: g4 `HolderIndex` (`reconcile.rs`), g5
  `ManifestRepairTargetSelector` (`oceanfs-node/src/repair.rs`), g6
  manifest filters (`oceanfs-node/src/routing_cache.rs`)
