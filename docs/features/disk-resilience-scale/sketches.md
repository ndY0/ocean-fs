# disk-resilience-scale — Feature Sketches (for the spec-writer)

> Architectural sketches only — the spec-writer expands each into a full
> feature doc under this directory using the repo feature template. ADR-0036 is
> the governing decision; the epic `epic.md` holds the DAG, DoD and crate notes.

---

## d0 — regression-gate + baseline fix

**Sketch.** **Pre-epic cleanup + gate.** The design session decided the seven
known pre-existing failures documented across the Phase A/B feature docs are
**fixed, not accepted** (decision 2026-09-07): d0 fixes them to zero (each in
its own commit + review), then runs the full workspace gate that has been
idle: `cargo test --workspace` lib suites + node integration binaries + the
quick functional e2e allowlist (crash_restart, wal_recovery,
cluster_lifecycle, data_pool_placement, metadata_pool_recovery, etc.), all
RocksDB-touching crates under `--test-threads=1` (PIPELINE §4.6), plus
clippy `-D warnings`/fmt/rustdoc on affected crates. Record the **green**
baseline (commit + counts) in the feature doc so later failures are
attributable. The seven items: (1) server `swim_death_detection_within_timeout`,
(2) server 2× `replicated_hlc`, (3) server rustdoc links `RING_PROBE_HASHES` +
`HintObjectApplier`, (4) durability dead test fn
`test_hint_wal_implements_wal_writer_trait` (hint_wal.rs:848),
(5) storage doctest `ObservedIo:987`, (6) flaky
`fetch_falls_through_on_replica_error_and_counts_failover`,
(7) in-process node restart leak (seal-worker-not-joined). **Key decision:**
fix, not accept. **Open questions:** item-7 scope (largest fix; storage-level
substitute restart coverage is the interim while it is tracked); item
archaeology (verify each reproduces at HEAD).

## d1 — pool-drain-state

**Sketch.** Add a registry-level `Draining` state to the pool runtime
(`oceanfs-storage/src/pool/`), distinct from `Degraded`/`Dead`:
`PlacementPolicy::select_data_pool` excludes `Draining` pools from *new*
segment targets immediately; the health monitor never transitions a
`Draining` pool to `Degraded`/`Dead` from drain-induced activity (read-only
ops keep flowing, so I/O signals stay normal — but the monitor must not fight
an operator's drain). Reads keep serving from the pool until the last copy
moves. Manifest carries the state so peers see "not a placement target" while
it drains. Blocked reason observability (`oceanfs_pool_drain_blocked_reason`)
feeds the no-destructive-failure rule. **Crate impact:** storage (pool state,
placement filter, manifest build), node (wiring). **Key decision:** where
`Draining` lives in the `PoolStatus` enum vs. a parallel flag — sketch assumes
a distinct status so every existing status consumer (health, placement,
routing, manifest) reasons about it in one place. **Open questions:** does a
peer routing filter treat `Draining` like Degraded (route around new writes)
while local reads still succeed? (Sketch: yes for writes, no for reads — same
split as `write_degraded`.)

## d2 — segment-relocation (the durable pool_id mutation)

**Sketch.** The core primitive. Extend `MetadataRefreshEvent` (the
ADR-0030-style refresh payload in `oceanfs-storage/src/segment/lifecycle.rs`)
with an **optional `pool_id` section** — same length-discriminated,
backward-compatible decode discipline as the `storage_locations` extension.
The lifecycle fold updates `SegmentMetadata.pool_id`; the event-WAL +
checkpoint remain the only durable writer (ADR-0025). Add a relocate operation
on the unified store (`oceanfs-storage/src/segment/data_store.rs`):
**copy → commit → unlink** under the per-segment write lock (ADR-0032 D3) —
copy `.dat` to the target pool root via the existing atomic write path, commit
the refresh event, then unlink the source copy. Reader per-segment
pool-root caches are purged at the commit. Crash windows are both safe (see
ADR-0036 D3). GC/reaper interplay: drain enumerates from the registry; a
relocated segment's new `pool_id` is what GC/compaction see; boot-residue
sweeps must not reap a pre-commit target copy as orphan (ordering handles it:
pre-commit the target file is unregistered — the reaper's once-per-boot sweep
could see it; verify the sweep's grace/registry rules and keep target files
outside the reaper's unregistered path until commit, or make the copy step
invisible to listing until committed). **Crate impact:** storage (lifecycle
event format + fold, data-store relocate), node (none direct). **Key
decision:** new event family vs. extended refresh event — ADR-0036 D2 chooses
**extend refresh** (established pattern). **Open questions:** exact wire
encoding of the optional section; interaction with `total_bytes`/
`contained_objects` accounting (segment does not change, only its pool — no
accounting delta expected); whether `relocate` needs an `explicit_pool`
override on the write path or a dedicated store method (sketch: dedicated
method that takes the target pool id and skips `resolve_pool`).

## d3 — intra-node drain (C1a)

**Sketch.** A `DurabilityTask` under the ADR-0017 scheduler Tier-1 budget.
Given a `Draining` data pool on a node with ≥ 2 eligible data pools, walk the
node's lifecycle registry for entries whose `pool_id` is the source
(**never a disk scan** — ADR-0034), and for each sealed segment relocate it
via d2 to a placement-chosen sibling pool (reuse `select_data_pool` with a
Draining-exclusion + headroom filter). Paced by configurable
`max_bytes_per_tick`. If no sibling fits (capacity/headroom), the worker
parks and reports blocked with the reason; nothing is deleted; the pool stays
`Draining`. On empty, the pool becomes `Detachable`. The workflow the feature
documents end-to-end: attach a sibling (f8) → drain → detach. **Crate
impact:** storage (worker), node (scheduler wiring + admin start/pause/resume).
**Key decision:** reuse placement policy vs. a drain-specific selector —
sketch reuses it with a filter. **Open questions:** drain order (largest
segments first? oldest first?) for bounded-tick usefulness; interaction with
concurrent sealing (a segment can't be mid-seal on the source — reserve/seal
must not target a `Draining` pool, which d1 guarantees); what happens to
active in-memory segments whose pool becomes Draining mid-life.

## d4 — cluster drain (C1b) — pool-only AND node-level

**Sketch.** The value feature. A controller that empties a **source** (one
pool with no viable sibling, an operator-selected set, or the node's whole
footprint) by moving copies to **other nodes**, then **source-releases** the
local copy. Per held segment (registry-enumerated): select a cluster target
with the existing `ManifestRepairTargetSelector` / repair dispatcher
(capacity-aware, healthy data pools, not `node_unavailable`), issue the
existing `RequestReReplication` RPC (ADR-0030) — the **target pulls**, its own
placement picks the pool. When the target's `storage_locations` stamp lands,
the source refreshes its own registry entry to the holder set **minus self**
(the locations payload is a set replacement) and unlinks the local `.dat`.
This is the new **source-release** primitive; reconciliation (g4) stops
counting the source on the durable refresh, so no heal loop re-adds it. The
controller runs as a paced Tier-1 task, pausable, terminal (empty/watermark),
and is the documented pre-step before node `leave(None)`. Pool-only retirement
must be possible even when the node has exactly one data pool (mover is
off-node). Admin surface: pool-level and node-level drain endpoints.
**Crate impact:** node (controller, admin, selector reuse), durability
(reuse RPC/worker; possibly a drain RPC if the controller needs to tell a
target "pull this for a drain" vs. repair — verify the existing
`RequestReReplication` reason field suffices), storage (source-release
refresh). **Key decision:** source-release mechanics + its durability/
reconciliation/GC story — this is where the epic's risk concentrates.
**Open questions:** does source-release need a distinct event reason or is it
an ordinary holder-set refresh? Target backpressure when many segments drain
at once (reuse the re-rep bounded queue)? Watermark semantics for node drain
(empty, or leave N replicas for still-owned ranges)? Interplay with g7/g8
boot-recovery paths if the node restarts mid-drain (registry rebuild must
recover Draining pools + in-flight source-releases). Ring-share consequence of
shrinking a node (without C2a the freed capacity is reclaimed via new writes +
repair targeting, not ring re-weighting — document as expected interim
behavior).

## d5 — detach-and-drop

**Sketch.** The inverse of f8 `attach`: `PoolRegistry::detach(id)` accepted
only when the pool is empty (`Detachable`). Removes the pool from the live
registry, drops it from the node's topology/config view, rebuilds + re-gossips
the `NodeManifest` (f6 path) so peers stop treating the node as having that
capacity. No restart. Validation mirrors attach (unique name/root/cardinality
checks in reverse; a `wal`/`metadata`/`hints` role pool cannot be detached —
those roles are cardinality-1 and their replacement is the g7/g8 boot path,
not drain). **Crate impact:** storage (registry detach), node (admin +
manifest wiring). **Key decision:** what "config drop" means for a
config-file-driven topology vs. the runtime registry. **SETTLED 2026-09-07
(ADR-0036 D8):** detach is **persistent** — boot applies `config − removed`,
where `removed` is a crash-safe node-local record keyed by name+root
(precedent: g7's wal-replacement marker); re-attach of the same name+root
clears the record. Rejected: ephemeral registry overlay (a restart would
resurrect an empty pool and placement would refill it) and config-file
rewrite. **Open questions:** removed-record storage path/format; record-GC
rule for never-matching stale records; empty-check authority; root-directory
handling; zero-data-pool runtime state (boot validation sees the post-overlay
set).

## d6 — capacity-weighted ownership (C2a) — OPTIONAL / LATE

**Sketch.** Make a node's ring share track its healthy data-pool capacity so
attach/drain/detach re-weight ownership. Requires: deriving a per-node weight
from the gossiped `NodeManifest` pools (f6), plumbing it into
`Ring`/`RingConfig` construction (currently uniform `vnodes_per_node`,
`oceanfs-routing/src/ring.rs`), deterministic convergence across nodes, and
hysteresis so capacity jitter does not churn the ring. Placed last and marked
optional: its blast radius touches the freshly-reworked membership/ring path
(ADR-0028 territory) and the refactor just stabilized it. **Crate impact:**
membership (weight source), routing (weighted ring), node (weight derivation).
**Key decision:** whether to attempt in this epic at all — measure difficulty
when d1–d5 land; defer to the backlog `disk-resilience-capacity` epic
(which also carries C2b) if it looks disruptive. **Open questions:** weight
semantics (total capacity vs. free capacity vs. explicit pool weight);
interaction with stamped `storage_locations` (ring share ≠ current placement);
rebalance-free acceptance that new writes/repairs reclaim freed capacity
slowly.
