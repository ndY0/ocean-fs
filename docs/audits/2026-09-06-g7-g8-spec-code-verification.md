---
audit_date: 2026-09-06
scope: feature-spec-vs-code
target: docs/features/disk-resilience-healing/wal-loss-recovery.md, metadata-loss-recovery.md
severity_counts:
  critical: 3
  high: 6
  medium: 5
  low: 2
---

# Audit: g7/g8 Healing Feature Specs vs Current Code (2026-09-06)

Verifies `docs/features/disk-resilience-healing/wal-loss-recovery.md` (g7)
and `metadata-loss-recovery.md` (g8) against the current tree. Both specs
are dated 2026-08-22 and predate the wave-2 substrate (composition-root
module builders, store unification/ADR-0032, event-WAL lifecycle/ADR-0025,
bounded metadata scans/ADR-0034, durability scheduler/ADR-0017, legacy-mode
removal/ADR-0031, manifest-aware AE+scrub/ADR-0033). g1–g6 of the healing
epic are `done`; g7/g8 remain `proposed`.

Method: two grounded code explorations (see findings for file:line), plus
reads of the two feature docs, the epic, and the wave-3 roadmap entry.

## Verdict summary

Both g7 and g8 keep a valid core premise but contain **stale code anchors**
and several **substantive contradictions with current machinery**. Neither
is implementable as written. g7 additionally depends on the not-yet-written
replicated-lifecycle-state ADR (review #30, roadmap wave 3) for restoring
seal-time metadata after event-WAL loss.

| Claim in spec | Current truth | Verdict |
|---|---|---|
| wal pool holds data WAL + event WAL; objects CF intact on metadata pool | True — data WAL at `modules/storage.rs:155-160`, event WAL at `storage.rs:338-349`; both under `{wal}/…`, `pool_paths.rs:77-83`; objects CF on metadata pool | ✔ holds |
| Registry is rebuilt by scanning data-pool `.dat` roots (g7) | **Absent**; registry is built only from checkpoint + event-WAL fold (`storage.rs:562-596`), and the startup residue sweep **deletes** registry-unknown `.dat` (`storage.rs:658-722`) | ✘ contradicts / missing |
| Fresh-WAL boot must never trust old files (g7) | `WalWriter::open` resumes last file; "fresh" is only the empty-dir case; no replacement marker | ◐ partial |
| Missing-segment enumeration from objects-CF chunk refs (g7) | No such path exists; `list_objects_all*` remain but have no callers (ADR-0034) | ✘ missing |
| Write-resume gate: clear `write_degraded` only when caught up + WAL verified (g7) | `write_degraded` set on wal Dead (`pool/health.rs:774-786`); Dead absorbing (`health.rs:856-858`); `reset_pool` exists but has no production caller | ✘ missing |
| `node_unavailable` is carried in the manifest so peers route around (g8) | **False**: `NodeManifest` has no node-level field (`membership/manifest.rs:151-158`); peer routing counts data-pool health only (`routing_cache.rs:280-339`) | ✘ contradictory |
| Metadata death is catastrophic locally; S3/read 503 (g8) | True locally — derived `node_serves_requests()` (`pool/mod.rs:1167-1171`); read gate `read/coordinator.rs:549`, write gate `write/coordinator.rs:469` | ✔ holds |
| Fresh store via `create_if_missing` reopen (g8) | True primitive (`metadata/store.rs:273-319`); store opened once at boot (`node.rs:274-278`), no runtime reopen | ◐ partial |
| Rebuild needs new `ListObjectsInRange`/`ObjectRow` RPC (g8) | Absent everywhere (no proto/handler/tests); healing surface is fixed (`healing.proto:67-108`) | ✔ spec gap confirmed |
| Rebuild enumerates owned **ring ranges** (g8) | `VnodeRange` exists only as degenerate add/remove return (`types/node.rs:86-96`, `ring.rs:89-134`); no owned-range API; ownership is per-segment `storage_locations` | ✘ missing abstraction |
| Tombstones not rebuilt — "orphan reaper covers" (g8) | Reaper/GC are byte-accounting over the deletions CF (`orphan_reaper.rs:41-62`, `liveness_tracker.rs:11-49`); an empty deletions CF blinds local reaping — reclamation leaks, not merely delays | ◐ half-true |

## Critical findings

### C1 — g7: startup residue sweep would DELETE the data pools on wal loss
`StorageModule::run_startup_recovery` (`modules/storage.rs:562-762`) folds
the event WAL into the registry and then unlinks every `.dat` whose registry
entry is `None`/`Deleted` (the once-per-boot residue sweep,
`storage.rs:658-722`, `delete_shards_with_pool` at `:703`). When the wal
pool (data WAL + event WAL + checkpoint) is replaced by an empty root, the
fold is empty and the registry is empty — so the sweep treats every intact
data-pool `.dat` as residue and deletes it. g7's required "rebuild registry
from `.dat` roots before GC/reaper" step does not exist and must also
neutralize/replace this sweep (adopt, not delete).

### C2 — g8: peers cannot route around a metadata-dead node
`Node::node_unavailable()` (`node.rs:571-573`) is derived from
`node_serves_requests()` and only consumed locally (read/write 503 gates).
`NodeManifest` (`membership/manifest.rs:151-158`) carries only per-pool
`id/role/status/write_degraded/…`. Peer filters (`routing_cache.rs:280-339`,
`can_accept_writes`) never consult the metadata role. A metadata-dead node
with healthy data pools remains a valid read candidate and write target;
SWIM keeps it Alive (sockets answer). The epic DoD "peers route around
without SWIM suspicion timeouts" requires new wire surface (node-level
unavailable flag in the manifest, or a routing rule keyed on metadata-pool
Dead) that does not exist.

### C3 — g7: `.dat`-only registry rebuild loses seal-time metadata
`merkle_root`, `storage_locations`, `contained_objects` (ADR-0034 f3),
`total_bytes` ride the event WAL / registry, not the segment header. A
rebuild from `.dat` files alone cannot reproduce them; g7 must recover them
from peers or via recompute. Roadmap wave 3 already lists the
replicated-lifecycle-state ADR (review #30, `segment_replicator.rs:353`) as
a prerequisite — g7's design should be written against that (or explicitly
fold a metadata-fetch into catch-up).

## High findings

### H1 — g7: fresh-WAL boot semantics undefined in code
`WalWriter::open` (`wal/writer.rs:109-142`) resumes from the last file
(`find_latest_file`); "fresh" is only the empty-dir case. g7 needs an
explicit "wal pool was replaced" detection (e.g., empty roots + present
data pools + absent checkpoint) and a defined reset path, because a normal
restart must still replay the existing WAL.

### H2 — g7/g8: no production reset path for a Dead pool
`PoolStatus::Dead` is absorbing (`pool/health.rs:856-858`);
`HealthMonitor::reset_pool` ("g7's WAL/metadata replacement…") exists
(`health.rs:641-685`) but has **no production caller**; the only
wal-recovery clear is a test (`health.rs:1468-1485`). Both recovery flows
must define and wire the runtime replacement/reset handoff.

### H3 — g7: missing-segment enumeration is absent and ADR-0034-constrained
No `enumerate_missing_segments` / `WalRecoveryOutcome` exist. The needed
objects-CF enumeration APIs (`list_objects_all`,
`list_objects_all_with_bucket`) remain but have no callers and their docs
are stale; the spec must justify a one-time recovery scan under ADR-0034 or
route enumeration through an owned/accounting-bounded path.

### H4 — g8: `ListObjectsInRange`/`ObjectRow` absent (confirmed gap)
No proto, generated stub, or handler. Healing service surface is fixed
(`proto/oceanfs/healing.proto:67-108`). This is genuinely new wire surface,
as the spec's own code-grounding predicted — still to be built.

### H5 — g8: no owned-ring-range abstraction
`VnodeRange` is used only as a degenerate add/remove return value
(`core/types/node.rs:86-96`, `ring.rs:89-134`); routing is per-key
`Ring::lookup`; `RingCache` exposes lookup/update/snapshot only. What a node
owns today is per-segment `storage_locations` (`repair.rs`, reconcile,
segment_replicator), not object-key ring arcs. g8's rebuild requires a new
range enumeration over vnode positions (feasible, absent).

### H6 — g7: recovery-ordering hazards for a new flow
AE Merkle snapshot is taken before recovery (`durability.rs:319-327` vs
`node.rs:358-364`); the ReRepWorker is not spawned until
`background::spawn_all` (`node.rs:393-404`), so enqueue-before-spawn into
its bounded 1024 channel buffers. Any g7 catch-up that must run before
serve and await completion needs an earlier spawn or direct `execute_repair`
calls (`durability/src/repair.rs`).

## Medium findings

### M1 — Stale code anchors throughout both specs
`node.rs:558-564 / 696-707 / 1159-1181 / 480`, `store.rs:169-182 / 201-247`,
`wal/entry.rs:52-79` (now `entry.rs:76-78`), and Phase-A-era naming ("f6",
"f7") reference pre-composition-root locations. Boot and construction now
live in `modules/{storage,durability,server,membership}.rs` and `node.rs`
calls module builders (`node.rs:305-426`).

### M2 — g8 "ObjectLookup membership-driven" wording collides with a live symbol
`ObjectLookup` (`gc/compaction_recovery.rs:76-91`) is the compaction-recovery
point-read trait, unrelated to any peer-driven object rebuild. The phrase in
the bounded-metadata-scans notes and g8 prose should not be reused for the
proposed rebuild.

### M3 — g8 tombstone-loss accounting is under-specified
Deletions CF now feeds reaper + GC byte accounting
(`liveness_tracker.rs:11-49`); skipping tombstone rebuild leaves the node
unable to reap/compact its own fully-dead segments (`dead(0) >= total` is
false, `orphan_reaper.rs:181-185`). The spec must state the reconciliation
mechanism (cross-node remap/re-replication rewrite, or peer deletions
listing) rather than "orphan reaper covers".

### M4 — Metrics names proposed in both specs are unregistered
`oceanfs_wal_recovery_*`, `oceanfs_metadata_rebuild_*`,
`oceanfs_metadata_unavailable_seconds` do not exist. Note
`oceanfs_startup_rebuild_ms` (`modules/storage.rs:506-508`) already records
the current startup recovery duration.

### M5 — Local gates already exist for the metadata case
Read/write coordinators already 503 on metadata Dead
(`read/coordinator.rs:540-551`, `write/coordinator.rs:459-471`). g8's local
unavailability half is partly done; the missing half is peer signaling +
rebuild.

## Low findings

### L1 — `RocksDbMetadataStore` open-on-replaced-root is untested
`create_if_missing` fresh-open works only on missing/empty dirs
(`metadata/store.rs:273-319`); no test covers "root wiped while process
down, reopen yields fresh DB" or a live-remount reopen.

### L2 — g7 writes-resume metric/verification-write primitive not defined
No WAL verification-write helper exists; the spec should specify what the
"one write+fsync+read-back" probe is against the current `WalWriter`
(`wal/writer.rs:173-230`).

## What still holds (no rewrite needed)

- Data WAL still journals inline blob data before `.dat` (`sealer.rs:587-595`,
  `coordinator.rs:1296-1328`), so the "accepted but not yet `.dat`" set is
  real and lives on the wal pool.
- Objects CF (metadata pool) is the surviving record g7 needs; chunk refs
  still point at segment ids.
- g8's no-re-replication core stays correct: `.dat` files (data pools) and
  the lifecycle registry (wal pool) survive metadata loss; only
  objects/deletions CFs are lost.
- `ReRepWorker::execute_repair` is the reusable catch-up primitive for g7.
- Boot order gives a clean hook: recovery runs between
  `StorageModule::build` and `ServerModule::build` / `serve`
  (`node.rs:334-382`), and the node does not serve until after it.

## References

- `docs/features/disk-resilience-healing/wal-loss-recovery.md`
- `docs/features/disk-resilience-healing/metadata-loss-recovery.md`
- `docs/features/disk-resilience-healing/epic.md`
- `docs/features/refactoring/review-2026-09-roadmap.md` (wave 3, review #30
  replicated-lifecycle ADR)
- Current code anchors cited inline (2026-09-06 working tree)
