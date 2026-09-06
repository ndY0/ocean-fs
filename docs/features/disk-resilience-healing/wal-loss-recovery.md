---
feature: "WAL-Pool Loss Recovery (Catch-up from Replicas)"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["loss-announcement", "re-replication-worker"]
adr: [0029, 0035]
perf: [1.3, 7.1]
created: 2026-08-22
updated: 2026-09-06
---

# WAL-Pool Loss Recovery (Catch-up from Replicas)

## Summary

ADR-0029 §D7's first durability-critical path, rewritten against ADR-0035
(replicated segment lifecycle state). When the **wal** pool dies, the node
loses the data WAL, the event WAL and the checkpoint — everything that
journals its own durability record (data WAL at
`modules/storage.rs:155-160`, event WAL at `modules/storage.rs:338-361`,
both under `{wal}/…`, `pool_paths.rs:76-83`). The node rejects new writes
(`write_degraded` is set when the wal pool is Dead,
`pool/health.rs:774-786`) but keeps serving reads: the objects CF on the
metadata pool and the data-pool `.dat` files are both intact.

When the wal pool is replaced — at boot after a restart, or **live via
remount** (mandatory, g7 D2) — recovery must NOT trust the old WAL
contents and must NOT run the normal event-WAL fold (the event log is
gone). It rebuilds its lifecycle registry from the **replicated lifecycle
state** (ADR-0035 D1/D2): every alive holder of a segment already carries
that segment's seal-time metadata in its own registry entry (the
`PushSealedSegmentRequest` payload, `proto/oceanfs/segment.proto:150`).
Recovery (a) detects the replacement, (b) suppresses the destructive
once-per-boot residue sweep, (c) re-derives the registry by pulling holder
lifecycle metadata for the candidate segments found in its intact
data-pool `.dat` files, and (d) re-materializes any segment that is
missing locally through the ADR-0030 `ReRepWorker` catch-up path. Writes
resume only after catch-up completes and the fresh WAL passes a
verification write.

## Scope

### In Scope

- `oceanfs-node` replaced-wal detection and branch selection:
  - The boot path folds the event WAL into the registry, then unlinks
    every `.dat` whose registry entry is `None`/`Deleted` once per boot
    (`run_startup_recovery`, `modules/storage.rs:562-762`; residue sweep
    `:658-722`, `delete_shards_with_pool` at `:703`). When the wal pool
    has been replaced, the fold is empty and the registry is empty — the
    sweep would treat every intact data-pool `.dat` as residue and delete
    it (audit C1). Recovery therefore needs a **"wal pool replaced"
    detection** that selects the rebuild-from-holders branch and
    suppresses the residue sweep (ADR-0035 D4).
  - Detection = an explicit replacement marker written by the
    operator/remount path, plus the boot heuristic: the wal root contains
    no WAL/checkpoint files while at least one data-pool root contains
    `.dat` files. A normal restart (existing WAL files present) takes the
    existing fold path unchanged — `WalWriter::open` resumes the last
    file, `wal/writer.rs:109-142`; "fresh" alone is only the empty-dir
    case, so the marker/heuristic (never CRC judgment alone) is what
    distinguishes replacement from a normal boot (audit H1).
  - **Fresh-WAL open (replaced branch)**: open a fresh data WAL
    (`WalWriter::open` on the empty root) and a fresh event WAL +
    checkpoint on `paths.event_wal`. Old files are never replayed or
    trusted — they are gone by definition of the replacement.
- **Registry rebuild from holders (ADR-0035 D2 — replaces the `.dat`-scan
  rebuild, which is absent and cannot work; audit C3)**:
  - Enumerate candidate segment ids from the intact data-pool roots
    (`data_store.list_segment_files`, the same per-root listing the
    residue sweep iterates at `storage.rs:671-722`).
  - For each candidate, pull the segment's lifecycle metadata from a live
    holder — new holder-fetch RPC (wire surface below). The holder's
    registry entry carries the full seal-time shape (`state`, `tier`,
    `ec_k`, `ec_m`, `merkle_root`, `storage_locations`,
    `contained_objects`, `total_bytes`, `pool_id` — ADR-0035 D1). These
    fields ride the event WAL / registry, not the segment header, so a
    `.dat`-only rebuild cannot reproduce them; the holder pull is
    mandatory, not optional.
  - Local-only segments (`storage_locations == {self}` or no live remote
    holder) are rebuilt from their own `.dat` (ADR-0035 D3): recompute
    the Merkle root, infer tier/EC geometry from the segment header, and
    accept the `contained_objects` / `total_bytes` accounting degradation
    (GC may over-retain; never a data-loss event).
  - The rebuilt registry feeds the same consumers the fold does
    (AE/scrub/GC/reaper/read path); the once-per-boot residue sweep must
    never run against it (ADR-0035 D4).
- **Missing-segment reconciliation and catch-up (ADR-0035 D2, g7 D3)**:
  - Cross-check the surviving objects CF (metadata pool, intact — chunk
    refs still point at segment ids) against the rebuilt registry + local
    `.dat` presence; referenced-but-not-materialized segment ids join the
    catch-up set. This is a **one-time, recovery-only** enumeration with
    an explicit ADR-0034 carve-out (it runs once per replaced-wal
    recovery, off the hot path, never periodically). The existing
    `list_objects_all` / `list_objects_all_with_bucket` primitives
    (`metadata/store.rs:733-762`) have no callers and stale docs today;
    this feature gives them their one caller and refreshes the docs.
  - Execute catch-up through the ReRepWorker (ADR-0030): enqueue a
    `ReRepRequest` per missing segment via `ReRepWorker::sender`
    (`durability/src/repair.rs:244-248`); the worker fetches full data +
    metadata from a live holder (`holders − self`), writes through the
    pool-aware store, registers reserve + seal, and stamps
    `storage_locations`.
  - **Boot/live wiring (g7 D3)**: the ReRepWorker object is constructed
    in `DurabilityModule::build` but its `run` loop is not spawned until
    `background::spawn_all` (`modules/durability.rs:657-664`,
    `node.rs:394`) — after `run_startup_recovery` (`node.rs:364`) and
    after the server binds. The catch-up drain must run with the worker
    loop live:
    - boot (replaced branch): run the rebuild-from-holders drain after
      `spawn_all` has started the worker and before the node clears
      `write_degraded` — the existing wal-Dead 503 gate
      (`write/coordinator.rs:540-549`) holds throughout the drain, while
      reads may serve (the objects CF and data pools are intact);
    - live remount: the worker is already running when the remount
      handler starts the same drain.
  - **Drain completion condition**: catch-up is complete when every
    enqueued segment is verifiably materialized — its `.dat` exists and
    its registry entry is sealed with a non-empty `storage_locations` —
    re-checked idempotently until the set is empty or a request exhausts
    the worker's retries. Permanently-failed segments keep
    `write_degraded` set and are re-driven by the g4 reconciliation loop
    (the existing repair backstop); reads of their objects surface the
    existing `SegmentUnavailable` path.
- **Write resume gate**: clear `write_degraded` only when (a) the
  catch-up set is empty AND (b) the fresh WAL passes a **verification
  write** (one write + fsync + read-back probe through `WalWriter`; no
  such helper exists today — `wal/writer.rs:173-230` — audit L2, this
  feature specifies it). Clearing rides the registry write gate:
  `PoolRegistry::set_status(wal_id, Healthy)` +
  `set_write_degraded(wal_id, false)` (`pool/mod.rs:1092,1126`), then
  `HealthMonitor::reset_pool(wal_id, Healthy)`
  (`pool/health.rs:641-685`, today with **no production caller**) so the
  monitor's internal `Dead` mirror is cleared and `Dead` stops being
  absorbing (`health.rs:856-858`). Audit H2.
- **Live remount path (mandatory, g7 D2)**: replacing the wal device must
  heal the node WITHOUT a restart. A remount handler (admin/attach
  surface) receives the replaced root, opens fresh WAL/event-WAL handles
  on it, writes the replacement marker, runs the same registry-rebuild +
  catch-up drain against the already-running ReRepWorker, then performs
  the reset handoff above. The boot-time path after a restart covers the
  same recovery via the marker.
- **Hint WAL note**: hint debt lives on the hints pool — if THAT pool is
  also lost, hints are delivery intent only; the reconciliation loop (g4)
  rebuilds the debt (ADR-0029 §D7).
- Metrics (fresh names — none of the earlier `oceanfs_wal_recovery_*`
  names are registered today; audit M4):
  - `oceanfs_wal_replaced_total` (counter) — replaced-wal recoveries
    entered;
  - `oceanfs_wal_recovery_registry_rebuilt_segments` (gauge) — registry
    entries restored from holders / recomputed;
  - `oceanfs_wal_recovery_caught_up_total` (counter) — segments
    re-materialized through the ReRepWorker;
  - `oceanfs_wal_recovery_pending` (gauge) — current catch-up set depth;
  - `oceanfs_wal_recovery_seconds` (gauge) — last recovery duration. The
    existing `oceanfs_startup_rebuild_ms` (`modules/storage.rs:506-508`)
    continues to record the normal-branch startup rebuild.
- Tests:
  - unit: replacement detection (marker present; empty-wal-root +
    non-empty data pools; normal boot with an existing WAL selects the
    fold path);
  - unit: residue-sweep suppression (a registry-unknown `.dat` that the
    normal fold would sweep is retained on the replaced branch);
  - unit: candidate enumeration (.dat-derived ∪ objects-CF-referenced
    ids; locally-present segments excluded);
  - unit: holder-metadata fetch fold (RPC response → registry entry
    restores every seal-time field byte-exact);
  - unit: local-only D3 recompute (no live holder → Merkle recompute from
    `.dat`; accounting gap recorded);
  - unit: write-resume gate (clears only after an empty catch-up set +
    WAL verification write);
  - integration (local 3-node, RF=2, **live remount**): kill the wal pool
    on A → writes 503, reads OK → replace the root with a fresh empty
    root WITHOUT restart → marker written, residue sweep suppressed (no
    data-pool `.dat` deleted), registry rebuilt from holders (metadata
    equals peers'), catch-up drains, verification write passes,
    `write_degraded` clears, writes resume, every pre-kill key reads
    back;
  - integration (local 3-node, boot variant): same scenario with A
    restarted after the replacement → the boot path performs the same
    recovery via the marker.

### Out of Scope

- Metadata-pool loss recovery (g8) — different store, different flow.
- Announcement/reconciliation/healing machinery (g3-g5) — reused, not
  built.
- Drain/rebalance (Phase C).
- Accepted-but-unsealed data that died with the data WAL and has NO live
  holder: the objects-CF row dangles (chunk refs to a segment with no
  `.dat` and no holder entry); reads surface `SegmentUnavailable`. This
  is not a re-replication event (there is no holder to pull from) and is
  the documented residual window of ADR-0029 §D7's "catch-up from
  replicas" (reachable only when a replica exists) / ADR-0035 D3-class.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Replaced-wal detection + branch selection; residue-sweep suppression on the rebuilt registry; registry rebuild from holder metadata; catch-up drain (boot/live wiring); live-remount handler + reset handoff |
| `oceanfs-durability` | Holder lifecycle-metadata fetch RPC handler (healing service); drain completion re-check over the ReRepWorker |
| `oceanfs-storage` | WAL verification-write probe helper (reuses `WalWriter`, `HealthMonitor::reset_pool`, `PoolRegistry` write gate) |
| `proto/oceanfs/healing.proto` | (to add) holder lifecycle-metadata fetch RPC + messages |

## Interface (Public API)

- `pub enum WalRecoveryMode { NormalFold, RebuildFromHolders }` — branch
  selector result.
- `pub fn detect_wal_recovery_mode(paths: &PoolPaths, registry:
  &PoolRegistry) -> WalRecoveryMode` — node-side, pure.
- `pub struct WalRecoveryOutcome { candidates: usize, restored: usize,
  missing: Vec<SegmentId>, caught_up: usize, verified: bool }`.
- `pub fn rebuild_registry_from_holders(candidates, holders, client,
  lifecycle) -> Result<usize>` — the holder-metadata fold (ADR-0035 D2).
- `pub fn enqueue_catch_up(missing: Vec<SegmentId>, sender) -> usize` —
  the ReRepWorker drain feed.
- `pub async fn verify_wal_write(wal: &WalWriter) -> Result<()>` — the
  one write + fsync + read-back probe (new; no helper exists).
- `pub fn reset_wal_pool(registry, monitor, pool_id)` — the
  `set_status(Healthy)` + `set_write_degraded(false)` + `reset_pool`
  handoff.

New wire surface (mark `proto/oceanfs/healing.proto` as **to add**; the
fields encode the seal-time registry shape the segment-push protocol
already carries, `proto/oceanfs/segment.proto:150`):
- `rpc FetchSegmentLifecycleMetadata(SegmentLifecycleQuery) returns
  (stream SegmentLifecycleEntry)` — pull the replicated lifecycle state
  for the requested segment ids from a live holder (ADR-0035 D1/D2).
  Request = `repeated SegmentId`. Response = one entry per segment the
  holder actually holds: `state, tier, ec_k, ec_m, merkle_root,
  storage_locations, contained_objects, total_bytes, pool_id`; ids the
  holder does not hold are simply absent from the stream (the caller's
  local-only recompute covers them, ADR-0035 D3). A "list the segments
  you hold" enumeration RPC is deliberately NOT added: the recovering
  node's candidate set is locally enumerable (data-pool `.dat` ∪
  objects-CF chunk refs) and the data pools are intact, so there is no
  locally invisible holder obligation to discover.

## Data Flow

```
wal pool Dead ──▶ write_degraded = true (health.rs:774-786) ──▶ local S3 writes 503
replacement (marker / live remount) ──▶ WalRecoveryMode::RebuildFromHolders
   └─ SUPPRESS the once-per-boot residue sweep (storage.rs:658-722)  ← never deletes data .dat
   └─ fresh data WAL + fresh event WAL on the empty root (old files never replayed)
   └─ candidate ids = data-pool .dat roots ∪ objects-CF chunk refs (one-time scan)
        └─ FetchSegmentLifecycleMetadata(live holder) ──▶ registry rebuilt (ADR-0035 D2)
             └─ local-only (no live holder) ──▶ recompute from .dat (ADR-0035 D3)
   └─ missing set ──▶ ReRepRequest via ReRepWorker::sender (repair.rs:244-248)
        └─ worker pull + write + register + stamp (running before the boot drain; already running live)
   └─ catch-up set empty ∧ WAL verification write ok
        └─ registry gate: set_status(wal, Healthy) + set_write_degraded(wal, false)
        └─ monitor.reset_pool(wal, Healthy) ──▶ writes resume
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-storage` (+ proto regen) — clean at
      HEAD `166348a`; `cargo clippy --lib -- -D warnings` clean on
      production code
- [x] **Tests:** all listed green — replacement detection (incl. the
      empty-placeholder audit-C1 regression), residue-sweep suppression,
      candidate enumeration, holder-metadata fold (byte-exact), local-only
      D3 recompute, write-resume gate, cancellable metrics poller — and
      the 3-node **live-remount** integration passes (asserts the outage
      503, reads served during the outage, no data-pool `.dat` swept, the
      write gate clears, and every pre-outage key reads back
      byte-identical). The boot-variant integration and a live
      holder-fold/catch-up e2e are NOT present; the reviewer accepted the
      unit-level substitute coverage (see Deviations a/c)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (`RUSTDOCFLAGS="-D warnings" cargo doc`, affected crates). One
      PRE-EXISTING broken doctest in `oceanfs-storage`
      (`io/disk_io.rs` ~line 995) is unrelated baseline cleanup, not a g7
      gate — Deviations d
- [x] **ADR:** ADR-0029 §D7 + ADR-0035 D1-D4 satisfied — registry rebuilt
      from holders (not `.dat`), residue sweep never runs against the
      rebuild-from-holders registry, ReRepWorker running before the drain,
      writes resume when caught up + verified
- [x] **Perf:** 1.3 (pre-sized candidate/registry vecs), 7.1 (all recovery
      scans are one-time, off the hot path)
- [x] **Integration:** the epic's wal-pool-kill DoD — the 3-node
      live-remount test asserts writes rejected (503) during the outage,
      reads served throughout, no data-pool `.dat` swept by recovery, the
      write gate cleared post-remount, and every written key read back
      byte-identical post-recovery (the deferred boot-variant e2e is
      Deviations a)

## Deviations (accepted)

Independent review: **PASS** at HEAD `166348a` (feature commits `38557c4`,
`4ce2f33`, `aaef22b`; review-fix commits `578be9f`, `26042a3`,
`166348a`). The first block records the design corrections from the
pre-implementation audit (2026-08-22 spec rewrite, commit `d0dce49`),
which the implementation follows. The lettered items (a)-(d) are the
review-agreed coverage/behavior deltas recorded when the feature closed,
and the last two bullets are review-hardening refinements folded in for
accuracy.

- **Registry rebuild is holder-pulled, not `.dat`-scanned.** Original
  (2026-08-22) g7 rebuilt the registry by scanning data-pool `.dat` roots.
  Audit C1/C3: no such path exists, a `.dat` scan cannot reproduce
  seal-time metadata (merkle/EC/`storage_locations`/`contained_objects`),
  and it collides with the destructive startup residue sweep. ADR-0035
  replaces it: each holder's registry entry IS the replicated lifecycle
  state (D1), pulled over a new holder-fetch RPC (D2). `.dat` files now
  supply only the candidate id set.
- **The startup residue sweep is suppressed, not merely preceded by a
  rebuild.** The original's "registry rebuild MUST run before
  GC/orphan-reaper" step is insufficient: `run_startup_recovery` unlinks
  registry-unknown `.dat` once per boot (`storage.rs:658-722`), so a
  replaced wal pool with an empty fold would delete the intact data pools
  (audit C1). The replaced branch never runs that sweep against the
  rebuild-from-holders registry.
- **Fresh-WAL semantics are a replacement-branch decision, not a CRC
  judgment.** The original pinned "WAL files fail CRC validation → open
  fresh". In code, `WalWriter::open` resumes the last file and "fresh" is
  only the empty-dir case (`wal/writer.rs:109-142`); a normal restart
  must replay the existing WAL. The replacement marker/heuristic selects
  the fresh-open branch (audit H1).
- **Missing-segment enumeration is a one-time recovery scan with an
  explicit ADR-0034 carve-out**, not a routine path: the objects-CF
  listing APIs exist but have no callers (`metadata/store.rs:733-762`),
  and ADR-0034's bounded-metadata-scans discipline must be cited for the
  single catastrophic recovery enumeration (audit H3).
- **Live remount is mandatory and implemented.** Original g7 described a
  startup flow plus "the g2 recovery path"; there was no production reset
  path (`HealthMonitor::reset_pool` has no caller; `PoolStatus::Dead` is
  absorbing — audit H2). Live remount now reuses `reset_pool` + the
  registry write gate, and boot recovery covers the restart case.
- **Catch-up executes through the ReRepWorker with explicit boot/live
  ordering and a drain-completion condition.** The worker is not spawned
  until `background::spawn_all` (after `run_startup_recovery` and the
  server bind — audit H6); enqueue-before-spawn would only buffer in the
  bounded channel. The replaced-boot drain therefore runs after the worker
  is live; the live-remount drain finds it already running.
- **Old code anchors replaced throughout.** `node.rs:558-564/696-707/
  1159-1181/480`, `store.rs:169-182/201-247` and `wal/entry.rs:52-79`
  referenced pre-composition-root locations; all anchors are now the
  module/audit locations cited inline (audit M1).

**Review-agreed deltas recorded at feature close:**

- **(a) The BOOT-VARIANT integration test is NOT present as an in-process
  3-node test.** A true boot-variant e2e — process restart after an
  out-of-band replacement, which would exercise the D2 holder fold +
  catch-up drain end-to-end against a genuinely empty registry — is
  deferred. An in-process same-directory RocksDB reopen is blocked because
  the server/data-plane tasks hold the store (`Arc<DB>`) past shutdown;
  only the RocksDB metrics poller was made cancellable (and is awaited at
  shutdown so the DB LOCK is released). The boot branch is covered at the
  unit level (detection incl. the empty-placeholder audit-C1 regression,
  residue-sweep suppression, D3 classifier truth table, candidate
  enumeration, holder fold, D3 recompute) and the live-remount integration
  test passes; the reviewer accepted this substitute coverage.
- **(b) There is NO out-of-band drain-completion watcher.** The catch-up
  drain is in-band only: `run_wal_pool_recovery` polls the outstanding set
  and stops when it empties, when it shows no progress for ~6 s (24 × 250
  ms rounds), or at the 120 s hard cap. If the drain stalls, local writes
  stay 503 until a restart (the retained replacement marker makes the next
  boot re-enter the rebuild branch) or a re-remount. The g4 reconciliation
  loop re-drives the repairs but never clears the write gate. Safe failure
  mode: no data loss — availability only.
- **(c) The D2 holder-fold/catch-up machinery is unit-verified but NOT
  exercised by a live end-to-end test.** The holder-metadata fold has a
  byte-exact unit test, and D3 recompute + the D3 classifier truth table
  are unit-covered; but the live-remount integration exercises only the
  remount surface, write gate, residue-sweep suppression and read-back — a
  live remount never loses the in-memory registry, so the holder fold is a
  no-op there (`restored=0 missing=0 caught_up=0`). The test header in
  `crates/oceanfs-node/tests/wal_pool_recovery.rs` states this honestly. A
  true e2e that runs the fold is the deferred boot-variant test (a).
- **(d) (informational — separate baseline cleanup, NOT a g7 deviation)**
  A pre-existing broken doctest at
  `crates/oceanfs-storage/src/io/disk_io.rs` (~line 995) constructs
  `ObservedIo` with a `total_bytes` field the struct never had (the field
  was added to the doc example by bounded-metadata-scans commit `6cb7958`,
  not to the struct). It fails on the base commit too and is unrelated to
  g7; tracked as baseline cleanup.
- **Checkpoint-before-marker-clear (review hardening).** The write-resume
  gate now persists a checkpoint of the rebuilt registry BEFORE clearing
  the replacement marker. On the live-remount path the in-memory registry
  survives but is never re-journaled to the fresh event WAL (the fold
  skips already-registered segments), so clearing the marker without a
  snapshot would let a later restart NormalFold an almost-empty event log
  and the residue sweep would delete every pre-remount `.dat`. The
  checkpoint covers the coordinator's LAST FOLDED position
  (`SegmentLifecycleCoordinator::last_folded_pos`), not the raw WAL tail —
  the same snapshot contract the coordinator's threshold checkpoint obeys
  (a snapshot covering an appended-but-unfolded event would seed a registry
  missing that segment). On checkpoint failure the marker is kept so the
  next boot re-enters the rebuild branch; on the boot path the snapshot is
  harmless (the rebuilt entries are also events in the fresh WAL) and just
  makes the next recovery cheaper.
- **WAL sync lower-bound reset (review hardening).** `WalWriter::truncate`
  and `reopen_fresh` now reset the sync group's `last_synced` lower bound
  when the write position rewinds below it. `verify_wal_write` exercises
  exactly this path — it appends its probe at offset 0, lets the group
  flusher advance past it, then truncates the probe away; a stale lower
  bound would let post-truncate appends be ACKed without a covering fsync
  (the flusher skips fsync while `current <= last_synced`). Regression
  test: `truncate_below_synced_position_resets_lower_bound`.
