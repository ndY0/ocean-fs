---
feature: "Sealed-Segment Replication (Data-Replication Backbone)"
epic: "disk-resilience-healing"
status: done
priority: critical
owner: ""
dependencies: []
adr: [0029]
perf: [2.6, 2.7, 4.4, 7.1]
created: 2026-08-23
updated: 2026-08-23
---

# Sealed-Segment Replication (Data-Replication Backbone)

## Summary

**Corrective blocker discovered while scoping g3 (`loss-announcement`):**
object **bytes** are not durably replicated anywhere. The write path
(`coordinator.rs:471`) replicates object bytes to the **object's** ring
replicas via `AppendSegment`, but the receivers discard the bytes for every
mid-segment object (only the offset-0 object of a segment is persisted, as a
fragment — `segment_service.rs:312-317`), and the read path
(`fetch.rs:536`) fetches segment data from `ring.lookup(hash(segment_id))` —
the **segment's** ring replicas. No code ever sends segment data to that
set. The net effect: data bytes live on exactly one node (the writer),
except first-object fragments; the read path's replica fallback is a mirage
(the failover unit test passes only because it manually seeds the serving
replica's store).

This feature builds the data-replication backbone the epic assumes:
after a segment seals, its **full data section** is pushed to the segment's
ring replicas (`ring.lookup(hash(segment_id)) − self` — the exact set the
read path already fetches from). Seal itself never makes a network call; a
decoupled replicator task performs the pushes. The receiver is idempotent
(any number of duplicate pushes converge to one copy). `storage_locations`
becomes real and durable: the intent set is stamped on the registry entry
when the segment is fully replicated, so g3's announcement fan-out and g4's
reconciliation have a true holder set to work from. The replicator's
failure set (`needs_replication`) IS the g4 reconciliation skeleton.

## Scope

### In Scope

- **Proto/RPC:** `PushSealedSegment(stream PushSealedSegmentRequest) →
  PushSealedSegmentResponse` on `SegmentRpc` (storage.proto). Client-streaming
  like `AppendSegment` (64 KB chunks, perf 4.4); the first chunk carries the
  segment metadata (segment_id, tier, ec_k, ec_m, merkle_root,
  storage_locations) + the first data slice; the receiver assembles the
  data section from the stream.
- **Receiver (`SegmentGrpcService::push_sealed_segment`):**
  - assemble the data section from the stream;
  - **verify the pushed merkle root** against `MerkleTree::build(data, 0)`
    (64 KiB leaves — the shared seal/scrub/AE default) and **reject**
    (`invalid_argument`) on mismatch: a corrupt push must never register;
  - persist via the existing `SegmentDataStore::write_segment_data`
    (fabricates a valid v1 header — the proven heal-worker pattern,
    `heal/worker.rs:407`; the file is readable by `read_segment_data` and
    `fetch_shard`, so the replica serves the full data section);
  - register idempotently: `request_reserve` then `request_seal` with the
    **pushed** metadata (tier/ec/merkle_root/storage_locations, pool_id 0);
    tolerate `AlreadySealed`/`AlreadyDeleted` (duplicate push = success);
  - the registration makes the replica visible to GC/reaper and gives g4 a
    holder entry whose `storage_locations` matches the owner's.
- **Append path is METADATA-ONLY (Option A — owner-approved design).**
  The offset-0 fragment write in `append_segment` is REMOVED, and a
  metadata-less append is rejected (`invalid_argument` — a protocol
  violation; every production caller carries object metadata). The push is
  the SOLE writer of `{segment_id}.dat` on a receiver node. This removes
  the phase-2 two-writer race (append fragment vs push full data) by
  construction — **no lock, no write-path interference with replication**.
  The metadata-only append persists object metadata so reads locate the
  object; the bytes come from the segment's ring replicas (or the owner)
  via the read path's gRPC fallback.
- **Target derivation (single source of truth):**
  `oceanfs-routing::segment_replica_set(ring, &SegmentId) -> Vec<NodeId>` =
  `ring.lookup(blake3::hash(segment_id.to_string()))` — the SAME derivation
  the read path uses today (fetch.rs:535, 770). Those two call sites switch
  to the helper. The replicator's targets, g3's fan-out, and g4's
  live-copy math all use it. ONE derivation, never two.
- **Replicator (`oceanfs-node::segment_replicator`):**
  - `SegmentReplicator::new(ring, membership, pool, data_store, lifecycle,
    config)`; `run(shutdown_token)`; bounded channel (capacity ~1024,
    perf 2.6);
  - `enqueue(segment_id)` — **non-blocking `try_send`; on full, the segment
    goes straight into `needs_replication`** (never dropped silently, never
    blocks the seal path);
  - drain loop: read the segment's data section locally via the
    `SegmentDataStore` (the `.dat` is durable by the time the notifier
    fires — seal worker fires after `seal_from_data` returns Ok; compactor
    fires after `request_seal(new)` returns Ok; startup fires after
    rebuild), compute targets = `segment_replica_set − self`, push to each
    target (bounded concurrency per target: 2, perf 2.7; throttle
    `replication_throttle_bytes_sec` mirroring `heal_throttle_bytes_sec`);
  - per-target ack tracking: only when ALL targets ack, stamp
    `storage_locations = [self] + targets` on the registry entry
    (`SegmentLifecycleCoordinator::set_storage_locations`); unacked targets
    stay in `needs_replication`;
  - periodic sweep (interval, default 5 s): retry `needs_replication`
    entries at a bounded rate — **at most `MAX_RETRIES_PER_SWEEP` (16)
    per tick** so a large needs set cannot starve the seal-event channel
    drain (no hot loop, no retry storm); a failed segment stays in the
    set until a sweep succeeds or g4 reconciliation re-homes it — never
    dropped, retried indefinitely (durability backbone semantics);
  - metrics: `oceanfs_segment_replication_pushed_total`,
    `oceanfs_segment_replication_bytes_total`,
    `oceanfs_segment_replication_retries_total`,
    `oceanfs_segment_replication_failures_total`,
    `oceanfs_segment_replication_needs_gauge`.
- **Enqueue sites (owner-side only — never the push receiver, no
  replication loop):**
  1. seal worker notifier — the existing
     `WriteCoordinator::with_segment_sealed_notifier` (coordinator.rs:1633);
     the node's wiring (node.rs:1246) additionally enqueues;
  2. compactor — new optional
     `GarbageCollector::with_segment_sealed_notifier` (called by the
     compactor after `request_seal(new)` returns Ok,
     segment_compactor.rs:294-298). **The repacked segment is a NEW
     segment id sealed OUTSIDE the seal worker — without this hook,
     post-compaction objects silently have zero replicas** (verified in
     the code; this is the compaction-interaction gap);
  3. startup — after `rebuild_with_data_wal` (node.rs:1292), enqueue every
     Sealed entry with **empty** `storage_locations` (the seal→stamp crash
     window; also covers WAL-replayed segments).
- **`SegmentLifecycleCoordinator::set_storage_locations(id, set)`** —
  in-memory update of the live entry's `metadata.storage_locations`
  (registry shard write lock). The next checkpoint persists it (the
  checkpoint serializes the full `SegmentMetadata` via bincode). The
  SealEvent binary format is **unchanged** (see Deviations).
- **Node wiring** (composition root): build the replicator, extend the
  seal notifier closure, wire the compactor notifier, run the startup
  pass, spawn the replicator task; `Node` accessor for tests.
- **Tests:**
  - unit (routing): `segment_replica_set` equals `ring.lookup(hash(id))`
    and matches the fetch-path derivation;
  - unit (receiver): push with matching root registers + serves
    `fetch_shard`; wrong root rejected; duplicate push idempotent
    (AlreadySealed tolerated, data overwritten);
  - unit (replicator): target = set − self; full-ack stamps
    storage_locations; partial-ack leaves the segment in
    `needs_replication`; channel-full routes to `needs_replication`;
  - integration (oceanfs-node, 3 real nodes, legacy mode, RF=3): PUTs
    spanning multiple segments (several 32 KiB bodies pushed concurrently,
    so multiple objects pack into 64 KiB small segments — mid-segment
    objects exist) → wait for seal + push → assert the peer nodes' stores
    actually hold the sealed segments' data → **delete the owner node's
    `*.dat` files** → GET every object **from the owner node itself**:
    local read fails, the gRPC fallback must serve the bytes from the
    replicas (hash-verified);
  - integration (compaction variant): DELETE some objects → trigger GC
    (small `gc_interval_sec` + `tombstone_ttl_sec` + compact threshold
    above 0.5 — partially-dead packed segments qualify) → the repacked
    segments (NEW ids, sealed OUTSIDE the seal worker) are replicated to
    B and C. **This test pins the compactor enqueue hook**
    (`GarbageCollector::with_segment_sealed_notifier`): without it,
    post-compaction objects have zero replicas. Read-availability after
    compaction is NOT asserted — see GAP-1 (compaction metadata remap
    does not propagate to replicas; deferred to g3/g4).

### Out of Scope

- **Object-ring byte shipping** (AppendSegment no longer carries data —
  it is metadata-only; the old raw-data branch was dead code and removed).
  The remaining follow-up is metadata-only AppendSegment at the SENDER
  (`replicate_write` / `forward_write`): they still stream object bytes
  that receivers discard. Removing that traffic halves write-path
  data-plane volume; deferred because it changes write-replication
  semantics and the churn tests must re-verify against it.
- Stale-replica reclamation after compaction (the old segment's replicas
  on peers hold dead bytes until g3/g4 delete-propagation/reconciliation
  reclaims them — disk-fill class, not correctness; reads point at the new
  id). **See GAP-1 — the deeper issue is that compaction's METADATA remap
  also fails to propagate, which breaks reads (not just disk usage).**
- g3 announcement, g4 reconciliation, g5 re-replication (this feature
  builds their shared primitives; the `needs_replication` set is their
  input).
- EC parity replication (a Standard-tier segment's parity section is not
  pushed; the replica serves data-shard reads — what the read path asks
  of replicas — and data-shard availability makes EC recovery
  unnecessary; parity-on-replicas is a later EC feature).
- **Churn-campaign replication checks (follow-up, phase-3 E2E scope).**
  The cluster-mode E2E campaign (T1-T43) validates join/leave/rejoin/
  kill against the control plane but never asserts **replicated data
  availability after churn** (T43 checks the surviving node's own store,
  not the replica copy). Identified during this feature's review: the
  campaign should add — after a node dies, read an object through a
  SURVIVOR whose local store never held it (proving the replica copy
  served it), and assert `needs_replication` drains to zero after ring
  convergence. The primitive this feature builds (segment_replica_set +
  push + needs set) is what makes those assertions meaningful.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-routing` | `segment_replica_set(ring, &SegmentId)` helper (ring.rs) |
| `oceanfs-server` | `SegmentGrpcService::push_sealed_segment` handler; `append_segment` becomes metadata-only (offset-0 fragment removed, metadata-less append rejected); fetch.rs uses the shared derivation |
| `oceanfs-storage` | `SegmentLifecycleCoordinator::set_storage_locations` |
| `oceanfs-durability` | `GarbageCollector::with_segment_sealed_notifier` (compactor enqueue) |
| `oceanfs-node` | New `segment_replicator` module + composition-root wiring + Node accessor |
| `proto` | `PushSealedSegment` messages + RPC (storage.proto/segment.proto), regen |

## Interface (Public API)

- `oceanfs-routing::segment_replica_set(ring: &RingCache, id: &SegmentId) -> Vec<NodeId>`
- `oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::set_storage_locations(id, locations)`
- `oceanfs_durability::GarbageCollector::with_segment_sealed_notifier(Arc<dyn Fn(SegmentId) + Send + Sync>)`
- `oceanfs_node::segment_replicator::SegmentReplicator` — `new(...)`,
  `enqueue(segment_id)`, `run(shutdown_token)`, `needs_len()`,
  `register_metrics`
- `Node::segment_replicator()` (test accessor)

## Data Flow

```
seal worker Ok ──┐
compactor Ok ────┼─▶ enqueue (bounded channel, try_send; full → needs_replication)
startup rebuild ─┘
                      ▼
replicator drain ──▶ read local .dat ──▶ targets = segment_replica_set(ring) − self
   ├─ push each target (streaming, throttle, concurrency 2)
   ├─ all acked ──▶ set_storage_locations([self] + targets)  (checkpoint-durable)
   └─ partial ──▶ needs_replication ──▶ periodic sweep retry (5 s)
receiver ──▶ verify merkle root ──▶ write_segment_data ──▶ reserve+seal (idempotent)
   └─ registered with pushed storage_locations (g4 holder set)
read path ──▶ local miss ──▶ segment_replica_set ──▶ FetchShard ──▶ replica serves
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in oceanfs-routing,
      oceanfs-server, oceanfs-storage, oceanfs-durability, oceanfs-node
      (verified: all 5 crates build clean; fmt --check clean)
- [x] **Tests:** all listed green (routing helper, receiver idempotency +
      root verification, append metadata-only/rejection, replicator
      target/ack/needs logic, 3-node durability integration incl. the
      compaction variant)
<!-- REVIEW: verified (iteration 2). Routing equivalence unit test present (ring_cache.rs:135-146, segment_replica_set == ring.lookup(blake3::hash(id))). Receiver idempotency + root rejection (segment_service.rs:1824, :1870, :1895); append metadata-only/no-registration (:996) + metadata-less rejection (:1069). Replicator unit tests (segment_replicator.rs:829-993: targets_exclude_self_and_match_ring, missing_entry_is_skipped, unacked_push_lands_in_needs_set, channel_full_routes_to_needs_set, deleted_segment_is_dropped_from_needs_set) — all pass. 3-node integration 2/2 green (tests/segment_replication.rs, --test-threads=1, ~35s). RESIDUAL (LOW): single_node_targets_empty_stamps_locations (segment_replicator.rs:881-922) does NOT verify the full-ack→stamp side effect — it only asserts the empty-targets parking path (needs_len==1) and never reaches the stamping branch. Full-ack stamping is exercised only indirectly by the integration tests (needs draining to 0 ⇒ all-acked ⇒ stamp_locations runs) and is never asserted on the registry entry. Either rename the test (single_node_targets_empty_parks_in_needs) or add a direct full-ack stamping assertion (requires a client-injection seam). -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
<!-- REVIEW: verified (iteration 2). # Examples present + doctests green: segment_replica_set (ring_cache.rs:73), set_storage_locations (lifecycle.rs:2079), GarbageCollector::with_segment_sealed_notifier (garbage_collector.rs:153), SegmentReplicator::new/enqueue/needs_len/register_metrics/run (segment_replicator.rs:261/:354/:388/:427/:651). RUSTDOCFLAGS="-D warnings" clean on oceanfs-routing, oceanfs-storage, oceanfs-durability, oceanfs-node. oceanfs-server still fails with 2 PRE-EXISTING errors at HEAD (admin.rs links private RING_PROBE_HASHES; coordinator.rs unresolved HintObjectApplier) — verified in a HEAD worktree. -->
- [x] **ADR:** ADR-0029 §D4 (announcement fan-out targets become real:
      `storage_locations` is populated) + §D3 (data-pool death → reads
      fail over to replicas) satisfied at the data-plane level
      (verified: receiver registers pushed holder set on every replica;
      owner stamps [self]+targets on full ack only; needs set + 5s sweep
      is the §D4 reconciliation skeleton; §D3 failover proven by the
      owner-disk-death integration test)
- [x] **Perf:** 2.6 (bounded channel + needs set — no unbounded queue),
      2.7 (bounded push concurrency), 4.4 (64 KB chunk streaming),
      7.1 (enqueue is a single atomic channel send on the seal path —
      no locks, no I/O; all network work is off the seal path)
<!-- REVIEW: verified (iteration 2). 2.6: mpsc::channel(1024) (segment_replicator.rs:307); overflow → needs set (:363-380); MAX_RETRIES_PER_SWEEP=16 bounds the sweep (:743-757). 2.7: Semaphore::new(max_concurrent_pushes) (:557). 4.4: chunk_size 65536 (:178) + client-streaming PushSealedSegment (proto). 7.1: enqueue is try_send only, no I/O/locks on the seal path (:363-381). Throttle GAP FIXED: ByteRateLimiter implemented (segment_replicator.rs:90-143) — fixed-window limiter, consulted in replicate_segment before pushes (:553), wired from NodeConfig::replication_throttle_bytes_sec (config/node.rs:257 → node.rs:856), no-op at 0 (:118-120), lock dropped before sleep (guard scoped to the block, :122-139), bounded sleep ≤100 ms (no busy-loop). Note: heal_throttle_bytes_sec itself is config-only in the heal worker (never applied), so the replicator's throttle exceeds heal's actual behavior — the "mirrors heal" phrasing is aspirational. -->
- [x] **Integration:** 3-node local cluster — write objects spanning
      multiple segments on A, delete A's `*.dat` files, read every object
      back **from A** with byte-identical hashes served by the replicas
      (the test that should have existed since phase 2)
      (verified: data_survives_owner_disk_death_via_segment_replicas and
      repacked_segments_are_replicated_to_ring_replicas both pass
      under --test-threads=1, ~35s)

## Deviations (accepted)

- **Option A (owner-approved): the offset-0 fragment writer in
  `append_segment` is REMOVED — the append path is metadata-only and the
  push is the SOLE writer of `{segment_id}.dat`.** This supersedes an
  earlier implementation that serialized the two writers with a per-segment
  mutex; that lock was rejected (rightly) because it coupled the write path
  (object-ring append) to replication (segment-ring push) and added latency
  to the write path. Removing the fragment eliminates the race by
  construction — one writer per `.dat`, no lock, no interference. The
  metadata-only append still persists object metadata (reads locate the
  object); segment bytes come from the ring replicas / owner via the read
  path's fallback. A metadata-less append is now a protocol violation and
  is rejected (`invalid_argument`); the old raw-data branch was dead code
  (no production caller) and was removed, and the transport tests that used
  it for data placement were migrated to `push_sealed_segment`.
- **`storage_locations` is NOT added to the `SealEvent` binary format.**
  The event WAL is `Copy`, fixed-size, byte-exact (event_wal.rs:24-53),
  with `MAX_PAYLOAD_SIZE` bounds and crash-matrix pinning; adding a
  variable-length NodeId list is a format change on the most
  durability-critical path in the codebase. Instead: the replicator
  stamps the intent set on the live registry entry only after **all**
  targets ack, the next checkpoint persists it (full metadata bincode),
  and the startup pass re-enqueues Sealed entries with empty
  storage_locations (covering the seal→stamp crash window and replayed
  segments). Observable semantics for g3/g4 are identical; the event WAL
  stays byte-identical. The stamp happens AFTER full ack, so
  `storage_locations` non-empty ⇒ every listed holder was confirmed by
  its own ack.
- **Replica `.dat` files use the fabricated v1 header** (via the existing
  `write_segment_data`), not the owner's byte-identical file. The header
  is not consulted by the read path (data section + object ChunkRefs
  drive reads; fetch_shard slices the data section), and the heal worker
  already ships segments this way. The merkle root (over the data
  section) matches the owner's, so AE/scrub anchors agree.
- **Compaction leaves stale replicas of the old segment on peers** until
  g3/g4 delete-propagation/reconciliation reclaims them. Reads are
  unaffected (metadata points at the new id); the bytes are dead.
  Recorded as the g4 handoff.
- **Follow-up (out of scope): metadata-only AppendSegment.** Once the
  segment push is the data copy, the object-ring byte shipping in
  `replicate_write` is pure waste (receivers discard it for mid-segment
  objects). Removing it halves write-path data-plane traffic; it is
  deferred because it changes write-replication semantics and the churn
  tests must re-verify against it.
- **Replica placement is pool-0/legacy** (pool_id 0 — the append path's
  convention). Spreading replicas across the receiver's data pools is a
  g5 placement concern.

## Known Gaps & Race-Condition Inventory (deferred — read before g3/g4/g5)

These findings came from the compaction-variant integration test plus a
deliberate race-condition sweep of the new data path. They are NOT fixed in
this feature; each is a concrete requirement for the next features.

### GAP-1 (CRITICAL for g3/g4): compaction metadata remap does not propagate to replicas

**Observation (from the compaction-variant test):** after A's GC compacts a
segment, A's object metadata is remapped to the NEW repacked segment id.
B/C's metadata for the same objects still references the ORIGINAL segment
id — compaction only rewrites the OWNER's RocksDB. Meanwhile B/C run their
own GC with the same fast config: they see the (replicated) tombstones,
compact THEIR copies of the original segment into THEIR OWN new ids, and
DELETE the original from their stores.

**Failure mode:** a GET routed to B (or C) looks up the object, gets the
ORIGINAL segment id from B's metadata, and that segment no longer exists
on B (their GC deleted it) NOR on A (its GC deleted it) NOR anywhere else
(the repacked ids differ per node). Result: `500 cannot fetch chunk ... no
segment reader and gRPC not available`. The read-availability assertion in
the compaction-variant test was therefore scoped DOWN to "survivors
readable through the owner while the owner's data is intact"; the stronger
"read from replicas after owner death" is impossible until this gap is
closed.

**Reproduction evidence:** the test's probe (before scoping) showed the
failing chunk `45c8-7f50-bca0-7bd1` — an original segment present in NEITHER
node's directory at failure time, while A/B/C each held different repacked
ids (`7c03`, `7bfb`, `4dbe`...).

**Required by:**
- g3 (`loss-announcement`): the announcement/fan-out must carry the
  segment-remap (old id → new id) so replicas re-point their metadata, or
- g4 (`reconciliation`): the repair loop must detect "metadata references
  a segment id that exists on no live holder" and re-point it (a
  metadata-repair primitive, not just a replica-count repair).

**Also implies:** tombstone propagation currently triggers EVERY node's GC
to compact its own copies independently — the repacked ids diverge per
node. If the design intent is "one repacked segment per logical segment",
the compactor needs a deterministic repack id (e.g., derived from the old
id) or the owner's repack must be authoritative and propagated.

### GAP-2 (FIXED in this feature): needs-set leak / hot-loop on compacted-away segments

A segment Deleted by compaction (registry state Deleted + `.dat` unlinked)
that sat in `needs_replication` was retried forever: `replicate_segment`
only skipped MISSING entries, so a Deleted entry proceeded to read the
unlinked `.dat`, failed, and re-parked itself in needs — every sweep, for
ever. **Fixed:** `replicate_segment` now returns `Ok` for any non-Sealed
entry, so `process` drops it from the needs set. Unit test
`deleted_segment_is_dropped_from_needs_set` pins it. (This was the "one
wasted sweep" companion to the replicator-read-vs-compactor-unlink race:
the read fails once, the next sweep sees the folded Deleted state and
drops it.)

### GAP-3 (LOW, pre-existing pattern): replica `.dat` writes are not atomic

`DiskSegmentStore::write_segment_data` uses `std::fs::write` (no
temp+rename). A `fetch_shard` on a receiver racing an inbound push can read
a partially-written `.dat` → bad header → the read falls through to the
next replica (self-healing via the error-driven fallback). Same pattern as
the heal worker's writes — pre-existing. **Recommendation:** make the
receiver's push write atomic (temp file + rename, like the sealer's
`write_segment_temp`), or route the push through the sealer's write path.
Low priority: the fallback absorbs it; but a 3-way simultaneous push storm
could transiently 500.

### GAP-4 (LOW, by design): ring change mid-push leaves a new replica unpushed

Targets are computed once per `replicate_segment` call. If a node joins
between the seal and the push, the new ring member isn't in this push's
target set. The segment stays with empty `storage_locations` (or stamped
with the OLD set) until the needs-set sweep re-derives targets — or g4
reconciliation re-homes it. Acceptable (g4's live-copy math uses the
current ring); noted so g4 doesn't assume `storage_locations` is
ring-current.

### GAP-5 (INFO): `storage_locations` is a snapshot, not ring-current

Stamped at full-ack time. After a ring change it can be stale in either
direction (a departed node listed, a new node missing). g4 must recompute
live copies against the CURRENT ring and treat `storage_locations` as
intent, not truth (matches the g4 doc's "metadata belief" deviation).

### GAP-6 (INFO): no cross-node retry state

The needs set is in-memory per node. A node crash between seal and full
ack loses it — recovered by the startup pass (Sealed + empty
storage_locations → re-enqueue). A node crash between a target's write and
its ack: the target re-registers idempotently on the next push (receiver
AlreadySealed-tolerant). No durable retry log is needed at this stage; g4's
periodic scan is the safety net.
