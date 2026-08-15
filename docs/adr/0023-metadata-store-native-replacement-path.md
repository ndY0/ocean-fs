# ADR-0023: Metadata Store — RocksDB Black-Box Costs and the Path to a Native Replacement

**Status:** Proposed
**Date:** 2026-08-15
**Deciders:** OceanFS architecture team

---

## Context

OceanFS's metadata store is RocksDB-backed (`RocksDbMetadataStore`,
`crates/oceanfs-storage/src/metadata/store.rs`): three column families
(objects, segments, deletions), point gets/puts, prefix scans (LIST),
full scans (GC, scrub, anti-entropy), and `WriteBatch` batching. It has
been a dependency since the storage-crate split (ADR-0009) and was
accepted as a proven embedded KV.

The 2026-08-13→15 seal-pipeline and memory-stability work surfaced a
cluster of costs that are **not tuning noise but structural properties
of the dependency**:

1. **The allocator is not controllable from the Rust wrapper.** The
   `rust-rocksdb` crate does not expose setting RocksDB's internal
   allocator (jemalloc is linked by the C++ side; `oceanfs`'s mimalloc
   and RocksDB's jemalloc coexist but cannot be unified). Memory
   governance — the thing the project does everywhere else (BufferPool
   size classes, byte budgets, bounded channels) — is impossible for
   the single largest heap consumer in the process.

2. **Memory footprint is not stable and not attributable.** In the
   seed-42 120 s load run, process `VmData` grew to ~3.4 GB in *both*
   the baseline and the new build; the growth is not explained by the
   seal path (which after optimization runs ~2.6% above baseline in
   average RSS). The block cache is configured, but memtable growth,
   SST cache, and internal arena behavior are a black box; RocksDB's
   own property APIs (`block-cache-usage`, `cur-size-all-mem-tables`)
   are not currently surfaced as metrics, so we cannot even attribute
   the 3.4 GB.

3. **Shutdown is not clean.** RocksDB's C++ background thread pool is
   not safe for concurrent open/close across multiple DB instances in
   one process; tests abort with `SIGABRT` (`terminate called without
   an active exception`) unless run with `--test-threads=1`
   (PIPELINE.md §4.6). This is a permanent test-infrastructure tax and
   a process-exit hazard in production.

4. **A page-pinning attempt caused a whole-node crash class.** The
   swap-defense feature (`mlock_block_cache`) used
   `mlockall(MCL_CURRENT|MCL_FUTURE)`. `MCL_FUTURE` made every
   subsequent `mmap` count against `RLIMIT_MEMLOCK`; once the process
   crossed the ceiling, ALL allocations failed with `EAGAIN` and Rust
   aborted via `handle_alloc_error`. Root cause of the intermittent
   "memory allocation of N bytes failed" test crashes (2026-08-15),
   fixed by switching to `MCL_CURRENT` only — but the fix also
   *silently removed* the intended pinning benefit, because the Rust
   crate cannot expose the block cache's memory region for precise
   `mlock`. We could not implement the feature we wanted because the
   dependency is opaque.

5. **The C++ build is a standing cost.** `librocksdb-sys` compiles
   ~500 KLoC of C++ on clean builds (mitigated by system RocksDB, but
   the dependency remains a portability and supply-chain surface).

6. **RocksDB is also a DATA store for the inline tier — not just
   metadata.** Blobs ≤ `inline_threshold_bytes` (default 4096;
   `SegmentSizeConfig`, core/types/config.rs) are stored **inside
   `ObjectMetadata.inline_data`** and persisted via `put_object` into
   the objects CF (write/coordinator.rs Inline arm). The segment WAL
   deliberately **skips inline entries during replay**
   (wal/replay.rs:60, 105-106): inline-blob durability is RocksDB's
   responsibility, not the segment pipeline's. Consequences:
   - RocksDB holds real user payloads, so its memory footprint is not
     purely metadata — part of the unattributable RSS is inline data
     riding in the block cache / memtables / SSTs.
   - Reads of small objects hit RocksDB with the payload inline; the
     "store sits behind the L2 cache" framing is weaker for the
     inline tier than for chunk-ref metadata.
   - A native replacement must give inline payloads a durable home of
     their own: the store's WAL must carry them and replay must
     restore them (today's segment replay explicitly does not).

The workload itself, however, is *not* an LSM workload:

- The **metadata** set is RAM-sized: millions of objects × ~100–300 B
  serialized `ObjectMetadata` is hundreds of MB, not tens of GB.
  The **inline payloads** add threshold × count (4 KB × 1M objects =
  4 GB worst case at the default threshold) — RAM-sized only if the
  store spills payloads or the threshold stays small; a pure
  in-RAM index must be explicit about which of the two it is.
- Access is **point get/put dominated** (GET metadata → chunk refs,
  PUT metadata, DELETE tombstone, GET/PUT inline blobs) with warm
  prefix scans (LIST) and cold full scans (GC/AE/scrub).
- The store carries **no cross-node consistency burden** — replicas
  reconcile via HLC LWW in the cluster layer.
- Durability is **process-crash recovery**, and the project already
  owns the exact machinery: a WAL with group-commit fsync batching
  (`WalSyncGroup`, `wal/sync.rs`), rotation, and replay
  (`replay_wal`, `wal/replay.rs`) — proven and hardened in the
  seal-pipeline work. (Replay of *inline payloads* is the one piece
  of that machinery the segment WAL does not do today.)

### Forces

- **Correctness is the product.** Object metadata is the system's
  source of truth; every replica runs the same store, so a store bug
  is silent data loss with no replica safety net. RocksDB's
  crash-safety is battle-proven; a native store's would be new. The
  inline tier widens the blast radius: the store also holds **user
  blob payloads** (≤ 4 KB each), so a native-store bug loses small
  objects themselves, not just their metadata.
- **The store is not the current bottleneck.** Seal fsync and EC
  encode were; the store's costs are quality-of-life (shutdown,
  RSS attribution, tuning) more than throughput.
- **The failure mode is asymmetric.** RocksDB misbehavior is a warning
  and a tunable; a native-store replay bug is customer-visible data
  loss.
- **A full LSM replacement would be expensive and hazardous.** That
  was the earlier verdict (task deemed too costly), and it remains
  correct for "replace RocksDB with another LSM-class engine".
- **But the simple alternative is not an LSM.** A memory-first store
  with the project's own WAL reuses the hard 20% (durability, fsync
  ordering, crash replay) that already exists and works — provided it
  also gives inline payloads a durable, replayable home (the segment
  WAL's replay deliberately skips them today).

## Decision

**Keep RocksDB for now; do not commit to a rewrite today. Instead,
adopt a three-phase path that preserves the option to replace the
store natively, and make the decision to replace only on measured
evidence.**

1. **Phase 0 — Attribute the memory (do first, cheap).** Surface
   RocksDB's own property metrics (`block-cache-usage`,
   `cur-size-all-mem-tables`, `estimate-table-readers-mem`,
   `live-sst-files-size`) as gauges via `register_metrics`, alongside
   process RSS/VmData. This answers the open question "what is the
   3.4 GB?" before any further decision — and must also bound the
   **inline-payload share** (live inline objects ×
   `inline_threshold_bytes` is the theoretical floor of
   payload-derived memory). Expected outcome: either the block cache
   (tunable) or memtable/SST growth (also tunable) — or, if
   unattributable, evidence FOR replacement.

2. **Phase 1 — Squeeze RocksDB with what the wrapper exposes.** Cap
   memtable and block-cache budgets explicitly, bound compaction
   threads, set `max_open_files`, and steer the linked jemalloc via
   `MALLOC_CONF` environment (the only allocator lever available
   without a C FFI extension). Keep `mlock_block_cache` as
   `MCL_CURRENT`-only best effort; do not reintroduce `MCL_FUTURE`.

3. **Phase 2 — Pre-position a native replacement as a shadow-mode
   feature, not a swap.** If Phase 0/1 leave the RSS or shutdown
   costs unacceptable, pursue a **memory-first metadata store with the
   project's own WAL**:
   - sharded in-RAM index (BTreeMap per shard or sorted
     `(bucket, key) → meta`),
   - WAL-append + group-commit fsync for writes (reuse
     `WalSyncGroup`/`WalWriter`),
   - crash recovery = WAL replay (reuse the `replay_wal` pattern),
   - periodic snapshot/checkpoint to bound replay,
   - byte-bounded via the `BufferPool` size-class discipline.
   **The inline tier is an explicit design input, not an afterthought:**
   - **Option A — payloads in the WAL-backed store.** Inline blobs
     ride the store's own WAL (group-committed, replayable) and live
     in the in-RAM index up to `inline_threshold_bytes` × count. RAM
     bound = metadata + threshold × live inline objects; acceptable
     while the default threshold (4 KB) and object counts keep the
     product bounded, but it is a hard ceiling on either knob.
   - **Option B — value-log spill.** Inline payloads are appended to a
     write-ahead value log (the same WAL discipline, one fsync per
     group-commit batch), the in-RAM index holds only
     `(bucket, key) → (log_offset, len)`; replay restores the log,
     GC compacts it. RAM holds metadata + small offsets, payloads are
     disk-backed — the honest answer if inline volume can grow
     unboundedly.
   Either way, **replay must restore inline payloads** — the segment
   WAL's current "skip inline" rule (wal/replay.rs:60, 105-106) is a
   RocksDB-era shortcut that a native store must not inherit.
   The migration runs **alongside** RocksDB: dual-write, serve reads
   from the new store, verify byte-identical results (now including
   inline blob payloads, not just metadata), then flip with a
   one-way rollback hatch. This converts "hazardous rewrite" into a
   measured migration with an exit.
   This phase is a separate ADR + feature; this ADR only records the
   intent and the criteria that would trigger it.

**Scope of this ADR:** the decision *process* and the trigger
criteria. It does NOT approve a rewrite; it records why the option is
more defensible than the earlier verdict suggested, and what evidence
would justify exercising it.

## Consequences

### Positive

- **The train of thought is persisted.** The structural costs (opaque
  allocator, unattributable RSS, unclean shutdown, the `mlock`
  failure) and the workload analysis (not an LSM workload, hard 20%
  already owned) are recorded for the next person who asks.
- **Phase 0 is cheap and strictly informative.** Attribute-then-decide
  prevents both a knee-jerk rewrite and a permanent "RocksDB is fine"
  assumption.
- **Phase 2, if triggered, is low-risk by construction.** Shadow mode
  gives a safety net no greenfield rewrite has; rollback is a flag.
  Dual-write verification covers **inline blob payloads** too — a
  byte-identical check on real user data, not just metadata.
- **Reuse of proven machinery.** The WAL group-commit / replay code is
  already hardened; the native store would inherit its crash story
  rather than reinvent it. The one new piece — inline-payload replay —
  is small and well-understood (value-log restore).
- **Elimination candidates, if replaced:** SIGABRT shutdown and
  `--test-threads=1`; allocator black box; jemalloc/mimalloc
  coexistence; C++ build dependency; `mlock` impotence; and the
  "RocksDB holds user data" ambiguity in RSS attribution.

### Negative

- **Phase 2 is real work with real risk** (1–2 weeks for a senior Rust
  engineer in this codebase, plus verification), and the subtle 20% is
  genuinely subtle: torn writes, partial-page crashes, snapshot-vs-WAL
  ordering, replay idempotency, iterator stability during concurrent
  mutation. **Inline payloads add a durability surface the segment WAL
  never had to handle** — the store's WAL + replay must be correct for
  actual blob bytes, not just metadata records.
- **Correctness asymmetry remains:** RocksDB's crash-safety is proven
  over a decade; a native store's is new. The shadow-mode gate is the
  mitigation, not a guarantee. Inline data makes the stakes concrete:
  a replay bug corrupts small objects themselves.
- **Phase 1 tuning buys time, not freedom.** The wrapper's knobs are
  finite; if the RSS story is dominated by untunable internals, Phase
  2 becomes the only real fix.
- **Opportunity cost.** Time spent on Phase 2 is time not spent on
  remaining performance/durability features.

### Neutral

- The store's API surface (`MetadataStore` trait,
  `oceanfs-storage-api`) already abstracts the implementation; a
  replacement does not change crate boundaries (ADR-0009 stands).
- `--test-threads=1` remains the documented RocksDB-era constraint
  until/unless a replacement lands.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Keep RocksDB, tune harder (Phases 0–1 only)** | Zero rewrite risk; battle-proven crash safety; fastest to ship | Opaque allocator remains; shutdown SIGABRT remains; RSS may stay unattributable; `mlock` stays impotent | **Chosen for now.** The decision is to *measure and tune first*; this is the default until Phase 0/1 evidence says otherwise |
| **Full LSM replacement (another LSM-class engine, e.g. redb/fjall/sled)** | Native Rust, clean shutdown, settable allocator | Still an LSM: overkill for a RAM-sized, point-access, HLC-reconciled set; new dependency with its own black boxes; same migration risk as native | Rejected — replaces one opaque engine with another while keeping the mismatch between LSM design and the actual workload |
| **Native memory-first store with project WAL (Phase 2)** | Matches the workload exactly; reuses proven WAL machinery; kills SIGABRT/allocator/RSS/mlock costs; byte-bounded like the rest of the project; inline payloads get an explicit durable home (Option A in-RAM bounded by threshold×count, or Option B value-log spill) | New correctness surface for the system's source of truth — including inline blob payloads that the segment WAL's replay never had to restore; subtle crash-recovery details must be designed and tested; real effort | **Deferred, not rejected.** The most defensible replacement if Phase 0/1 evidence warrants it; shadow-mode migration makes it a measured option |
| **Precise block-cache `mlock` via C FFI extension** | Restores the swap-defense feature properly | New unsafe FFI surface; only fixes one symptom (pinning), not the structural costs | Rejected for now — a stopgap for one symptom; revisit only if Phase 2 is not pursued |
| **Route metadata through the existing segment WAL instead of RocksDB** | Reuses WAL group commit directly | Object metadata has different lifecycle than segment data (LWW updates, tombstones, prefix scans); would entangle two durability domains; inline payloads would need a new replay path anyway (the segment WAL skips them today) | Rejected — the native store (Phase 2) already subsumes this idea cleanly |

## References

- `crates/oceanfs-storage/src/metadata/store.rs` — `RocksDbMetadataStore`
  (three CFs, `batch_write`, `mlock_block_cache` → `MCL_CURRENT` fix,
  2026-08-15)
- `crates/oceanfs-core/src/types/config.rs` — `SegmentSizeConfig`,
  `inline_threshold_bytes` (default 4096), `SizeTier::Inline`
- `crates/oceanfs-server/src/write/coordinator.rs` — Inline arm stores
  `ObjectMetadata.inline_data` (blob payloads ≤ threshold) via
  `put_object` into RocksDB
- `crates/oceanfs-storage/src/wal/replay.rs` — Inline-tier entries
  skipped during replay (lines 60, 105-106): inline-blob durability is
  RocksDB's, not the segment WAL's
- `crates/oceanfs-storage/src/wal/{sync,writer,replay}.rs` — the
  project-owned WAL machinery a native store would reuse
  (`WalSyncGroup`, group-commit fsync, `replay_wal`)
- `crates/oceanfs-storage/src/buffer_pool.rs` — byte-bounded size-class
  discipline the native store would follow
- `crates/oceanfs-core/src/config/metadata.rs` — `MetadataConfig`
  (block cache, memtable, `mlock_block_cache` doc — records the
  `MCL_FUTURE` rationale)
- PIPELINE.md §4.6 — RocksDB SIGABRT / `--test-threads=1` caveat
- `docs/features/performance-optimization/seal-pipeline-batching/feature.md`
  — the 2026-08-15 work that surfaced the RSS/shutdown/`mlock` costs
- `docs/features/performance-optimization/metadata-io-off-async-workers/feature.md`
  — the spawn_blocking adapter that mitigates blocking-call costs
  while RocksDB remains
- ADR-0009 (storage-crate split) — established the `MetadataStore`
  trait boundary that keeps a replacement in-scope of one crate
- ADR-0018 (durability WAL consolidation) — the WAL as the project's
  durability backbone; pre-positions the native store's recovery story
- ADR-0021 (seal-window data set) — removal-only-after-durable
  discipline the native store's WAL replay must preserve
