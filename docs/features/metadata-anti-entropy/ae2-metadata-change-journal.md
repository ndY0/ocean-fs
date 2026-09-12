---
feature: "Metadata Change Journal & Pull-Based Catch-Up"
epic: "metadata-anti-entropy"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: fleet-degradation/f0-hints-durability-gate
    reason: f0 is the ack boundary this feature complements; journal J1 mirrors f0's fail-closed admission discipline
  - feature: fleet-degradation/f4-pool-degradation-under-load
    reason: P4 Pass A records the residual baseline; Pass B is this feature's acceptance harness (f4 is paused; the fleet resumes only on the user's go-ahead)
  - feature: ae1-debt-hardening-loss-signal
    reason: Ordering only (S1 first, independent of the journal); S1 removes the last silent hint-loss path, and the journal is the convergence complement
adr:
  - 0038-metadata-change-journal
  - 0023-metadata-store-native-replacement-path
  - 0034-bounded-metadata-accounting
  - 0027-hinted-handoff-ownership-model
  - 0028-membership-plane-full-swim-gossip
  - 0030-re-replication-target-pull
  - 0033-manifest-aware-peer-selection
  - 0017-durability-task-abstraction
  - 0025-segment-lifecycle-state-machine
  - 0035-replicated-segment-lifecycle-state
perf:
  - "3.4 group commit for the journal fsync (one fsync per batch; never a per-write fsync)"
  - "3.1 journal files are append-only (no seek/overwrite; trim unlinks whole files)"
  - "1.1 BytesMut for journal append buffers and fetched row payloads"
  - "1.3 pre-size per-cycle batch buffers with known bounded capacity"
  - "4.4 FetchMetadataRows is server-streaming (never a single multi-MB unary payload)"
  - "8.5 semaphore bounds max_inflight_pulls (no unbounded catch-up fan-out)"
  - "11.1 atomic counters/gauges for the journal and sync metrics (nothing lock-guarded on the write path)"
created: 2026-09-12
updated: 2026-09-12
---

# Metadata Change Journal & Pull-Based Catch-Up

> **Design:** [ADR-0038](../../adr/0038-metadata-change-journal.md) — ratified
> by the user 2026-09-12 (all open decisions aligned; the ADR file's status
> flip is an upstream edit). This spec **encodes** the decision; it does not
> re-decide it. The old rotating-scan spec is
> [superseded](pr3-metadata-anti-entropy.md). Bootstrap, the range iterator,
> the read-trigger, and the spot-check are **S3/S4 follow-ups**, explicitly
> outside this feature's DoD.

## Summary

Build the **change edge** of index-plane convergence: every logical metadata
mutation a node applies appends a tiny `{seq, op, key, hlc}` entry to a
node-local, co-durable, trimmable journal; peers consume journals through
durable per-peer watermarks; repair **point-fetches the key's current state**
(never replays row bodies from the journal) and applies it with the
mandatory HLC-LWW guards. Detection cost is ∝ changes applied, never ∝ rows
stored, and there is **no recurring scan anywhere** in this feature.

The work spans `oceanfs-storage` (journal file set + capture hook at the
store mutation choke point), `oceanfs-durability` (watermark store, Tier-1
`metadata_sync` worker, `FetchJournal`/`FetchMetadataRows` handlers),
`oceanfs-core` (config + metrics), and `oceanfs-node` (wiring only). The
journal lives at the pinned location
`<metadata_pool_root>/metadata-journal/` — same pool/device as the metadata
store, **not** the hints pool, **not** a RocksDB CF/WAL. It is inert by
default (`[metadata_sync] enabled = false`): no journal file is opened, no
worker runs, no handler serves, no metric registers.

## Ratified decisions encoded here

| # | Decision (2026-09-12) | Where encoded |
|---|---|---|
| 1 | **J1 fail-closed**: reject a metadata write whose journal entry cannot be recorded; same durability class as the row (group commit, no extra per-write fsync); a gap marker only for power-loss windows; mirrors f0 | §J1/co-durability, Data Flow, DoD |
| 2 | **Enable-time bootstrap required** (triggered, range-bounded, key-ordered transfer of owned ranges heals pre-enable divergence; f4 Pass A creates it before Pass B) | §S3/S4 boundaries — follow-up, **not S2 DoD** |
| 3 | **Bootstrap pacing**: Tier-1 for catch-up; node-gating only for the empty-store boot rebuild (today's g8 semantics) | §S3/S4 boundaries — follow-up |
| 4 | **ADR-0027 D5 clarification**: foreign chunk refs are normal; **no local-segment-presence precondition anywhere** | §Apply semantics; ADR-0027 doc follow-up recorded (epic OQ c) |
| 5 | **O1 fingerprint CF** stays the documented, unwired fallback | §Out of scope |
| 6 | **Spot-check key provisioning** deferred to S4 | §S3/S4 boundaries, Out of scope |
| 7 | **Journal location pinned** at `<metadata_pool_root>/metadata-journal/` (segmented files + manifest; same pool; whole-segment trim; 256 MiB / 7 d cap; counted against pool capacity; replaced with the pool → new epoch) | §Journal storage (note: supersedes the ADR's illustrative `metadata_sync/journal/` path at implementation) |

## Scope

### In Scope

- **Journal storage and format** (pinned): segmented append-only files + a
  small manifest under `<metadata_pool_root>/metadata-journal/`; entry
  `{seq: u64, op: u8 (1=put, 2=delete), key: bytes, hlc}` — no value bytes,
  no chunk refs, no size/hash. Files rotate at a fixed internal size (64 MiB
  per ADR-0038 D2), carry a `{magic, version, node_id, epoch, first_seq}`
  header, and records are length-prefixed + trailer-checksummed so a torn
  tail is truncatable at recovery. The manifest holds epoch/generation, base
  seq, and the active segment; it is written atomically (temp + rename).
- **Capture points at the store mutation choke point** — not in callers —
  covering: client PUT/overwrite (`put_object_in_bucket`), DELETE
  (`delete_object`), hint apply (`apply_hinted_object` → `put_object`), g8 /
  bootstrap row apply (`rebuild_apply_object_row` /
  `rebuild_apply_deletion_row`), and compaction/healing remaps (`batch_write`)
  **iff** `(size, blake3_hash, inline_data, hlc)` changes (the two current
  callers preserve HLC and only re-point refs, so they do not journal; a
  defensive read-before-write is required if a future caller can change
  logical identity). Supersede records, `delete_dead_chunk_record`, and all
  GC accounting are **never** journaled.
- **Co-durability and ordering (J1).** For a mutation batch the journal
  entry (or entries) is appended and made durable via **group commit**
  *before* the metadata row write is made visible. A mutation whose journal
  append fails is **fail-closed**: the row is not committed, the caller gets
  an error, and `oceanfs_metadata_journal_append_errors_total` increments.
  There is no per-write fsync beyond the group's barrier. The J2 crash
  window (entry durable, row absent) is a consumed no-op; the invariant is
  *if the row survived on a node, its change record survived on that node*.
  A **gap marker is recorded only for power-loss windows** (e.g. a torn tail
  boundary) so peers treat that range as a gap (bootstrap trigger, S3)
  rather than silently skipping it.
- **Epoch.** A random 128-bit `journal_epoch` is created with the journal
  directory and persisted in the manifest and every file header. A metadata
  pool replacement or journal loss starts a new epoch. Watermarks name
  `(epoch, seq)`; an epoch change invalidates a peer's prior watermarks
  (gap semantics).
- **Trim/retention.** `frontier = min(acked[P])` over peers **retained in
  the membership topology** (Alive/Suspect/Dead — ADR-0027 D1 / ADR-0028;
  only `Left`/removed peers leave the frontier), excluding self. Files
  entirely below the frontier are unlinked and the directory is fsync'd.
  Hard caps `journal_max_bytes` (256 MiB) and `journal_max_age_secs` (7 d)
  override the frontier; when a cap bites, the journal trims past the
  lagging peer's watermark and records the new `oldest_seq` — that peer
  receives a `gap` on its next pull.
- **Per-peer durable watermarks.** `consumed(P)` (highest seq of P's journal
  applied here) and `acked(P)` (highest seq of this node's journal that P
  confirmed). Persisted at `<metadata_pool_root>/metadata_sync/watermarks/<peer>.wm`
  via atomic temp+rename, lazily flushed; a lost advance only re-consumes
  entries (idempotent).
- **Journal pull exchange.** `FetchJournal(epoch, from_seq, max_entries,
  max_bytes, requester_id) → (epoch, oldest_seq, entries[], next_seq)`;
  `from_seq < oldest_seq` or a foreign epoch with prior progress returns a
  **gap**. Watermarks exchange both directions opportunistically: every pull
  carries the requester's `consumed` of the responder (acks), and the
  responder replies with its `consumed` of the requester.
- **Tier-1 `metadata_sync` worker** (ADR-0017): one node-local worker owned
  by `oceanfs-durability`, wired by the composition root; `DurabilityTask`
  named `metadata_sync` with `keyspace_fraction() == 1.0` (its unit is the
  journal, not CF ranges); one Tier-1 `acquire_housekeeping` permit per cycle,
  released between cycles. Triggers: periodic (`interval_sec`) plus a peer
  becoming Alive (membership event, ADR-0028) plus an observed peer epoch
  change. Per peer per cycle: pull ≤ `max_entries_per_cycle` entries /
  `max_bytes_per_cycle`; filter entries to keys this node **currently**
  co-owns (re-checked at apply time against the live ring snapshot); take the
  **latest** ordered entry per key; coalesce; point-fetch current state from
  one manifest-healthy co-holder (ADR-0033 D1); apply via HLC-LWW; advance
  `consumed(P)`. `max_inflight_pulls` bounds fetch concurrency. The worker
  **never pushes**. Ownership is never cached or mutated, so a ring change
  mid-cycle cannot produce a false repair; non-owned keys are skipped
  (`keys_skipped_total{not_owner}`), already-current keys are skipped
  (`already_current`). Missing rows inherit the existing reconcile/G4
  urgency when under-replication is known; missing **bytes** ride the Tier-0
  ADR-0030 worker unchanged. A gap increments
  `oceanfs_metadata_sync_gap_detected_total` and enqueues a bootstrap (the
  S3 consumer; S2 records the signal).
- **Point fetch of current row state.** New `FetchMetadataRows(keys[]) →
  stream MetadataRow` on `HealingRpc`, generalizing the `FetchHintObject`
  current-state pattern (`healing.proto:83-87,248-266`) and reusing
  `MetadataRow` (`:386-406`). A holder returns the key's current stored state
  (object row or plain tombstone; supersedes excluded); disabled handlers
  return `unavailable`.
- **HLC-LWW idempotent apply.** Logical identity is `(size, blake3_hash,
  inline_data, HLC)`; chunk refs are **not** identity. Three states are
  distinct: `(no row)` ≠ `(object row)` ≠ `(plain tombstone)`. HLC-LWW is
  mandatory; equal HLC with different logical identity is broken
  deterministically by max identity hash, applied symmetrically on both
  sides. A fetched inline row applies directly; a fetched tombstone applies
  through the local-aware path (delete the local live row, preserve the
  pulled HLC/deletion version, write the tombstone); a re-PUT clears a local
  tombstone through the same LWW rules. Supersede records never travel.
  Re-applying current state is a no-op; entries are consumed once per
  `(epoch, seq)`; a watermark never moves backwards within an epoch.
  **No local-segment-presence precondition** (ratified): a repaired row is
  applied even when it references segments this node does not hold — rows
  and bytes live on independent rings, foreign chunk refs are normal and
  resolve at read time (g6/ADR-0033 failover). Bytes are never repaired
  here. This is the deliberate refinement of ADR-0027 D5 (pull-only,
  version-guarded, logical-identity-scoped), recorded as a doc follow-up for
  the ADR owner.
- **Kill-switch and config, exact names** (ADR-0038 D8): full section below.
- **Metrics** (ADR-0038 D9 subset for this stage): full table below;
  registered only when enabled.
- **ADR-0034 accounting statement** (ADR-0038 D10): full statement below.
- **Tests:** unit + in-process multi-node (below), plus the gated f4 Pass B
  acceptance.
- **Docs:** config, journal format, watermark files, RPC semantics, and
  metrics; `# Examples` on new/changed `pub` items.

### Out of Scope (for this feature)

- **S3 (ownership edge):** the `visit_rows_range` store API, triggered
  bootstrap (new-owner / gap / pool-loss / **enable-time**), persisted
  bootstrap cursors, the g8 lister replacement, and the read-miss
  read-trigger (single-flight + negative cache). Gap detection in S2 emits
  the signal; the bootstrap consumer lands with S3.
- **S4:** the HMAC spot-check verifier and mixed-mesh/epoch/fault-injection
  hardening beyond the S2 kill-switch tests; spot-check key provisioning is
  deferred to S4 (ratified).
- **O1 fingerprint CF** — documented, unwired fallback only.
- Segment-plane reconciliation/holder-set repair/EC reconstruction; any
  repair of **bytes**; read-path push repair and debt-replay repair;
  C2b proactive row migration; repair of extra (non-owner) stale rows.
- Changing the hint delivery contract (ADR-0027 as amended) or the f0 gate;
  hint mirroring (S1 keeps it rejected).
- Any recurring audit/sampling scan of the keyspace, and any new RocksDB
  CF/WAL coupling.
- Test-only hooks; fleet provisioning before the user approves the f4
  resume (PIPELINE §7).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New `crates/oceanfs-core/src/config/metadata_sync.rs` (`MetadataSyncConfig`, serde defaults, validation, `# Examples`), re-exported from `oceanfs_core`; `NodeConfig` gains `#[serde(default)] pub metadata_sync: crate::MetadataSyncConfig` beside `anti_entropy`/`durability`; metric registration/constants. |
| `oceanfs-storage` | New `src/metadata/journal.rs` (file set, manifest, rotation, recovery, group commit, trim) + capture hooks at the `RocksDbMetadataStore` mutation choke points (`metadata/store.rs`); no new CF, no `MetadataStore` trait change in S2. |
| `oceanfs-durability` | New `src/metadata_sync/` (watermark store, Tier-1 worker, pacing/ownership filter, bootstrap-enqueue signal) + scheduler adaptor (`src/scheduler/adaptors.rs` pattern); `HealingGrpcService` handlers for `FetchJournal` / `FetchMetadataRows`; proto messages/RPCs in `proto/oceanfs/healing.proto`. |
| `oceanfs-node` | Wiring only: construct/open the journal and register the worker **only when enabled**; manifest-aware peer selection (ADR-0033); metrics registration; journal/watermark paths from the metadata pool root. |
| `oceanfs-server` | No change planned — the HLC-LWW/tombstone guards of `PutObjectMetadata` (`src/grpc/segment_service.rs:655-803`) and `rebuild_apply_*` are reused. |
| `proto` | `FetchJournal` request/response + `JournalEntry`; `FetchMetadataRows` reusing `MetadataRow` (counted under `oceanfs-durability`). |
| `e2e` / `scripts` | No harness change — Pass B flips `[metadata_sync] enabled` in the SUT configs and restarts the service. |

## Interface (Public API)

### Config — exact names and defaults (ADR-0038 D8)

```toml
[metadata_sync]
enabled = false                  # kill-switch (default OFF)
interval_sec = 10                # freshness knob: Tier-1 sync cycle cadence
max_entries_per_cycle = 4096     # journal entries consumed per cycle (all peers)
max_bytes_per_cycle = 4194304    # pulled row bytes per cycle
max_inflight_pulls = 2           # bounded catch-up fetch concurrency
journal_max_bytes = 268435456    # hard journal cap (256 MiB) — trim floor
journal_max_age_secs = 604800    # hard age cap (7 d) — trim floor
spot_check_keys_per_cycle = 16   # S4-reserved (accepted, inert in S2)
read_trigger_enabled = true      # S3-reserved (accepted, inert in S2)
bootstrap_batch_keys = 1024      # S3-reserved (accepted, inert in S2)
```

- `pub struct MetadataSyncConfig` in `crates/oceanfs-core/src/config/metadata_sync.rs`
  (same pattern as `config/durability.rs`), re-exported from `oceanfs_core`;
  serde defaults equal the values above; validation rejects invalid
  combinations (`interval_sec == 0` while enabled, zero caps, etc.) with the
  crate's config error type. Documented with `# Examples`.
- The last three keys are the S3/S4 knobs; S2 parses and documents them as
  **reserved/inert** (or omits them — implementer OQ; see below). They must
  not silently change behavior.
- `enabled = false`: the composition root does not construct the journal or
  the worker; no journal file is opened, read, or written (the store's
  journal handle is `None` — a branch on an `Option`, no I/O); no sweep
  runs; the fetch handlers return `unavailable`; no metrics register. Zero
  behavior change.
- `enabled = true`: the journal opens on the metadata pool, capture turns on
  at the store boundary, the Tier-1 worker registers, handlers serve, and
  metrics register.
- **Mixed-version / mixed-kill-switch meshes:** enabled peers skip disabled
  peers (a disabled peer publishes no epoch/watermark and is not expected to
  consume); disabled peers answer `unavailable`. A later enable starts a new
  epoch; peers treat it as a gap and bootstrap where needed. No correctness
  claim is made while a quorum of the ring is disabled — convergence is best
  effort, exactly like today.

### Proto (on `HealingRpc`; `MetadataRow` reused)

```proto
message JournalEntry {
  uint64 seq = 1;                       // per-node monotonic, assigned at append
  uint32 op  = 2;                       // 1 = put (live state), 2 = delete (plain tombstone)
  bytes  key = 3;                       // full store key {bucket}\0{key}
  oceanfs.common.HlcTimestamp hlc = 4;  // the logical version the apply carried
}
message FetchJournalRequest {
  bytes  epoch = 1;                     // requester's view of the responder's epoch
  uint64 from_seq = 2;                  // exclusive resume point (consumed(P))
  uint32 max_entries = 3;
  uint64 max_bytes = 4;
  bytes  requester_id = 5;              // responder records acked(requester)
}
message FetchJournalResponse {
  bytes  epoch = 1;                     // responder's current epoch
  uint64 oldest_seq = 2;                // trim frontier; from_seq < oldest_seq => gap
  repeated JournalEntry entries = 3;
  uint64 next_seq = 4;                  // exclusive resume point for the next pull
}
rpc FetchJournal(FetchJournalRequest) returns (FetchJournalResponse);
rpc FetchMetadataRows(MetadataRowsRequest) returns (stream MetadataRow);
// MetadataRowsRequest { repeated bytes keys = 1; } bounded by the cycle budget;
// MetadataRow is the existing message (healing.proto:386-406).
```

A `gap` is returned when `from_seq < oldest_seq` or the epoch is foreign with
prior progress (empty entries); the requester records
`gap_detected_total` and enqueues a bootstrap (S3 consumer).

### Rust surface (sketch; names finalized at implementation)

- `oceanfs_storage::metadata::Journal` (or `MetadataJournal`) — open/recover,
  `append(entries) -> Result<()>` (group-committed), `trim(frontier)`,
  `read_range(from_seq, max_entries, max_bytes)`, `epoch()`, `oldest_seq()`,
  `on_disk_bytes()`.
- `oceanfs_durability::metadata_sync::MetadataSync` — the Tier-1 worker;
  task name `metadata_sync`, `keyspace_fraction() == 1.0`.
- `oceanfs_durability::metadata_sync::WatermarkStore` — per-peer
  `consumed`/`acked` persistence (atomic rename, lazy flush).
- Captured at the store boundary: `RocksDbMetadataStore` gains an optional
  journal handle; no `MetadataStore` trait change in S2.

### Metrics (ADR-0038 D9 subset; registered only when enabled)

| Metric | Type | Meaning |
|---|---|---|
| `oceanfs_metadata_journal_appended_total` | counter | change records appended |
| `oceanfs_metadata_journal_bytes` | gauge | on-disk journal size |
| `oceanfs_metadata_journal_append_errors_total` | counter | append/fsync failures (write fails closed) |
| `oceanfs_metadata_journal_trim_seq` | gauge | lowest replayable seq (trim frontier) |
| `oceanfs_metadata_sync_entries_consumed_total` | counter | journal entries consumed |
| `oceanfs_metadata_sync_keys_skipped_total{reason}` | counter | `not_owner`, `already_current` |
| `oceanfs_metadata_sync_rows_pulled_total` | counter | point-fetch current-state rows |
| `oceanfs_metadata_sync_rows_applied_total` | counter | rows applied through the LWW guard |
| `oceanfs_metadata_sync_rows_rejected_total{reason}` | counter | `lww`, `tombstone`, `hash_mismatch` |
| `oceanfs_metadata_sync_watermark_lag_seconds{peer}` | gauge | age of the oldest unconsumed entry per peer |
| `oceanfs_metadata_sync_gap_detected_total` | counter | trims/epoch changes forcing bootstrap |

S1's `hinted_handoff_pending_debt` / `oceanfs_repair_unrecoverable_total` are
independent and unaffected. Bootstrap/read-trigger/spot-check series land
with S3/S4.

### ADR-0034 accounting statement (ADR-0038 D10 — explicit exception, argued)

**This is an explicit exception to "no new persisted surface", not a new
per-row index.**

- **Disk:** the journal is O(changes within the retention window) ≤
  `journal_max_bytes` (256 MiB default) and trims toward zero; it is **not**
  ∝ rows, carries **no row bodies**, and dies with the metadata pool whose
  fate it shares. It is not a column family, not a derived per-key
  structure, and not RocksDB WAL internals. It is counted against the
  metadata pool's capacity.
- **Write path:** one small append + group-commit fsync per logical mutation
  batch — the only new hot-path cost, bounded and amortized, incurred only
  when enabled. Strictly smaller than O1's per-mutation CF row + disk ∝ rows.
- **Read path:** bounded point reads and journal reads; no recurring CF
  traversal anywhere in this feature. (S3's triggered full-ownership walk is
  the one scan-class operation in the full design, gated by a trigger and
  resumable.)
- **Memory:** fixed journal buffers + per-peer watermarks O(N) + bounded
  fetch buffers; no per-row state.
- **ADR-0023:** the journal sits at the store-mutation boundary; the native
  store can provide it without inheriting RocksDB (the range API is S3).

## Data Flow

```
write path (enabled; store choke point)
  client PUT / hinted apply / DELETE / g8-bootstrap apply / batch_write(identity-changed)
    → journal.append({seq, op, key, hlc})  [group commit]
        append/fsync fails → FAIL CLOSED: row not committed, caller error,
                             oceanfs_metadata_journal_append_errors_total++
        durable            → row commit (same durability class) → ack
    (crash after journal fsync, before row commit → consumer fetches current
     state; a key absent everywhere is a consumed no-op — J2)
    (power-loss window → gap marker; peers see a gap → bootstrap trigger [S3])

sync cycle (Tier-1 permit; only when metadata_sync.enabled)
  triggers: interval_sec | peer became Alive | peer epoch change
  per peer P:
    FetchJournal(epoch, from_seq = consumed(P), …)
      gap (from_seq < oldest_seq / foreign epoch) → gap_detected_total++
                                                    + bootstrap enqueue (S3)
      entries → keep latest per key, coalesce, filter to currently co-owned keys
                (not_owner / already_current skip counters)
      point fetch: FetchMetadataRows(keys) → stream MetadataRow (manifest-healthy
                   co-holder, ADR-0033 D1; max_inflight_pulls semaphore)
      apply HLC-LWW (logical identity; tombstone-safe; no local-presence
                     precondition; supersedes never travel)
      advance consumed(P) durably (atomic rename)
      acks: requester's consumed(responder) rides the pull → acked(P) advances
  trim: files below min(acked[retained peers]) unlinked + dir fsync; caps override
        → oldest_seq advances → lagging peer gets a gap

kill-switch: enabled = false
  no journal constructed/opened/written; no worker; handlers unavailable;
  no metrics; behavior identical to today
```

## Implementation Plan (by crate/module)

1. **`oceanfs-core` — config + metrics.** `config/metadata_sync.rs`
   (`MetadataSyncConfig`, defaults, validation, docs), re-export, `NodeConfig`
   field; metric constants.
2. **`oceanfs-storage` — journal.** `metadata/journal.rs`: segmented files,
   manifest, header/record encoding, recovery (torn-tail truncation), group
   commit, epoch, trim; capture hook at the store mutation choke points with
   the defensive `batch_write` identity compare.
3. **`oceanfs-durability` — watermarks + worker.** `metadata_sync/`:
   watermark store, per-peer pull loop, ownership filter, coalescing, point
   fetch client, LWW apply integration, Tier-1 scheduler adaptor
   (`metadata_sync`, `keyspace_fraction() == 1.0`), gap signal.
4. **`oceanfs-durability` — protocol.** `FetchJournal` /
   `FetchMetadataRows` on `HealingGrpcService`, proto messages, disabled =
   `unavailable`.
5. **`oceanfs-node` — wiring.** Open/construct only when enabled; peer
   selection; metrics registration; journal/watermark paths from the
   metadata pool root.
6. **Tests + fixtures** per crate (see Test Plan); no production code for
   tests.

## Test Plan

### Unit (in-crate)

- **Journal format/recovery:** entry round-trip; manifest/header parse;
  rotation boundaries; torn tail truncates and discards only an un-fsynced
  tail; epoch creation and persistence; `oldest_seq`/trim frontier math.
- **Capture completeness matrix:** every enumerated path journals
  (`put_object_in_bucket`, `delete_object`, hint apply, g8/bootstrap apply);
  supersede/GC accounting never journals; `batch_write` journals iff logical
  identity changes (fault-injected caller) and not when it only re-points
  refs.
- **J1/co-durability:** append failure → row not committed, error returned,
  counter increments; group commit batches concurrent append waiters; J2
  crash window (journal durable, row absent) is consumed as a no-op; power-
  loss gap marker is recorded and surfaced as a gap.
- **Watermarks:** atomic temp+rename; a lost advance re-consumes entries
  (idempotent); never moves backwards within an epoch; epoch change
  invalidates; bidirectional ack advance.
- **Trim safety:** frontier = min(acked) over retained peers; a
  retained-Dead (stuck) peer holds the frontier; caps override and record
  `oldest_seq` → gap; whole-file deletion only below the frontier.
- **Ownership filter:** `not_owner` and `already_current` skips; live-ring
  re-check at apply; ring change mid-cycle produces no false repair.
- **Apply semantics:** three states; chunk-ref differences are identity-
  equal; HLC-LWW; equal-HLC tie-break (max identity hash) converges
  identically from both sides and does not oscillate; tombstone apply is
  local-aware and never resurrects; re-PUT clears a local tombstone;
  foreign-ref row applies with no local segment.
- **Kill-switch parity:** `enabled = false` → no journal I/O, no worker, no
  RPC traffic, no metric series.

### In-process multi-node (crate integration)

- **Missed row heals without any scan:** remove one node's row, run sync
  cycles, and read back the row from the co-holder (assert the journal path
  is the source; no CF traversal).
- **Missed DELETE converges to the tombstone** — the stale live row is never
  "repaired" back (no resurrection).
- **Crash between journal fsync and row commit** across every capture point:
  recovery is consistent, no acked write is lost, the orphan entry is a
  no-op.
- **Trim-vs-watermark race:** a slow peer's watermark is honored until a cap
  bites; then `gap_detected_total` fires and the bounded bootstrap signal is
  enqueued (S3 consumer stubbed in S2 tests).
- **Mixed kill-switch mesh:** enabled nodes converge among themselves;
  disabled nodes are skipped; re-enabling starts a new epoch → gap; no
  storm.
- **Pre-enable divergence is NOT healed by S2:** a row lost before
  enablement stays divergent until the S3 bootstrap — recorded explicitly as
  the boundary (mirrors the f4 Pass A/Pass B mapping).

### Cloud acceptance — f4 P4 Pass B (gated)

- **Precondition:** the user approves the paused f4 resume (PIPELINE §7); the
  fleet is provisioned only then. Same-session protocol: Pass A (sync off)
  records the residual baseline; Pass B enables `[metadata_sync]` on all SUT
  configs and restarts the service.
- **Assertions (post-enable divergence, ADR-0038 S2):** divergence created
  after enablement is detected (`oceanfs_metadata_sync_*` counters/gauges
  move), rows are pulled/applied, RF coverage is restored, the missed
  DELETE converges to the tombstone, per-cycle budgets are respected, and
  there is no bounded-discipline regression. The pre-enable residual is
  expected to remain until S3's enable-time bootstrap and is recorded as
  such.
- Correctness only — no throughput/latency assertion (volume-backed fleet
  non-comparability rule). No load suite on the dev machine (PIPELINE §6).

## Definition of Done

- [ ] **Code (build):** `cargo build --all-targets` succeeds in the affected
      crates (`oceanfs-core`, `oceanfs-storage`, `oceanfs-durability`,
      `oceanfs-node`); regenerated proto stubs compile; no test-only hooks.
- [ ] **Code (config):** `[metadata_sync]` parses with the exact D8 names and
      defaults; `MetadataSyncConfig` lives in
      `crates/oceanfs-core/src/config/metadata_sync.rs`, is re-exported, and
      is on `NodeConfig` with `#[serde(default)]`; validation covers invalid
      values; S3/S4-reserved keys are documented as inert; `# Examples` on
      every `pub` item.
- [ ] **Code (journal):** pinned `<metadata_pool_root>/metadata-journal/`;
      segmented append-only files + manifest (epoch/generation, base seq,
      active segment); `{seq, op, key, hlc}` entries with no row bodies;
      fixed-size rotation; header per file; length-prefixed/checksummed
      records; torn-tail truncation at recovery; random 128-bit epoch on
      creation and after pool replacement; no CF/WAL; counted against pool
      capacity.
- [ ] **Code (capture):** every enumerated mutation path journals at the
      store choke point; supersedes/GC accounting never journal;
      `batch_write` journals iff logical identity changes (defensive
      read-before-write); the capture matrix is tested with fault injection.
- [ ] **Code (co-durability):** J1 append-before-commit with group commit (no
      per-write fsync); append failure is fail-closed (row not committed,
      error to the caller, `journal_append_errors_total` increments); the J2
      window is a consumed no-op; a power-loss gap marker is recorded only
      for power-loss windows and surfaces as a peer gap.
- [ ] **Code (trim/watermarks):** frontier = min(acked) over retained peers
      (Alive/Suspect/Dead, excluding self); whole-file deletion below the
      frontier + directory fsync; caps override → `oldest_seq` + gap;
      per-peer `consumed`/`acked` persisted atomically and lazily flushed;
      `FetchJournal` exchange with bidirectional acks; gap on
      `from_seq < oldest_seq` or foreign epoch.
- [ ] **Code (worker):** `metadata_sync` registered as a `DurabilityTask`
      with `keyspace_fraction() == 1.0`; one Tier-1 `acquire_housekeeping`
      permit per cycle released between cycles; periodic + peer-Alive +
      epoch-change triggers; per-peer cycle bounded by
      `max_entries_per_cycle` / `max_bytes_per_cycle`; latest-entry-per-key
      coalescing; ownership filter re-checked at apply; `max_inflight_pulls`
      semaphore; never pushes; gaps emit the metric and enqueue the bootstrap
      signal (S3 consumer).
- [ ] **Code (protocol):** `FetchJournal` and `FetchMetadataRows(keys[]) →
      stream MetadataRow` on `HealingRpc`; the point fetch generalizes
      `FetchHintObject` and reuses `MetadataRow`; disabled handlers return
      `unavailable`; `MerkleExchange` is not overloaded.
- [ ] **Correctness (apply):** logical identity `(size, blake3_hash,
      inline_data, HLC)`; chunk refs are not identity; **no
      local-segment-presence precondition** (foreign refs are normal; the
      ADR-0027 D5 clarification is recorded); three states distinct;
      tombstone-safe with no resurrection; HLC-LWW mandatory; equal-HLC
      tie-break by max identity hash, symmetric; idempotent per `(epoch,
      seq)`; watermark never moves backwards.
- [ ] **Correctness (kill-switch):** `enabled = false` is structurally inert —
      no journal constructed/opened/written, no worker, no RPC serving, no
      metric series — and behavior is identical to today; mixed enabled/
      disabled meshes converge with bounded catch-up (no storm); re-enable is
      a new epoch → gap.
- [ ] **Correctness (accounting):** the ADR-0034 statement above is recorded
      and holds — journal O(changes within the window) ≤ cap, not ∝ rows, no
      row bodies; one bounded group-committed append; no recurring
      traversal; fixed memory + O(N) watermarks.
- [ ] **Metrics:** the exact D9 S2 series are registered only when enabled;
      S1's series are independent; no `ae_*` segment-plane names are reused.
- [ ] **Tests (unit):** journal format/recovery/rotation/epoch, capture
      matrix, J1 fault injection + crash window, trim safety (stuck
      retained-Dead peer; cap override), watermark persistence/epoch,
      ownership filter, apply semantics, kill-switch parity.
- [ ] **Tests (in-process multi-node):** missed row and missed DELETE
      converge with no scan; no resurrection; crash between journal fsync
      and row commit; trim-vs-watermark race → gap; mixed kill-switch mesh;
      pre-enable divergence explicitly not healed by S2.
- [ ] **Tests (cloud, gated):** f4 P4 Pass B per the Test Plan — blocked
      until the user approves the paused f4 resume; correctness only.
- [ ] **Docs:** every new/changed `pub` item has `# Examples`;
      `#![deny(missing_docs)]` passes; config, journal file/manifest format,
      watermark files, RPC semantics, and metric names are documented; the
      S3/S4 follow-ups are pointed to.
- [ ] **ADR:** ADR-0038 D1/D2/D3/D4/D5/D8/D9/D10 constraints for this stage
      are addressed; the ratified decisions are encoded (table above); S3/S4
      items (bootstrap/range/read-trigger/spot-check) are explicitly
      deferred; O1 stays unwired; the ADR-0027 D5 doc follow-up is recorded.
- [ ] **Perf:** the cited rules are followed — 3.4 (group commit), 3.1
      (append-only), 1.1 (`BytesMut`), 1.3 (pre-sized batches), 4.4
      (streaming point fetch), 8.5 (bounded inflight pulls), 11.1 (atomic
      metrics); the write path adds only the bounded group-committed append.
- [ ] **Integration:** a node-level in-process multi-node scenario exercises
      journal → pull → point fetch → LWW apply end to end (missed row and
      missed DELETE), and f4 Pass B provides the fleet acceptance once
      unblocked.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

1. **Journal manifest layout and file naming.** Exact record encoding for
   `{epoch/generation, base_seq, active_segment}`, file naming under
   `metadata-journal/` (`*.jrn` per ADR-0038 D2), and whether the manifest is
   a single atomic-rename file or a small two-slot pair.
2. **Seq assignment under concurrency.** Single writer task vs atomic
   reservation; must preserve strictly increasing `seq` and correct order
   with group commit.
3. **Power-loss gap-marker representation.** How a torn tail/durability
   boundary is distinguished from an ordinary truncation; on-disk shape of
   the gap marker and how `FetchJournal` reports it (`oldest_seq` jump vs an
   explicit flag).
4. **Watermark flush policy.** Lazy cadence for `consumed`/`acked`; whether
   the piggybacked ack is persisted eagerly or batched.
5. **FetchJournal paging.** `max_entries`/`max_bytes` truncation and
   `next_seq` semantics at file boundaries.
6. **Peer selection for pulls.** Rotation policy across N peers and the
   manifest-health filter (ADR-0033 D1) — reuse the g8 lister's peer
   selection or a new one.
7. **Group-commit seam.** Where the journal fsync barrier sits relative to
   the RocksDB `WriteBatch` commit so that J1 holds without adding a
   per-write fsync (the current store commit already has a critical
   section).
8. **Reserved config keys.** Accept-and-document the three S3/S4 keys as
   inert, or omit them until their features land (recommend the former for
   config stability); record the choice.
9. **Trim mechanics.** Directory fsync on whole-file unlink; behavior when a
   journal file is externally deleted; `journal_bytes` gauge accounting.
10. **Bootstrap-enqueue seam.** The interface by which a detected gap
    enqueues work for the S3 bootstrap coordinator (an internal hook now, so
    S3 wires without rework).

## Cross-links

- Epic: [metadata-anti-entropy](epic.md).
- Design: [ADR-0038](../../adr/0038-metadata-change-journal.md); superseded
  predecessor [ADR-0037](../../adr/0037-metadata-anti-entropy-detection.md);
  old spec [pr3 (superseded)](pr3-metadata-anti-entropy.md).
- Prior stage: [ae1 debt hardening](ae1-debt-hardening-loss-signal.md).
- Acceptance harness: [f4 P4](../fleet-degradation/f4-pool-degradation-under-load.md)
  (paused; Pass B).
- Complement: [f0 gate](../fleet-degradation/f0-hints-durability-gate.md).
- Constraints/patterns: [ADR-0023](../../adr/0023-metadata-store-native-replacement-path.md),
  [ADR-0017](../../adr/0017-durability-task-abstraction.md),
  [ADR-0027](../../adr/0027-hinted-handoff-ownership-model.md),
  [ADR-0028](../../adr/0028-membership-plane-full-swim-gossip.md),
  [ADR-0030](../../adr/0030-re-replication-target-pull.md),
  [ADR-0033](../../adr/0033-manifest-aware-peer-selection.md),
  [ADR-0034](../../adr/0034-bounded-metadata-accounting.md),
  [ADR-0035](../../adr/0035-replicated-segment-lifecycle-state.md).

## Deviations (accepted)

_None yet — filled at implementation close. The pinned journal location
(`<metadata_pool_root>/metadata-journal/`) supersedes the ADR-0038 D2
illustrative `metadata_sync/journal/` path at implementation; the ADR text is
an upstream input and is not edited here._
