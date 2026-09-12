---
feature: "Metadata Anti-Entropy — Rotating Bounded Scan, Target-Pull Repair"
epic: "metadata-anti-entropy"
status: superseded
priority: high
owner: ""
dependencies:
  - feature: fleet-degradation/f0-hints-durability-gate
    reason: Complementary admission gate — f0 bounds NEW divergence (no ack without durable debt); AE heals EXISTING divergence from any cause (device loss erases recorded debt). The locked architecture is f0 + AE, not either alone.
  - feature: fleet-degradation/f4-pool-degradation-under-load
    reason: The f4 P4 rerun is this feature's acceptance harness — Pass A records the residual divergence with AE off, Pass B reruns with AE on in the same fleet session. f4 is paused and its fleet is destroyed (0 resources); resume only on the user's go-ahead.
adr:
  - 0037-metadata-anti-entropy-detection
perf:
  - "1.3 pre-size the fingerprint batch buffer (bounded by batch_keys; ≈512 × 85 B ≈ 44 KB)"
  - "4.4 server-streaming FetchMetadataRows (never a single multi-MB unary payload)"
  - "5.1 BLAKE3 for row_hash (runtime SIMD; the repo's hash convention)"
  - "8.5 semaphore bounds max_inflight_compares (no unbounded compare fan-out)"
created: 2026-09-12
updated: 2026-09-12
---

# Metadata Anti-Entropy — Rotating Bounded Scan, Target-Pull Repair

> **SUPERSEDED (2026-09-12).** This spec encodes the rejected ADR-0037 O3
> rotating-scan design. The ratified design is
> [ADR-0038 — Metadata Convergence: Co-Durable Change Journal with
> Pull-Based Catch-Up](../../adr/0038-metadata-change-journal.md) (user
> aligned 2026-09-12; supersedes ADR-0037). The rewritten feature set is:
> [epic](epic.md),
> [ae1 — hint-debt hardening & segment unrecoverable signal](ae1-debt-hardening-loss-signal.md),
> and [ae2 — metadata change journal & pull-based catch-up](ae2-metadata-change-journal.md);
> bootstrap/read-trigger (S3) and spot-check (S4) are epic rows.
>
> **The body below is kept intact as the historical O3 record. Do not
> implement it; its recurring full-metadata scan is rejected by the governing
> performance principle.**

> **PRECONDITION — blocked on ADR-0037 acceptance (user decision).**
> ADR-0037 is `proposed`, not accepted. This spec is written against the
> ADR's **recommended Option 3** (a durable rotating bounded scan cursor
> over the metadata CFs, comparing key-ordered fingerprint batches per
> co-owned key, repairing by target-pull). **If a different option is
> chosen**, apply
> [ADR-0037 § "If a Different Option Is Chosen — Feature-Spec Deltas"](../../adr/0037-metadata-anti-entropy-detection.md#if-a-different-option-is-chosen--feature-spec-deltas)
> and revise this document before implementation. This spec does **not**
> re-decide the ADR; it transcribes D1–D10 into buildable form.
>
> **Additional block:** the cloud acceptance (f4 P4 Pass B) is blocked until
> the user approves the paused f4 resume — no fleet is provisioned before
> that (PIPELINE §7). The final coverage bound is left unasserted until the
> ADR is accepted and Pass A data exists (see [Test Plan](#test-plan)).

## Summary

The index plane has no eventual-repair path: hinted handoff is the only
metadata-**row** backfill, read repair delegates the remote-newer case to it,
and segment reconciliation restores **bytes** by holder set without knowing
which rows reference them. Once a row is missing and its hint is gone,
nothing re-derives it — a cold key can stay under-replicated forever. The
verified gap, its evidence, and the acceptance placeholders are recorded in
the [design draft](design-draft.md); the mechanism is decided in
[ADR-0037](../../adr/0037-metadata-anti-entropy-detection.md) (recommended
Option 3).

This feature builds the worker, the cursor, the two new comparison RPCs, and
the target-pull apply path. A node-local `MetadataAntiEntropy` worker
(`oceanfs-durability`, wired by `oceanfs-node`) walks the objects CF then the
deletions CF in raw `{bucket}\0{key}` order with a durable cursor, accumulates
up to `batch_keys` locally-held rows per batch, fingerprints each row, and
compares the batch with a manifest-healthy co-holder of those keys. Where the
local side is behind it pulls the rows (`FetchMetadataRows`) and applies them
through the mutation choke point with HLC-LWW/tombstone/capture-preserving
semantics; where the peer is behind, the peer pulls on its own rotation.
Missing bytes are never invented: a segment-stored row is applied only when
every referenced segment is locally present, otherwise the missing segments
are handed to the existing ADR-0030 re-replication worker and the row is
re-detected next rotation. The mechanism adds **no new persisted surface** —
a ~40 B cursor is the only artifact — and is inert by default
(`[metadata_ae] enabled = false`).

## Scope

### In Scope

The ADR's decisions D1–D10, as requirements:

- **D1 — Mechanism.** One node-local worker, `MetadataAntiEntropy`, owned by
  `oceanfs-durability` and wired by the composition root. Each cycle: resume
  the cursor at `(phase, last_key)` where phase `objects` walks the objects
  CF and phase `deletions` walks the deletions CF in raw RocksDB key order
  (`{bucket}\0{key}`, not hash order); accumulate up to `batch_keys`
  locally-held rows; fingerprint each (D2); select, per key, one comparison
  peer from the key's current replica set (`Ring::lookup(SHA-256(bucket/key))`)
  filtered by manifest health (ADR-0033 D1) — one peer for the whole
  rotation, rotating across rotations until all RF−1 peers are covered; keys
  this node does not currently hold are not compared; one `CompareMetadata`
  RPC per batch; pull and apply differences where the local side is behind
  (D4), never push. Advance the cursor past `min(local batch end, responder
  truncation point)` and persist it; compare success advances the cursor even
  when a repair is deferred. A full pass covers every locally-held key.
- **D2 — What is compared.** Three distinguishable states: `(no row)` /
  `(object row)` / `(plain tombstone row)`; absence is never offered as an
  entry (the protocol reports it), and `(no row)` vs `(tombstone)` is a real
  mismatch. Object fingerprint = `state=object` + `hlc` +
  `row_hash = BLAKE3(serialized objects-CF value)` covering size, blake3
  hash, inline payload, and the chunk-ref list. Tombstone fingerprint =
  `state=tombstone` + `hlc` only (`chunks`/`deletion_time` are GC accounting,
  not logical state). Supersede records are excluded from comparison and are
  never repaired. HLC participates; equal HLC with different `row_hash` is
  broken deterministically by **max row_hash**, symmetrically on both sides.
  Keys travel in the entry (key-ordered exchange — localization is free).
- **D3 — Comparison protocol.** Add to `HealingRpc`: `CompareMetadata`
  (unary, batched, bounded) and `FetchMetadataRows` (server-streaming,
  reusing the existing `MetadataRow`). The responder answers from point gets
  for offered keys plus one bounded scan of `[start_key, end_key]` for
  extras; `max_entries` caps the response and `truncated_at` tells the
  initiator where to advance the cursor (an initiator with a large key gap
  still discovers peer-only keys at bounded cost and never stalls). Either
  side may discover it is behind; the behind side pulls. Both sides include a
  key only if the peer is in the key's current replica set, and the applying
  side re-checks co-ownership at apply time. `MerkleExchange` is **not**
  overloaded.
- **D4 — Apply semantics (bytes before rows).** Inline rows (`chunks` empty)
  apply directly. Segment-stored rows verify every `ChunkRef.segment_id` is
  locally present (lifecycle registry entry `Sealed` and resolvable by the
  local data store): if all present → apply; if any missing → do **not** apply
  the row, dispatch re-replication for the missing segment(s) through the
  existing ADR-0030 target-pull worker (`RequestReReplication`, reason
  `REPAIR_REASON_RECONCILIATION`), and remember the key in a small bounded
  in-memory `awaiting_bytes` set; when the segment lands the pending row is
  applied, and on restart the next rotation re-detects it. Plain tombstones
  apply unconditionally (no chunks needed) through a local-aware path: delete
  the local live row, capture the **local** row's chunks into the tombstone,
  preserve the pulled `deletion_time`/`hlc`, and **drop any pulled chunk refs
  for segments this node does not hold** (no foreign dead-bytes accounting).
  Apply through the mutation choke point with a capture-preserving apply
  (supersede capture on overwrite, tombstone migration on re-PUT), reusing
  the LWW/tombstone guards of `PutObjectMetadata` and `rebuild_apply_*`; the
  streamed value's `row_hash` is re-verified before apply. Bytes before rows:
  a row whose bytes are missing stays unapplied (read failover continues to
  serve from replicas) until the byte repair completes.
- **D5 — Tombstone TTL invariant.** The AE rotation period (plus worst-case
  clock skew and partition window) must be strictly less than the tombstone
  TTL (`gc.tombstone_ttl_sec`, default 259 200 s). Expose
  `oceanfs_metadata_ae_rotation_seconds`; when `metadata_ae.enabled`, validate
  at boot that `gc.tombstone_ttl_sec ≥ max_rotation_budget + skew_margin`
  (`max_rotation_budget` derived from R-independent config: budget × cycles);
  if the configured budget cannot guarantee it, **refuse to start the AE
  worker** (fail loud, leave the feature disabled) and log the mismatch.
- **D6 — Pacing and budget (Tier-1 under ADR-0017).** The worker is a
  `DurabilityTask` registered with the `DurabilityScheduler` (name
  `metadata_anti_entropy`, `keyspace_fraction() == 1.0` — it owns a finer
  cursor than the scheduler's shard rotation). Each cycle acquires one
  Tier-1 housekeeping permit (`DurabilityBudget::acquire_housekeeping`) and
  releases it between cycles. A cycle is bounded by `interval_sec`,
  `batch_keys`, `max_batches_per_cycle`, `max_bytes_per_cycle` (pulled rows
  + payload bytes), and `max_inflight_compares`. Repair urgency inherits the
  existing reconcile/G4 priority when a missing row is under-replicated;
  missing **bytes** ride the Tier-0 ADR-0030 worker and its prioritization
  unchanged. No repair storm on rejoin: the returning node compares at its
  configured cadence, other nodes' cursors encounter it on the next rotation,
  and per-cycle budgets cap how much is pulled per tick.
- **D7 — Crash safety and boot cost.** Persisted: the enabled flag (config)
  and a ~40 B cursor `(phase, last_key, rotation_index, peer_index)` written
  atomically (temp + rename) at
  `<metadata_pool_root>/metadata_ae.cursor`. Rebuilt: nothing — a
  missing/corrupt cursor restarts the rotation from the beginning (safe,
  never a correctness issue). Worst-case boot cost: **zero**; the worker
  starts after cluster readiness and resumes from the cursor. A metadata-pool
  replacement (g8) wipes the cursor with the pool; the rebuilt store starts a
  new rotation.
- **D8 — ADR-0034 accounting statement.** No new persisted metadata surface:
  no new column family, no per-row/per-key derived rows, and the only new
  persistent artifact is the constant-size ~40 B cursor. Tombstones are
  **read** (and included in comparison) but never duplicated; supersede
  records are neither compared nor repaired. Per-tick work is bounded
  (`max_batches_per_cycle × batch_keys` keys, `max_bytes_per_cycle`), and the
  rotation is a multi-cycle pass — the ADR-0017 §3 keyspace_fraction model,
  not a per-event `list_objects_all` scan.
- **D9 — Metrics.** Registered only when enabled, with the exact names in
  [Interface](#metrics-d9--registered-only-when-enabled).
- **D10 — Kill-switch and rollout.** Config section `[metadata_ae]` with
  exact names/defaults (see [Interface](#config-d10--exact-names)) in
  `crates/oceanfs-core/src/config/metadata_ae.rs`. `enabled = false` ⇒ the
  composition root does not construct/register the worker, does not spawn a
  loop, does not read/write the cursor, and the `CompareMetadata` /
  `FetchMetadataRows` handlers return `unavailable`; metrics do not register.
  Zero behavior change. `enabled = true` ⇒ the worker registers as a Tier-1
  task, the handlers serve, metrics register. Land with `enabled = false`
  before f4 resumes for the P4 two-pass protocol.

### Out of Scope (for this feature)

The ADR's out-of-scope list, verbatim in spirit:

- Segment-plane reconciliation / holder-set repair (bytes are already owned
  there).
- Read-path backfill and debt-replay repair (dropped 2026-09-11).
- C2b proactive row migration; AE may later be reused by it, but this feature
  does not decide C2b.
- Repair of **extra** (non-owner) stale rows; GC/ownership handles those.
- Sampling mode — coverage is complete per rotation by design.
- Changing the hint delivery contract (ADR-0027 as amended) or the f0 gate.
- The deferred bounded hot-set accelerator (ADR-0037 hybrid) — it can be
  added later behind the same RPCs without an ADR-level change; adopting it
  now is not in scope (Open Question 1).
- Any new admin route or manual trigger — the kill-switch is config-only
  (D10); observability is the D9 metrics and logs.
- Test-only hooks; fleet provisioning before the user approves the f4 resume.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New `crates/oceanfs-core/src/config/metadata_ae.rs` (`MetadataAeConfig`, ~150 LOC) re-exported from `oceanfs_core`; `NodeConfig` gains `#[serde(default)] pub metadata_ae: crate::MetadataAeConfig` beside `anti_entropy`/`durability` (`crates/oceanfs-core/src/config/node.rs:220-230`). |
| `oceanfs-durability` | New worker/cursor/compare client/Tier-1 adaptor (~800–1200 LOC, e.g. `src/metadata_ae/`); scheduler adaptor registration (pattern of `AeTask`, `src/scheduler/adaptors.rs:190-228`); `HealingGrpcService` handlers for the two RPCs plus the `proto/oceanfs/healing.proto` messages/RPCs (~400 LOC). Reuses `DurabilityBudget::acquire_housekeeping`, the `MetadataRangeLister` server pattern (`src/healing_service.rs:121,799`), and the reconcile repair sink. |
| `oceanfs-storage` | Classified row visit over the objects CF then the deletions CF in raw key order, plus capture-preserving AE apply through the mutation choke point (~400 LOC; `src/metadata/store.rs:529-602,1133-1178,1460-1616`, CF layout `src/metadata/cf.rs:14-16,26-56,58-148`); local-segment-presence check via the lifecycle registry / local data store. |
| `oceanfs-node` | Wiring only (~200 LOC): construct/register the worker **only when enabled**; manifest-aware peer selection (ADR-0033); pending-bytes hand-off to the ADR-0030 re-replication worker; cursor path from the metadata pool root; metrics registration. |
| `oceanfs-server` | No change planned; the HLC-LWW/tombstone guards of `PutObjectMetadata` (`src/grpc/segment_service.rs:655-803`) and `rebuild_apply_*` are reused, not modified. |
| `proto` | New messages/RPCs in `proto/oceanfs/healing.proto` (counted under `oceanfs-durability`). |
| `e2e` / `scripts` | No harness change — the f4 P4 pass driver already exists; Pass B only flips `metadata_ae.enabled` in the SUT configs and restarts the service. |

## Interface (Public API)

### Config (D10 — exact names)

```toml
[metadata_ae]
enabled = false                # kill-switch (default OFF)
interval_sec = 30              # Tier-1 cycle cadence
batch_keys = 512               # keys per compare RPC
max_batches_per_cycle = 8      # per-cycle scan budget
max_bytes_per_cycle = 4194304  # per-cycle transfer budget
max_inflight_compares = 2
```

- `pub struct MetadataAeConfig` (`oceanfs_core`, re-exported) — serde
  defaults must equal the values above; validation rejects invalid
  combinations (e.g. `batch_keys == 0`, `interval_sec == 0` while enabled)
  with the crate's config error type. Documented with `# Examples`.
- `NodeConfig::metadata_ae` with `#[serde(default)]`, parsed from
  `oceanfs.toml`; an absent section yields the defaults above.
- No admin route; no CLI flag.

### Proto (D3 — messages/RPCs)

Added to `HealingRpc` (`proto/oceanfs/healing.proto:67-128`); the service is
already implemented at `crates/oceanfs-durability/src/healing_service.rs:799`:

```proto
rpc CompareMetadata(MetadataCompareRequest) returns (MetadataCompareResponse);
rpc FetchMetadataRows(MetadataFetchRequest) returns (stream MetadataRow);
```

Illustrative message shape (the ADR notes field layout is finalized at spec
time; `MetadataRow` is the existing message at `healing.proto:386-406`):

```proto
message MetadataFingerprint {
  bytes key = 1;                      // full CF key {bucket}\0{key}
  uint32 state = 2;                   // 0 = absent marker, 1 = object, 2 = plain tombstone
  oceanfs.common.HlcTimestamp hlc = 3;
  bytes row_hash = 4;                 // BLAKE3(value); empty for tombstone/absent
}
message MetadataCompareRequest {
  bytes start_key = 1;                // inclusive cursor position
  bytes end_key = 2;                  // inclusive local batch end (empty = open)
  repeated MetadataFingerprint held = 3;  // initiator's rows in [start,end]
  uint32 max_entries = 4;             // responder work cap
}
message MetadataCompareResponse {
  repeated MetadataFingerprint differing = 1;   // responder state for offered keys that differ
  repeated MetadataFingerprint absent = 2;      // offered keys the responder does not hold
  repeated MetadataFingerprint extras = 3;      // co-owned keys the responder holds that were not offered
  bytes truncated_at = 4;             // responder-local bound when max_entries hit
}
message MetadataFetchRequest { repeated bytes keys = 1; }  // bounded by the cycle budget
```

### Worker, task, and cursor

- Worker type: `MetadataAntiEntropy`, owned by `oceanfs-durability`, wired by
  the composition root (`oceanfs-node`).
- Scheduler task name: `metadata_anti_entropy` (`DurabilityTask::name()`),
  `keyspace_fraction() == 1.0`.
- Cursor: `<metadata_pool_root>/metadata_ae.cursor`, ~40 B,
  `(phase, last_key, rotation_index, peer_index)`, phases `objects` and
  `deletions`, written atomically (temp + rename). Corrupt/missing ⇒ restart.
- Disabled node: the two RPC handlers return `unavailable` (an enabled peer
  skips the node for that rotation).

### Metrics (D9 — registered only when enabled)

| Metric | Type | Meaning |
|---|---|---|
| `oceanfs_metadata_ae_keys_compared_total` | counter | keys offered/compared |
| `oceanfs_metadata_ae_mismatches_total{state}` | counter | `object`/`tombstone`/`absent` differences found |
| `oceanfs_metadata_ae_rows_pulled_total` | counter | rows fetched for repair |
| `oceanfs_metadata_ae_rows_applied_total` | counter | rows applied through the choke point |
| `oceanfs_metadata_ae_rows_skipped_total{reason}` | counter | `lww`, `missing_segment`, `not_holder`, `hash_mismatch` |
| `oceanfs_metadata_ae_bytes_pulled_total` | counter | repair transfer volume |
| `oceanfs_metadata_ae_repair_lag_seconds` | histogram | now − pulled row HLC wall time at apply (convergence lag) |
| `oceanfs_metadata_ae_rotation_seconds` | gauge | duration of the last completed rotation |
| `oceanfs_metadata_ae_rotation_progress` | gauge | 0..1 estimated coverage of the current rotation |
| `oceanfs_metadata_ae_cursor_resets_total` | counter | cursor missing/corrupt → rotation restarted |
| `oceanfs_metadata_ae_pending_bytes_keys` | gauge | keys awaiting re-replicated bytes |

Naming follows the g8 durability convention (`oceanfs_metadata_rebuild_*`)
and avoids the segment-plane `ae_*` counters.

## Data Flow

```
cycle (Tier-1 permit; only when metadata_ae.enabled)
  phase=objects (then deletions), raw {bucket}\0{key} order
    resume cursor (phase, last_key)
    visit ≤ batch_keys locally-held rows → fingerprint each (D2)
    per key: one manifest-healthy co-holder peer (ADR-0033 D1;
             rotates per rotation until RF−1 covered)
    CompareMetadata(start_key, end_key, held[], max_entries)
      ← differing[] / absent[] / extras[] / truncated_at
    local behind  → FetchMetadataRows(keys) → stream MetadataRow
      apply (D4, bytes before rows):
        inline          → apply
        segment-stored  → all chunk segments locally present?
                            yes → apply
                            no  → RequestReReplication(ADR-0030, RECONCILIATION)
                                  + awaiting_bytes (bounded, in-memory)
                                  apply when bytes land; restart re-detects
        tombstone       → local-aware apply: capture LOCAL chunks,
                          preserve pulled deletion_time/hlc,
                          drop pulled refs for unheld segments
    peer behind   → the peer pulls on its own next rotation (never push)
    cursor.advance(min(local batch end, truncated_at)) — even if repair deferred
  wrap → rotation_index++ / peer_index rotates

f4 P4 two-pass (one fleet session, after the user approves the resume):
  Pass A (AE off)  → P4 unchanged → residual cold-key divergence recorded (baseline)
  Pass B (AE on)   → enable on all SUT configs + service restart → rerun P4
                     → assert detection, repair, RF coverage within bound,
                       no tombstone resurrection, per-cycle budget respected
```

## Implementation Plan (by crate/module)

Following the ADR's breakdown:

1. **`oceanfs-core` — config (~150 LOC).** New
   `crates/oceanfs-core/src/config/metadata_ae.rs` (pattern:
   `config/durability.rs:34-151`): `MetadataAeConfig`, serde defaults,
   validation, `# Examples`; re-export from `oceanfs_core`; add the
   `NodeConfig` field (`config/node.rs:220-230`) and default (`:732-734`
   pattern).
2. **`oceanfs-durability` — worker, cursor, compare client, Tier-1 adaptor
   (~800–1200 LOC).** Cycle driver and cursor persistence under a new
   `src/metadata_ae/` module tree; scheduler adaptor modeled on `AeTask`
   (`src/scheduler/adaptors.rs:190-228`) with `name() == "metadata_anti_entropy"`
   and `keyspace_fraction() == 1.0`; per-cycle `acquire_housekeeping` permit;
   `max_inflight_compares` semaphore; cursor file at the metadata pool root;
   metrics registration; D5 boot validation before the worker starts.
3. **`oceanfs-durability` — healing-service handlers + proto (~400 LOC).**
   `CompareMetadata` / `FetchMetadataRows` on `HealingGrpcService`
   (`src/healing_service.rs:799`), with a new node-injected handler trait
   modeled on `MetadataRangeLister` (`:121-135`); bounded responder scan and
   `max_entries`/`truncated_at`; messages/RPCs in
   `proto/oceanfs/healing.proto` (`:67-128`, `:386-406` for `MetadataRow`).
   Disabled ⇒ `unavailable`.
4. **`oceanfs-storage` — classified row visit + capture-preserving AE apply
   (~400 LOC).** Key-ordered iteration of the objects then deletions CFs
   (CF layout `src/metadata/cf.rs:14-16,26-56,58-148`), row classification
   (object / plain tombstone / supersede-excluded), and an apply entry point
   through the mutation choke point (`put_object_in_bucket`,
   `src/metadata/store.rs:529-602`) with capture preservation (supersede
   capture on overwrite, tombstone migration on re-PUT), reusing the
   `PutObjectMetadata` guards (`oceanfs-server/src/grpc/segment_service.rs:655-803`)
   and `rebuild_apply_*` (`store.rs:1460-1616`); local-segment-presence check
   against the lifecycle registry/local data store; `awaiting_bytes` support.
5. **`oceanfs-node` — wiring, peer selection, pending-bytes hand-off
   (~200 LOC).** Construct/register the worker only when enabled; cursor path
   from the metadata pool root; manifest-aware peer selection (ADR-0033);
   hand-off of missing bytes to the ADR-0030 worker; metrics registration;
   boot validation ordering. Reuses the g8 lister pattern
   (`src/modules/metadata_recovery.rs:42-89,332-379`) and the durability
   module wiring.
6. **Tests + fixtures** distributed per crate (see [Test Plan](#test-plan));
   no production code is added for tests.

## Test Plan

### Unit (in-crate)

- **Fingerprints/states:** `(no row)` / `(object row)` / `(tombstone row)`
  are distinct; the object hash covers the full logical row (size, blake3,
  inline payload, chunk-ref list); the tombstone hash is HLC-only and ignores
  chunk/deletion-time differences; supersede records are never offered.
- **Tie-break:** equal HLC + different `row_hash` converges identically from
  both sides (max `row_hash` wins) and does not oscillate across repeated
  comparisons.
- **Cursor:** atomic temp+rename; a crash before rename leaves the previous
  cursor (safe); missing/corrupt file resets (`cursor_resets_total`) and
  restarts; advance is `min(batch end, truncated_at)` including when a repair
  is deferred.
- **D5 TTL validation:** pass at exactly `max_rotation_budget + skew_margin`;
  fail below it (worker refuses to start, feature stays disabled, mismatch
  logged); validation skipped when disabled.
- **Pacing:** per-cycle key/byte caps; one housekeeping permit per cycle;
  semaphore bound on compares.

### In-process 3-node (crate integration)

- **Missed row heals:** with one node's row removed, AE cycles re-derive it
  from a co-holder; read-back matches the source bytes/hash.
- **Missed DELETE converges:** a node that missed a tombstone pulls it and
  converges to the tombstone — the live row is never "repaired" back
  (no resurrection).
- **Dangling-row gate:** a pulled row whose segment bytes are absent is not
  applied; the ADR-0030 re-replication intent is observed; the row applies
  after the bytes land; a restart before landing re-detects it on the next
  rotation.
- **Kill-switch parity:** `enabled = false` runs identically to today — no
  worker registration, no compare/fetch traffic, no cursor I/O, no AE metric
  series, handlers `unavailable`.

### Cloud acceptance — f4 P4 two-pass (blocked until the user approves f4 resume)

- **Pass A (AE off):** run P4 unchanged; the report records the post-f0
  residual cold-key divergence — the AE baseline and the only sizing data
  that replaces ADR-0037 assumptions A1–A5.
- **Pass B (AE on):** set `metadata_ae.enabled = true` on all SUT configs and
  restart the oceanfs service (same fleet session; no re-provisioning); rerun
  the same P4 scenario. Assertions:
  - the residual divergence is detected
    (`oceanfs_metadata_ae_mismatches_total > 0`);
  - it is repaired (`oceanfs_metadata_ae_rows_pulled_total` and
    `oceanfs_metadata_ae_rows_applied_total > 0`);
  - manifest verification returns to full coverage **within the rotation
    bound for the P4 key count** — the bound is left unasserted until the
    ADR is accepted and Pass A data exists (mirroring the design draft's
    no-bound-before-data caution);
  - tombstone-safety: the missed DELETE is not resurrected (the node that
    missed it converges to the tombstone);
  - no bounded-discipline regression: per-cycle keys/bytes stay within
    budget.
- The report records both passes and both metric sets; **correctness only** —
  no throughput/latency assertion (volume-backed fleet non-comparability
  rule). No load suite runs on the dev machine (PIPELINE §6).

## Definition of Done

- [ ] **Code (build):** `cargo build --all-targets` succeeds in the affected
      crates (`oceanfs-core`, `oceanfs-durability`, `oceanfs-storage`,
      `oceanfs-node`); regenerated proto stubs for the two new RPCs compile;
      no test-only hooks.
- [ ] **Code (config):** `[metadata_ae]` parses with the exact D10 names and
      defaults (`enabled=false`, `interval_sec=30`, `batch_keys=512`,
      `max_batches_per_cycle=8`, `max_bytes_per_cycle=4194304`,
      `max_inflight_compares=2`); `MetadataAeConfig` validation covers
      invalid values; `NodeConfig` carries it with `#[serde(default)]`;
      every `pub` item has `# Examples`.
- [ ] **Code (worker/pacing):** `MetadataAntiEntropy` registered as a
      `DurabilityTask` named `metadata_anti_entropy` with
      `keyspace_fraction() == 1.0`; one Tier-1 `acquire_housekeeping` permit
      per cycle released between cycles; cycle bounded by `interval_sec`,
      `batch_keys`, `max_batches_per_cycle`, `max_bytes_per_cycle`;
      `max_inflight_compares` bounds in-flight compares; no repair storm on
      rejoin.
- [ ] **Code (cursor):** durable `(phase, last_key, rotation_index,
      peer_index)` cursor (~40 B) at `<metadata_pool_root>/metadata_ae.cursor`
      written atomically (temp + rename); missing/corrupt restarts the
      rotation safely; success advances past `min(local batch end, responder
      truncation point)`; zero boot scan / rebuild cost.
- [ ] **Code (protocol):** `CompareMetadata` (unary, batched) and
      `FetchMetadataRows` (server-streaming, reusing `MetadataRow`) on
      `HealingRpc`; responder work capped by `max_entries` with
      `truncated_at`; extras discovery bounded; `MerkleExchange` not
      overloaded; disabled handlers return `unavailable`.
- [ ] **Code (apply path):** rows apply through the mutation choke point with
      capture-preserving semantics (supersede capture on overwrite, tombstone
      migration on re-PUT), reusing the `PutObjectMetadata` LWW/tombstone
      guards and `rebuild_apply_*`; the streamed `row_hash` is re-verified
      before apply; HLC is mandatory.
- [ ] **Correctness (three states):** `(no row)` ≠ `(object row)` ≠
      `(tombstone row)`; absence is never offered as an entry and
      `(no row)` vs `(tombstone)` converges; comparison/repair always uses
      the peer's **logical state for the key** looked up across both CFs (the
      phase is a scan device, not semantic).
- [ ] **Correctness (HLC-aware):** same key with a different HLC is a
      mismatch resolved by LWW; the fingerprint distinguishes the versions
      the apply path treats as distinct.
- [ ] **Correctness (supersede excluded):** supersede records are never
      compared and never pulled; they are produced locally by the apply
      path's capture semantics.
- [ ] **Correctness (equal-HLC tie-break):** equal HLC with different
      `row_hash` is broken deterministically by **max row_hash**, applied
      symmetrically on both sides so convergence is stable.
- [ ] **Correctness (tombstone capture):** tombstone apply is local-aware —
      delete the local live row, capture the **local** row's chunks into the
      tombstone, preserve the pulled `deletion_time`/`hlc`, and drop pulled
      chunk refs for segments this node does not hold (no foreign
      dead-bytes accounting).
- [ ] **Correctness (apply rules):** inline rows apply directly;
      segment-stored rows verify every `ChunkRef.segment_id` is locally
      present (registry `Sealed` + resolvable by the local data store) before
      apply; plain tombstones apply unconditionally.
- [ ] **Correctness (missing bytes):** a row with missing bytes is **not**
      applied; the missing segments are dispatched through the existing
      ADR-0030 target-pull worker (`RequestReReplication`, reason
      `REPAIR_REASON_RECONCILIATION`); the key is tracked in the bounded
      in-memory `awaiting_bytes` set; the row applies when the bytes land; a
      restart re-detects it on the next rotation.
- [ ] **Correctness (bytes before rows):** ordering is enforced — a row whose
      bytes are missing leaves the key unapplied until the byte repair
      completes (reads continue to fail over to replicas).
- [ ] **Correctness (ownership):** one comparison peer per key per rotation
      from the current replica set, manifest-health filtered (ADR-0033 D1),
      rotating until RF−1 peers are covered; both sides compare only
      co-owned keys; the applying side re-checks co-ownership at apply time;
      keys whose arc moved mid-rotation are never falsely repaired.
- [ ] **Correctness (deferred repair advances):** compare success advances
      the cursor even when a repair is deferred; deferred repairs are
      re-detected next rotation; data always moves by pull.
- [ ] **Correctness (D5 TTL invariant):** boot validation refuses to start
      the AE worker when
      `gc.tombstone_ttl_sec < max_rotation_budget + skew_margin`, fails loud,
      leaves the feature disabled, and logs the mismatch;
      `oceanfs_metadata_ae_rotation_seconds` exposes the margin.
- [ ] **Correctness (kill-switch):** `enabled = false` is structurally inert
      — no worker, no RPC traffic, no cursor I/O, no metric series — and
      behavior is identical to today.
- [ ] **Correctness (ADR-0034 accounting):** no new column family, no
      per-row/per-key derived state; the ~40 B cursor is the only new
      persistent artifact; tombstones are read/compared but never duplicated;
      per-tick work is bounded; no recurring `list_objects_all`-class call on
      the AE path.
- [ ] **Tests (unit):** fingerprint states/coverage, equal-HLC tie-break,
      supersede exclusion, cursor atomicity/restart/advance, D5 validation
      boundaries, pacing budgets.
- [ ] **Tests (in-process 3-node):** missed row heals; missed DELETE
      converges to tombstone with no resurrection; dangling-row gate routes
      bytes through the ADR-0030 worker and applies after landing;
      `enabled = false` behaves identically to today.
- [ ] **Tests (cloud, blocked):** f4 P4 two-pass on the fleet — Pass A
      baseline recorded, Pass B assertions per the [Test Plan](#cloud-acceptance--f4-p4-two-pass-blocked-until-the-user-approves-f4-resume);
      **blocked until the user approves the paused f4 resume**; the final
      coverage bound stays unasserted until the ADR is accepted and the
      Pass A data exists.
- [ ] **Docs:** every new/changed `pub` item has `# Examples`;
      `#![deny(missing_docs)]` passes; the config section, cursor file
      format, RPC semantics, and metric names are documented.
- [ ] **ADR:** ADR-0037 constraints D1–D10 are addressed (this spec does not
      re-decide them); the Feature-Spec Deltas path is recorded for a
      different option choice; no unaddressed ADR constraint remains.
- [ ] **Perf:** the cited rules are followed — 1.3 (pre-sized fingerprint
      batch), 4.4 (server-streaming fetch), 5.1 (BLAKE3 row hash), 8.5
      (semaphore-bounded compares). AE is Tier-1 background work with **zero
      write-path cost** and no hot-path change (D1/D8); the remaining rules
      do not apply.
- [ ] **Integration:** a node-level in-process 3-node scenario exercises
      detect → pull → gate → apply end to end (including the byte-repair
      hand-off), and the f4 P4 two-pass provides the fleet acceptance once
      unblocked.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Deviations (accepted)

_None yet — filled at implementation close. If the ADR owner selects a
different option, the applied [Feature-Spec Deltas](../../adr/0037-metadata-anti-entropy-detection.md#if-a-different-option-is-chosen--feature-spec-deltas)
revision is recorded here._

## Open Questions (carried from the ADR)

1. **Rotation budget defaults at horizon scale:** is ~20 h at 10⁷ rows
   acceptable, or should the first implementation ship the bounded hot-set
   accelerator? (Trigger: f4/f5 rotation metrics + first production-scale
   row counts.)
2. **Compare RPC payload tuning:** is a key-ordered full-fingerprint batch
   (default 512) the right unit, or should a digest (Merkle-style) compress
   the equal case for large batches? (Decide at spec/implementation time
   with a micro-benchmark; the protocol shape supports either.)
3. **Ownership during in-flight joins:** ADR-0028's lossless boundaries are
   assumed; confirm that a key whose arc moved mid-rotation is either
   compared against the new holder or skipped without a false repair
   (re-checked at apply time per D3).
4. **Cursor placement under the metadata pool:** confirm the pool-root file
   location against ADR-0029/ADR-0031 path rules and the g8 replacement path.
5. **Tie-break semantics for equal-HLC content divergence:** max `row_hash`
   is proposed; verify against the hint/read-repair paths so the whole system
   uses one deterministic rule.

## Cross-links

- Epic: [metadata-anti-entropy](epic.md).
- Gap capture: [design-draft.md](design-draft.md).
- Decision: [ADR-0037](../../adr/0037-metadata-anti-entropy-detection.md)
  (proposed; acceptance pending).
- Acceptance harness: [f4 pool-degradation-under-load](../fleet-degradation/f4-pool-degradation-under-load.md)
  (paused; P4 two-pass).
- Complement: [f0 hints-durability-gate](../fleet-degradation/f0-hints-durability-gate.md)
  (admission honesty).
- Constraints/patterns: [ADR-0015](../../adr/0015-anti-entropy-merkle-protocol.md)
  (segment-plane Merkle), [ADR-0017](../../adr/0017-durability-task-abstraction.md)
  (Tier-1 scheduling), [ADR-0023](../../adr/0023-metadata-store-native-replacement-path.md)
  (native-store path), [ADR-0027](../../adr/0027-hinted-handoff-ownership-model.md)
  (hint delivery/§D5), [ADR-0029](../../adr/0029-storage-pools-disk-resilience.md)
  (pool-root cursor placement; §D7 metadata recovery),
  [ADR-0030](../../adr/0030-re-replication-target-pull.md) (target-pull),
  [ADR-0033](../../adr/0033-manifest-aware-peer-selection.md)
  (manifest-aware peer selection), [ADR-0034](../../adr/0034-bounded-metadata-accounting.md)
  (bounded-metadata discipline).
