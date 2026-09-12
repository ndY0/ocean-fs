# ADR-0038: Metadata Convergence — Co-Durable Change Journal with Pull-Based Catch-Up

**Status:** Proposed
**Date:** 2026-09-12
**Deciders:** OceanFS architecture team (proposal; pending architecture-owner acceptance)

**Supersedes:** [ADR-0037 rotating bounded scan](./0037-metadata-anti-entropy-detection.md)
(rejected 2026-09-12 — recurring full-metadata scans are incompatible with the
governing performance principle).

**Related:** [ADR-0034 bounded metadata accounting](./0034-bounded-metadata-accounting.md)
(the constraint), [ADR-0023 native-store path](./0023-metadata-store-native-replacement-path.md)
(anti-RocksDB-coupling posture), [ADR-0027 hinted-handoff ownership](./0027-hinted-handoff-ownership-model.md)
(debt ledger, §D5), [ADR-0028 membership plane](./0028-membership-plane-full-swim-gossip.md)
(retained-Dead topology, origin-attributed state), [ADR-0029 storage pools](./0029-storage-pools-disk-resilience.md)
(§D3/§D7 metadata loss), [ADR-0030 target-pull](./0030-re-replication-target-pull.md),
[ADR-0033 manifest-aware peer selection](./0033-manifest-aware-peer-selection.md),
[ADR-0017 durability scheduling](./0017-durability-task-abstraction.md)
(Tier-1, 2026-09-06 amendment), [ADR-0015 segment-plane Merkle](./0015-anti-entropy-merkle-protocol.md)
(untouched), [ADR-0025 lifecycle registry](./0025-segment-lifecycle-state-machine.md),
[ADR-0035 replicated lifecycle state](./0035-replicated-segment-lifecycle-state.md).
Design capture: [`docs/features/metadata-anti-entropy/design-draft.md`](../features/metadata-anti-entropy/design-draft.md);
current (stale) spec: [`pr3-metadata-anti-entropy.md`](../features/metadata-anti-entropy/pr3-metadata-anti-entropy.md)
(still encodes ADR-0037 O3; rewritten by the spec-writer after acceptance).

---

## Context

### The gap (unchanged from ADR-0037)

The index plane has no eventual-repair path. Hinted handoff is the only
metadata-**row** backfill — read repair explicitly delegates the remote-newer
case to it (`crates/oceanfs-server/src/read/coordinator.rs:1142-1149`) — and
segment reconciliation restores **bytes** by holder set without knowing which
rows reference them (`crates/oceanfs-durability/src/reconcile.rs`). Once a row
is missing and its hint is gone, nothing re-derives it. The f3 artifacts show
the shape: whole keys absent from every alive node after hint death / outage
windows (`docs/features/fleet-degradation/artifacts/f3-control-dirty-20260911.json:36-77`,
6 of 94 keys; `…/f3-full-run3-20260911.json:36-65`, 4 of 103 keys). f0 bounds
**new** divergence at the ack boundary; ADR-0038 is the **convergence**
complement. f4's P4 two-pass is the acceptance harness
(`f4-pool-degradation-under-load.md:155-184,309-311`); f4 is paused and no P4
divergence counts exist yet.

### The governing principle (user decision, 2026-09-12)

ADR-0037's Option 3 was rejected because it is a durable **rotating
full-metadata scan**: every rotation re-reads every locally-held row, so
recurring I/O and wire cost are ∝ rows, forever. The user's decision:

> **The system must never perform recurring full-metadata scans — performance
> is the center of the design.** Pay in complexity, never in performance. No
> RocksDB coupling. Triggered, range-bounded bootstrap reads are the only
> allowed recovery reads (they are rare, triggered, and each read is bounded).
> N>RF tests are required.

This kills O3 and every "rare audit scan" framing (including the previously
deferred hybrid hot-set + cold-scan safety net). It does **not** weaken the
correctness bar: convergence must still be complete enough that no cold key
stays divergent forever, and it must remain tombstone-safe.

### Why the alternatives violate the principle

| Candidate | Cost shape | Violates |
|---|---|---|
| O3 — rotating bounded scan (ADR-0037) | I/O + wire ∝ rows **per rotation** | "never recurring full scans" |
| O2 — in-memory Merkle | RAM ∝ rows; boot scan ∝ rows | RAM/rows + boot scan |
| RocksDB WAL tailing (`GetUpdatesSince`) | cheap detection, but correctness coupled to RocksDB internals | ADR-0023 native-store path |
| O1 — fingerprint CF | write-path tax on every mutation + disk ∝ rows + a new CF | ADR-0034 "no new RocksDB surface"; native-store parity |
| hint-style best-effort journal | erased on delivery, no watermarks, not co-durable | cannot support a completeness claim |

What survives the filter is a **change-proportional** mechanism: a small,
co-durable journal of *logical* changes, consumed by peers through durable
watermarks, with triggered bootstraps as the completeness backstop. Detection
cost ∝ changes applied, never ∝ rows stored.

### Grounding facts (verified 2026-09-12; re-verified at the cited lines)

- Rows live in RocksDB CFs `objects`/`deletions`, ~100-300 B/row
  (ADR-0023:84-86), millions per node at horizon. There is **no metadata
  mutation log** anywhere: `MetadataStore` (`crates/oceanfs-storage-api/src/metadata_store.rs:78-278`)
  exposes point get/put/delete/batch only — **no range API**; the scheduler
  documents `keyspace_fraction` as shipping **inert** for exactly that reason
  (`crates/oceanfs-durability/src/scheduler/mod.rs:15-34`).
- Apply is HLC-mandatory, LWW-guarded by callers
  (`crates/oceanfs-server/src/grpc/segment_service.rs:655-803`: `PutObjectMetadata`;
  `crates/oceanfs-server/src/write/coordinator.rs:1151-1364`: `apply_hinted_object`);
  `batch_write` (`crates/oceanfs-storage/src/metadata/store.rs:1133`)
  bypasses the logical checks and is used only by compaction/healing remaps
  (`crates/oceanfs-durability/src/gc/segment_compactor.rs:464`,
  `crates/oceanfs-durability/src/healing_service.rs:750-795`) which preserve HLC
  and only re-point physical chunk refs.
- Hints: per-target WAL files, fsync per hint, erased on delivery; give-up at
  10 attempts; TTL 7 d (`hint_ttl_sec = 604800`,
  `crates/oceanfs-core/src/config/node.rs:668-682,779-782`) prunes with **no
  escalation**; node-local, unreplicated; no pending gauge; f0 refuses ack
  when the hints pool is Dead (`coordinator.rs:1369-1377`).
- Placement: object-key ring vs `blake3(segment_id)` ring; segment ids are
  random UUIDv7 → the two rings are independent. Segment repair failure has no
  terminal state: targetless re-rep requests retry and park, retry exhaustion
  is a log line (`crates/oceanfs-durability/src/repair.rs:332-363`).
- g8 rebuild: boot-time gate when both CFs are empty; the server side
  hash-filters a **full scan of both CFs** (`crates/oceanfs-node/src/modules/metadata_recovery.rs:42-89`,
  consumer `:210-291,332-397`; wire `proto/oceanfs/healing.proto:120-127,376-406`).
  Acceptable once at boot, unusable as a recurring mechanism.
- Transport patterns to reuse: `FetchHintObject` (point fetch of current
  object state, server-streaming, `healing.proto:83-87,248-266`),
  `ListObjectsInRange`/`MetadataRow` (row transport, `healing.proto:120-127,386-406`).

### Constraints

- **C1 — no recurring full scans.** Any new mechanism whose recurring cost is
  ∝ rows is rejected regardless of cleverness. Recovery reads must be
  triggered and each read range-bounded.
- **C2 — no RocksDB coupling.** No new CF; no dependence on RocksDB WAL
  internals; the mechanism must be implementable on the ADR-0023 Phase-2
  native store from the same store-mutation boundary.
- **C3 — complexity accepted, performance preserved.** Extra machinery,
  configuration, and failure modes are the accepted currency; the write path
  may pay only a small, bounded, group-committed append.
- **C4 — correctness rules** (from the fact-find; unchanged from ADR-0037's
  D2/D5 intent): logical row identity only — `(size, blake3_hash, inline_data,
  HLC)` — never physical chunk refs (they differ legitimately per replica);
  **no local-segment-presence precondition** (rows and bytes live on
  independent rings; foreign chunk refs are normal and resolvable through the
  read path; bytes are the segment plane's); three states
  `(no row)` ≠ `(object row)` ≠ `(tombstone row)`; tombstone-safe replay (no
  resurrection); deterministic equal-HLC tie-break; kill-switch inert when
  disabled (no worker, no RPC, no journal I/O).
- **C5 — completeness backstop.** The journal accelerates convergence; it is
  not the sole completeness guarantee. Every state transition the journal
  cannot cover (new owner, trimmed-away range, epoch change, pool loss) has a
  triggered, range-bounded bootstrap.

---

## Decision

**Adopt a metadata operation journal with pull-based catch-up.** Every
logical metadata mutation a node applies appends a small `{seq, key, op, HLC}`
entry to a node-local, append-only, trimmable journal that is **co-durable
with the row**. Peers consume journals through durable per-peer watermarks and
repair by **point-fetching current state** (never by replaying row bodies from
the journal). A bounded sync worker provides the freshness knob. Completeness
outside the retained journal window comes from **triggered, range-bounded,
key-ordered bootstraps** over owned ranges, which also replace the g8
full-CF recovery scan. A read-miss point fetch serves the user edge. Debt
hardening, a bounded HMAC spot-check verifier, and a segment-plane
unrecoverable signal are complements. **No recurring scan exists anywhere in
the design.**

### D1. The journal — entry, capture points, co-durability

**Entry (fixed, tiny, no row body):**

```
JournalEntry {
    seq:  u64,     // per-node monotonic; assigned at append; strictly increasing
    op:   u8,      // 1 = put (live state), 2 = delete (plain tombstone state)
    key:  bytes,   // full store key {bucket}\0{key}
    hlc:  Hlc,     // the logical version the apply carried
}
```

No value bytes, no chunk refs, no size/hash. The entry is a *trigger*: the
consumer asks a holder for the key's **current** state (D5), so stale entries
are self-correcting. A re-PUT is `op=put`; supersede records and
`delete_dead_chunk_record` are GC accounting, not logical state, and are
**never journaled**.

**Capture points — at the store mutation choke point, not in callers.** The
journal hook lives in `RocksDbMetadataStore` beside the existing mutation
methods (`crates/oceanfs-storage/src/metadata/store.rs:529-692,1133-1178,1569-1616`),
so every path is covered once (the ADR-0034 D2 capture discipline):

| Path | Site | Journal? |
|---|---|---|
| Client PUT / overwrite (S3, replica apply) | `put_object_in_bucket` | yes (`put`; new HLC) |
| Client DELETE | `delete_object` | yes (`delete`) |
| Hint apply | `apply_hinted_object` → `put_object` | yes (`put`) |
| g8 / bootstrap row apply | `rebuild_apply_object_row` / `rebuild_apply_deletion_row` | yes — the rebuilt node's journal covers the state it restores |
| Compaction / healing remap | `batch_write` | yes **iff** `(size, blake3_hash, inline_data, hlc)` changes; the two current callers preserve HLC and re-point refs only, so they do not journal. A defensive read-before-write (compare logical identity) is required if a future caller can change it. |

**Co-durability and ordering.**

- **J1 — append-before-commit.** For a mutation batch, the journal entry (or
  entries) is appended and made durable via **group commit** *before* the
  metadata row write is made visible. A mutation whose journal append fails is
  **fail-closed**: the row is not committed, the caller gets an error, and
  `oceanfs_metadata_journal_append_errors_total` increments. (Alternative
  "commit-with-gap-marker" is an open question, not this decision.)
- **J2 — crash window.** A crash after the journal fsync and before the row
  commit leaves an entry with no row. This is harmless: the consumer fetches
  current state; a key absent everywhere is a consumed no-op entry. The
  inverse — a row without an entry — is impossible by J1. This is the
  invariant: **if the row survived on a node, its change record survived on
  that node.**
- **J3 — journal ≠ RocksDB.** The journal is an independent append-only file
  set on the metadata pool root with its own group-commit and recovery. It is
  **not** a CF, not RocksDB's WAL, and not read via `GetUpdatesSince`. A
  native store (ADR-0023 Phase 2) hooks the same store-mutation boundary.

### D2. Journal storage, epochs, and trim/retention policy

- **Files:** `<metadata_pool_root>/metadata_sync/journal/<file_no>.jrn`,
  rotated at a fixed internal size (e.g. 64 MiB). Each file carries a header
  `{magic, version, node_id, epoch, first_seq}`; each record is
  length-prefixed + trailer-checksummed so a torn tail is truncatable at
  recovery (truncation discards only an un-fsynced tail; J1 makes that safe).
- **Epoch:** a random 128-bit `journal_epoch` created with the journal
  directory and persisted in every file header. A metadata-pool replacement
  or journal loss starts a **new epoch**. Watermarks name `(epoch, seq)`;
  an epoch change invalidates the peer's prior watermarks (gap semantics,
  D4).
- **Trim frontier.** `frontier = min(acked[P])` over peers **retained in the
  membership topology** (Alive/Suspect/Dead — ADR-0027 D1 / ADR-0028 retain
  Dead nodes in the ring; only `Left`/removed peers leave the frontier),
  excluding self. Files entirely below the frontier are unlinked (fsync'd
  directory). This is **membership-driven retention**, not blind TTL: a
  Dead-but-retained peer keeps its entries and can replay everything it
  missed when it returns.
- **Hard caps override the frontier.** `journal_max_bytes` (default 256 MiB)
  and `journal_max_age_secs` (default 7 d) are absolute bounds. When a cap
  bites, the journal trims past the lagging peer's watermark and records the
  new `oldest_seq`; that peer receives a `gap` on its next pull and bootstraps
  the ranges it owns (D6). Journal disk is therefore O(changes within the
  retention window) and ≤ the cap, independent of row count.
- **Trim safety argument.** (a) Every retained peer's watermark is honored,
  so nothing a peer still needs at its current watermark is trimmed.
  (b) A peer beyond the frontier has no claim on the trimmed range: its
  required state is its **owned ranges' current state**, which a triggered
  bootstrap fetches completely (D6). (c) Sequence numbers are never reused
  within an epoch and epoch changes invalidate stale watermarks, so a
  watermark can never silently skip records.

### D3. Watermarks — protocol and persistence

Per peer P, each node durably tracks two directions:

- `consumed(P)` — highest seq of **P's** journal this node has applied
  (advances during sync);
- `acked(P)` — highest seq of **this node's** journal that P has confirmed
  consumed (learned from P's pull requests, which carry `consumed_me`).

- **Persistence:** `<metadata_pool_root>/metadata_sync/watermarks/<peer>.wm`,
  atomic temp+rename, lazily flushed (a lost advance only re-consumes entries
  — idempotent). Journal loss and watermark loss share a fate (same pool),
  which is correct: a new epoch makes the old watermarks meaningless anyway.
- **Pull exchange:** `FetchJournal(epoch, from_seq, max_entries, max_bytes,
  requester_id) → (epoch, oldest_seq, entries[], next_seq)`. `from_seq <
  oldest_seq` or a foreign epoch with prior progress returns a **gap** (no
  entries); the requester enqueues a bootstrap.
- Watermarks are exchanged **both directions** opportunistically: every pull
  carries the requester's `consumed` of the responder (acks) and the responder
  replies with its `consumed` of the requester (so the requester's `acked`
  view stays fresh without a third RPC).

### D4. Sync worker — the freshness knob (Tier-1, ADR-0017)

- One node-local `MetadataSync` worker owned by `oceanfs-durability`, wired by
  the composition root; a `DurabilityTask` named `metadata_sync` with
  `keyspace_fraction() == 1.0` (its unit is the journal, not CF ranges),
  acquiring one Tier-1 `acquire_housekeeping` permit per cycle and releasing
  it between cycles. The scheduler's skip/overrun semantics apply unchanged.
- **Triggers:** periodic (`interval_sec`, the freshness knob) **plus**
  event-triggered sweeps when a peer becomes Alive (membership event, ADR-0028)
  or when the worker observes a peer's epoch change.
- **Cycle shape, per peer:** pull up to `max_entries_per_cycle` entries /
  `max_bytes_per_cycle`; filter entries to keys this node currently co-owns
  (re-checked at apply time); for each key take the **latest** ordered entry;
  coalesce keys; point-fetch current state from one manifest-healthy
  co-holder (ADR-0033 D1); apply via HLC-LWW; advance `consumed(P)`.
  `max_inflight_pulls` bounds fetch concurrency. The worker never pushes.
- **Priorities and storms:** missing rows inherit the existing reconcile/G4
  urgency when under-replication is known; missing **bytes** ride the Tier-0
  ADR-0030 worker unchanged. Rejoin is capped by the per-cycle budget; a
  returning node requests from its watermarks and cannot cause a storm.
- **Ownership filter and ring-change rules:** the sync worker consumes every
  entry past its watermark but applies only keys this node **currently**
  co-owns, re-checked against the live ring snapshot at apply time. Entries
  for keys whose arc moved away are skipped (`keys_skipped_total{not_owner}`)
  — their new co-owners get them via their own consumption or a bootstrap.
  Newly owned keys (join/leave/re-weight) are covered by the ownership-edge
  bootstrap trigger (D6); the worker never caches or mutates ownership, so a
  ring change mid-cycle cannot produce a false repair. A key that is
  co-owned but whose row is already logically current is skipped
  (`already_current`).
- **Bootstrap is separate and triggered** (D6): it is not a scheduled loop and
  must not be mistaken for a recurring scan. Steady-state bootstrap cost is
  zero.

### D5. Catch-up semantics — point fetch, logical identity, no local-presence precondition

- **Fetch:** a new `FetchMetadataRows(keys[]) → stream MetadataRow` RPC on
  `HealingRpc` returns each key's **current stored state** from a holder
  (object row or plain tombstone; supersedes excluded). It generalizes the
  `FetchHintObject` current-state pattern (`healing.proto:83-87,248-266`)
  and reuses `MetadataRow` (`:386-406`). Disabled handlers return
  `unavailable`.
- **Logical identity:** comparison and anti-false-divergence use
  `(size, blake3_hash, inline_data, HLC)`. Chunk refs are **not** identity:
  two replicas holding the same logical object with different physical refs
  are in sync and are not repaired. This closes the HLC-blind-divergence class
  (`review/cluster-churn-resolution-2026-09-10.md:72-73`) without
  false positives.
- **No local-segment-presence precondition (supersedes ADR-0037 D4).** A
  repaired row is applied even when it references segments this node does not
  hold. Rows and bytes live on independent rings (object-key ring vs
  `blake3(segment_id)` ring; segment ids are random UUIDv7); foreign chunk
  refs are normal and are resolved at read time by the cluster read path
  (g6/ADR-0033 failover). **Bytes are never repaired here** — segment
  under-replication, including foreign refs whose bytes are gone, is
  f5/reconcile/ADR-0030's plane. This is a deliberate refinement of ADR-0027
  D5: that rule prohibits a *read-repair push of a remote winner as local
  truth*; this path is pull-only, version-guarded (HLC-LWW), and
  logical-identity-scoped. (The ADR-0027 D5 wording should be read with that
  distinction; recorded in Risks.)
- **Three states:** `(no row)`, `(object row)`, `(plain tombstone)` are
  distinct. A fetched object row with `inline_data` applies directly; a
  fetched tombstone applies through the local-aware path: delete the local
  live row, preserve the pulled HLC/deletion version, write the tombstone;
  supersede records never travel. A re-PUT clears a local tombstone through
  the same LWW rules.
- **Conflicts:** HLC-LWW is mandatory; equal HLC with different logical
  identity is broken deterministically by max identity hash, applied
  symmetrically on both sides (carried forward from ADR-0037 D2).
- **Idempotence:** re-applying current state is a no-op; entries are consumed
  once per `(epoch, seq)`; a watermark never moves backwards within an epoch.
- **Read-trigger (user edge):** on a local metadata miss where this node is in
  the key's current replica set and has no local tombstone, issue one bounded
  point fetch to a co-holder, apply via the D5 rules, and serve; failure falls
  back to today's `404`/absent behavior. Concurrent misses for the same key
  are single-flighted; misses are briefly negatively cached to prevent fetch
  storms. This is scan-free and closes the "local row absent but the node is
  an owner" hole without waiting for a sync cycle.

### D6. The ownership edge — triggered range-bounded bootstrap

**Triggers.** A node bootstraps (a subset of) its owned ranges when:
1. it is a **new owner** — join, leave, or ring re-weight changed its owned
   ranges since the last persisted ownership snapshot;
2. a **gap** is detected — its watermark of a journal is below `oldest_seq`,
   or a peer's journal epoch changed with prior progress;
3. its **metadata pool was lost/replaced** (g8 boot rebuild is this trigger's
   first consumer);
4. the journal was lost while rows survived (new epoch without a rebuild) —
   bootstrap (or an equivalent journal-rebuild pass) re-establishes coverage;
5. the operator **enables** the journal on a cluster with pre-existing
   divergence and wants it healed — one-time bootstrap of owned ranges.
   Otherwise the journal heals only changes applied after enablement (an
   operational rule, not a bug).

**Transfer.** Key-ordered, range-bounded, resumable. The requester walks its
owned ranges in **raw store key order** (ownership is by key hash; hash order
would make every read unbounded). For each window it sends
`FetchMetadataRange(start_key, end_key, max_rows, max_bytes)` to one
manifest-healthy holder; the responder iterates the objects CF then the
deletions CF through the new range-iterator API, filters per row by the
requester's ownership arcs, streams up to the cap, and returns `next_key`
(resume point). The requester applies through the D5 LWW/semantic guards,
journals the applied state (D1 — the rebuilt node's journal then covers its
state onward), and persists its bootstrap cursor so an interrupted bootstrap
resumes. Windows are bounded; the transfer is triggered and rare, never
scheduled.

**Range-iterator API (backend-neutral; on the metadata store trait).** This is
the piece the current `MetadataStore` lacks and the piece that unblocks the
inert `keyspace_fraction` scheduler (`crates/oceanfs-durability/src/scheduler/mod.rs:28-31`):

```
/// Keyspace selector for range iteration.
enum MetadataKeyspace { Objects, Deletions }

trait MetadataStore {
    /// Visits raw rows of one keyspace in key order, from `start`
    /// (inclusive) until `end` (exclusive) or `limit` rows, whichever
    /// comes first. Never materializes the range.
    ///
    /// Returns `Ok(None)` when the range is exhausted, or the next
    /// resume key (exclusive) when `limit`/`end` stopped the visit.
    fn visit_rows_range(
        &self,
        ks: MetadataKeyspace,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> std::io::Result<Option<Vec<u8>>>;
}
```

- Each backend implements it natively (RocksDB: `iterator_cf` with an explicit
  seek key and `IteratorMode::From`; a native store: its sorted index). The
  trait signature is synchronous raw-row, matching the existing `MetadataStore`
  style; the bootstrap consumer runs it on the blocking pool and streams.
- **Replacing g8's full scan.** `MetadataStoreRangeLister` (server side,
  `crates/oceanfs-node/src/modules/metadata_recovery.rs:42-89`) switches from
  `visit_objects_rows` + `visit_deletions_rows` (whole-CF hash-filtered) to
  per-window `visit_rows_range` calls; `ObjectRangeRequest` gains
  `start_key`/`end_key`/`max_rows`/`max_bytes` (backward-compatible fields;
  the old hash-arc fields stay until the consumer migrates). The rebuild
  consumer walks windows and paginates with `next_key`. The **total** work of
  a full ownership bootstrap is still O(responder rows) when every window must
  be visited — that is the honest cost of a filter without a hash-ordered
  index — but it is triggered, resumable, paced, and never recurring; each
  individual read is bounded. This is the "only allowed recovery read" class.

### D7. Complements

- **Debt hardening (hints).**
  - *Membership-driven retention*: hint debt for a retained-Dead target is
    held while the target remains in the ring (ADR-0027 D1); blind TTL is no
    longer the only fate.
  - *TTL expiry fires the same escalation as give-up*: `prune_expired`
    (`crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs:298-330`)
    routes expired debt through the existing f5 D3 `HintDropSink`
    (`hint_delivery.rs:271-274`, repair-intent conversion) instead of deleting
    silently; escalation on `Left`/removal likewise.
  - *Pending-debt gauge*: `hinted_handoff_pending_debt{target}` (count/bytes)
    so outstanding debt is observable; `hints_expired_total` alone is not
    enough.
  - *No hint mirroring*: hints stay node-local; replicating hint WALs is
    rejected (it multiplies debt and does not converge rows — the journal +
    catch-up is that path).
- **Bounded HMAC spot-check verifier.** Each sync cycle samples up to
  `spot_check_keys_per_cycle` keys from a bounded in-memory recent-touch
  reservoir (fed by local reads and journal appends; capped, never a
  traversal), asks a manifest-healthy co-holder for the key's HMAC tag
  (keyed BLAKE3 over the logical identity + HLC), and compares. A mismatch or
  absent response escalates to a D5 point fetch + apply and increments
  `oceanfs_metadata_sync_spot_mismatches_total`. It is probabilistic bug
  detection for what the journal cannot prove (capture gaps, silent loss) and
  is complementary, never a coverage mechanism. The tag is a compact
  comparison token, not an authentication boundary in the open trust model
  (ADR-0028); key provisioning is an open question.
- **Segment-plane unrecoverable signal.** When a repair request's holder set
  has no live recorded holder (`storage_locations ∩ live = ∅`), classify it
  terminal instead of parking/retrying forever
  (`crates/oceanfs-durability/src/repair.rs:332-363`): emit
  `oceanfs_repair_unrecoverable_total{reason="no_live_holder"}`, surface it on
  admin, and stop re-enqueueing (bounded dedupe set). When a recorded holder
  returns, clear and resume. EC-reconstructible segments remain the EC/heal
  plane's responsibility; this signal is the row/segment planes' dead end.

### D8. Kill-switch and configuration (exact names)

New section in `oceanfs.toml`; `MetadataSyncConfig` in
`crates/oceanfs-core/src/config/metadata_sync.rs` (same pattern as
`config/durability.rs`), re-exported from `oceanfs_core` and added to
`NodeConfig` beside `anti_entropy`/`durability`
(`crates/oceanfs-core/src/config/node.rs:220-230`) as
`#[serde(default)] pub metadata_sync: crate::MetadataSyncConfig`.

```toml
[metadata_sync]
enabled = false                  # kill-switch (default OFF)
interval_sec = 10                # freshness knob: Tier-1 sync cycle cadence
max_entries_per_cycle = 4096     # journal entries consumed per cycle (all peers)
max_bytes_per_cycle = 4194304    # pulled row bytes per cycle
max_inflight_pulls = 2           # bounded catch-up fetch concurrency
journal_max_bytes = 268435456    # hard journal cap (256 MiB) — trim floor
journal_max_age_secs = 604800    # hard age cap (7 d) — trim floor
spot_check_keys_per_cycle = 16   # HMAC samples per cycle
read_trigger_enabled = true      # owner-only point fetch on a read miss
bootstrap_batch_keys = 1024      # key-ordered window size per bootstrap request
```

- `enabled = false`: the composition root does not construct the journal or
  worker; no journal file is opened, read, or written (the store's journal
  handle is `None` — a branch on an `Option`, no I/O); no sweep runs; the
  fetch/bootstrap/spot-check handlers return `unavailable`; no metrics
  register. Zero behavior change.
- `enabled = true`: the journal opens on the metadata pool, capture turns on
  at the store boundary, the Tier-1 worker registers, handlers serve, metrics
  register.
- **Mixed-version / mixed-kill-switch meshes:** enabled peers skip disabled
  peers (a disabled peer publishes no epoch/watermark and is not expected to
  consume); disabled peers answer `unavailable`. A later enable starts a new
  epoch; peers treat it as a gap and bootstrap where needed. No correctness
  claim is made while a quorum of the ring is disabled — convergence is best
  effort, exactly like today.

### D9. Metrics

Registered only when enabled (a disabled node exposes no series):

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
| `oceanfs_metadata_sync_bootstrap_rows_total` / `..._bytes_total` | counter | range-bounded bootstrap volume |
| `oceanfs_metadata_sync_spot_checked_total` / `..._spot_mismatches_total` | counter | samples / detected divergences |
| `oceanfs_metadata_sync_read_trigger_served_total` / `..._missed_total` | counter | read-miss repairs served / failed |
| `hinted_handoff_pending_debt{target}` | gauge | outstanding hint debt (D7) |
| `oceanfs_repair_unrecoverable_total{reason}` | counter | terminal no-live-holder repairs (D7) |

Naming follows the `oceanfs_metadata_*` (g8) and `hinted_handoff_*`
conventions and avoids the segment-plane `ae_*` series.

### D10. ADR-0034 accounting statement

**This is an explicit, argued exception to "no new persisted surface", not a
new per-row index.**

- **Disk:** the journal is O(changes within the retention window) ≤
  `journal_max_bytes` (256 MiB default) and trims toward zero; it is **not**
  ∝ rows, carries **no row bodies**, and dies with the metadata pool whose
  fate it shares (g8 bootstrap). It is not a column family, not a derived
  per-key structure, and not RocksDB WAL internals.
- **Write path:** one small append + group-commit fsync per logical mutation
  batch — the only new hot-path cost, bounded and amortized, incurred only
  when enabled. This is strictly smaller than O1's per-mutation CF row + disk
  ∝ rows and than O2's per-mutation in-memory feed.
- **Read path:** the sync worker performs bounded point reads and journal
  reads; there is no recurring CF traversal anywhere. The bootstrap's
  triggered full-ownership walk is the one O(rows) *scan-class* operation,
  and it is gated by a trigger and resumable — the design's only allowed
  recovery read.
- **Memory:** fixed journal buffers + per-peer watermarks O(N) +
  bounded spot-check reservoir; no per-row state.
- **ADR-0023:** the journal and range API sit at the store-mutation boundary;
  the native store provides both without inheriting RocksDB.

### D11. Failure semantics — what this can and cannot fix

**Can fix:** missing or stale rows/tombstones for co-owned keys when a live
holder exists (hint death, device loss, bounded give-up, crash windows); the
post-enable change stream; pre-enable divergence after a bootstrap; read-miss
absences (read-trigger).

**Cannot fix:** segment **bytes** (f5/reconcile/ADR-0030); a segment with no
live recorded holder (unrecoverable signal, D7); data whose every holder is
gone; EC reconstruction (EC/heal plane); clock skew outside HLC semantics.

**Regimes:**

| Regime | Mechanism |
|---|---|
| row absent, bytes held somewhere | journal/read-trigger point fetch + LWW apply |
| row stale (older HLC) | journal sync / read-trigger; LWW accepts current state |
| row present, bytes under-replicated | segment plane (reconcile/ADR-0030); this ADR does nothing |
| row references segments this node lacks | normal (independent rings); reads fail over; bytes plane repairs |
| journal gap / epoch change / new owner | triggered range-bounded bootstrap of owned ranges |
| metadata pool lost | g8 boot rebuild via the same bootstrap path (D6) |
| no live recorded holder for a segment/probe | unrecoverable signal; no infinite retry |
| capture bug (mutation never journaled) | not deterministically detectable — HMAC spot-check gives probabilistic detection; O1 is the fallback |

### Out of scope

- Segment-plane reconciliation, holder-set repair, EC reconstruction.
- Read-path **push** repair and debt-replay repair (dropped 2026-09-11).
- C2b proactive row migration (may later reuse the range API).
- Repair of extra (non-owner) stale rows — GC/ownership handles those.
- Changing the hint delivery contract (ADR-0027 as amended) or the f0 gate
  beyond the D7 hardening.
- Any recurring audit/sampling scan of the keyspace.

---

## Consequences

### Positive

- **The gap closes with detection cost ∝ changes, never ∝ rows.** The user's
  governing principle is satisfied by construction; no recurring full scan
  exists in the design.
- **No RocksDB coupling and no new CF.** The journal is an independent file;
  the range API is a store-trait method; ADR-0023's native-store path is
  preserved.
- **Hot-path cost is one bounded append**, only when enabled; group commit
  amortizes it.
- **Completeness is layered:** the journal (change edge) + triggered bootstrap
  (ownership edge) + read-trigger (user edge) + spot-check (probabilistic
  bug detection) — each with an explicit bound.
- **Debt stops being blind:** membership-driven retention, escalating TTL,
  and a pending gauge remove the last silent-loss path in hints.
- **The `keyspace_fraction` blocker is removed** for future key-ordered
  sharding (a side effect of the range API).

### Negative

- **New failure domain: the journal.** A bug in capture, ordering (J1), trim,
  or recovery can create silent divergence; this is why capture completeness
  and trim safety are the top risks and carry an explicit test matrix.
- **One fsync-group per mutation on the write path** when enabled. Small and
  amortized, but non-zero; the fail-closed policy means a journal I/O fault
  fails writes (loudly, with metrics) rather than degrading silently.
- **Bootstrap is a triggered O(rows)-walk** (per window bounded, resumable,
  rare). It is the honest cost of filtering without a hash-ordered index;
  it is not recurring and not on any schedule.
- **Operational coupling:** correct convergence assumes the journal is not
  silently disabled in a mesh; mixed meshes are best-effort.
- **Pre-enable divergence is not retroactively healed** by the change edge;
  it needs a bootstrap or a re-created workload.

### Neutral

- Segment-plane `[anti_entropy]` is untouched. g8 changes implementation
  (range windows) but keeps behavior; the old wire fields remain during
  migration.
- `pr3-metadata-anti-entropy.md` (O3) is stale until the spec-writer rewrites
  it after acceptance; the epic and cross-links must be updated then.
- Existing hint and repair behavior is extended, not replaced.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why rejected |
|---|---|---|---|
| **O3 — rotating bounded scan (ADR-0037)** | exact, no new persisted surface, no hot-path cost | recurring I/O + wire ∝ rows per rotation, forever | **Rejected by principle (2026-09-12):** recurring full-metadata scans are the one thing the design must never do; "rare" does not fix ∝ rows |
| **O2 — in-memory Merkle (change-fed)** | exact, localized, no disk | RAM ∝ rows; boot scan or a new WAL surface; keyspace sharding prerequisite | Change journal + catch-up reaches the same locality with O(changes) memory and no boot scan |
| **Tailing RocksDB's WAL (`GetUpdatesSince`)** | detection nearly free; no new files | couples correctness to RocksDB internals; breaks ADR-0023; no epoch/ownership semantics; per-CF logical ops not exposed | ADR-0023 posture; the journal is the backend-neutral equivalent |
| **Hint-style best-effort journal** | reuses hint WAL code | delivery-oriented, erased on delivery, not co-durable, no watermarks | Cannot support the completeness invariant or replay |
| **O1 — hash-ordered fingerprint CF** | exact detection, cheap comparison; provably complete | permanent write-path tax; disk ∝ rows; new CF; native-store parity burden | **Documented fallback**, not chosen: adopt only if journal capture completeness proves intractable. Kept in this table as the sanctioned escape hatch |
| **Hint cloning / mirroring debt** | simple redundancy | multiplies debt, still best-effort, no change coverage | Does not converge rows; the journal is the convergence path |
| **Full-row push replication to all peers** | simple mental model | bandwidth ∝ writes × N; no localization; no backpressure story | Aggressively violates the performance principle |

---

## Risks and Open Questions

1. **Capture completeness.** Every logical-mutation path must journal
   (enumerated in D1, mirroring ADR-0034 D6). The defensive `batch_write`
   identity-compare is the guard against future callers. Residual risk: a new
   mutation path added later bypasses the store. Test: capture matrix with
   fault injection; spot-check catches probabilistic drift.
2. **Trim safety and watermark crash windows.** A crash between apply and
   watermark persistence re-consumes entries (safe); a race between trim and a
   peer's pull must never remove an entry whose watermark is not yet durable
   at the trimmer. The frontier is `min(acked)` computed from *received*
   acks, so a lost ack only delays trim. Bound: cap/gap conversion. Needs a
   dedicated crash/race test matrix.
3. **Co-durability ordering cost.** J1's group-commit fsync is the only
   hot-path change; measure per-batch latency under load on the fleet before
   enabling by default. Open: fail-closed vs. durable gap-marker on append
   failure (fail-closed chosen; the alternative trades availability for a
   bootstrap requirement).
4. **Ownership churn semantics.** A ring change mid-bootstrap or mid-sync:
   entries for keys no longer owned are skipped; a bootstrap cursor whose
   range moved must be invalidated (recompute owned windows per attempt);
   co-ownership is re-checked at every apply. Open: how ownership snapshots
   are persisted and invalidated (derive per attempt vs. store a ring-version
   cursor).
5. **Mixed-version / mixed-kill-switch meshes.** Enabled peers must skip
   disabled ones cleanly; epoch/version fields must be forward-compatible
   (proto evolution). No rolling-upgrade concern today (all-at-once deploys),
   but the handler `unavailable` path and epoch mismatch must be tested.
6. **Bootstrap API migration.** Replacing `MetadataStoreRangeLister`'s
   full-CF hash filter with ranged windows changes g8's wire semantics
   (key-ordered windows vs. hash arcs); the old fields must remain until all
   consumers migrate, and the range trait must be implementable on the
   planned native store. Golden test: windowed bootstrap output ≡ g8 output
   on a seeded store.
7. **Read-trigger amplification.** A widely-missing key can send a fetch from
   every owner on every read; single-flight + short negative caching + a
   per-node fetch bound are required. Open: exact negative-cache TTL and
   budget.
8. **Foreign refs vs. ADR-0027 D5.** D5's text prohibits "metadata-only repair
   (pushing foreign chunk references)". This ADR's pull path stores the
   holder's refs by design. The distinction (push of local truth vs.
   pull of version-guarded current state; refs are not identity; the bytes
   plane is independent) should be recorded as a clarification/amendment to
   ADR-0027 D5 if the user accepts this ADR.
9. **Spot-check selection and keying.** Reservoir selection, sampling cadence,
   HMAC key provisioning (`metadata_sync` config vs. cluster secret), and
   false-positive handling are open implementation questions.
10. **Epoch and gap semantics.** Multiple journal losses and re-epochs in a
    mesh, and a peer that never re-enables, must each terminate in a bounded
    bootstrap or an explicit unrecoverable count — never an infinite gap
    loop.

---

## Complexity and Staging

### Per-component estimates

Production LOC are estimates against this codebase's conventions; test LOC
are in-crate unit + integration tests (RocksDB-affected crates run with
`--test-threads=1`, PIPELINE §4.6).

| Stage / component | Production LOC | Test LOC | Risk |
|---|---|---|---|
| **S1 — debt hardening (hints)** (retention/escalation/pending gauge; `hinted_handoff/*`, config, metrics) | 250-400 | 250-350 | Low |
| **S1 — segment unrecoverable signal** (`repair.rs`, admin/metrics) | 100-200 | 100-150 | Low |
| **S2 — journal file, group commit, rotation, recovery, trim** (storage; new `metadata/journal.rs`) | 600-900 | 700-1,000 | **Highest** |
| **S2 — store capture points + completeness matrix** (`store.rs` choke points) | 250-450 | 350-500 | **High** |
| **S2 — watermark store + protocol** (`oceanfs-durability`) | 250-400 | 300-450 | High |
| **S2 — sync worker, pacing, config, wiring** | 400-600 | 400-600 | Medium |
| **S2 — point-fetch RPC + proto** (`FetchMetadataRows`, epoch fields) | 250-400 | 250-400 | Medium |
| **S3 — range-iterator API + RocksDB impl** (`oceanfs-storage-api` + storage) | 250-400 | 300-450 | Medium |
| **S3 — bootstrap coordinator + triggers + cursors** | 500-800 | 600-900 | High |
| **S3 — g8 lister replacement + wire migration** | 150-250 | 200-300 | Medium |
| **S3 — read-trigger** (read path + single-flight/negative cache) | 250-400 | 300-450 | Medium |
| **S4 — HMAC spot-check verifier + RPC** | 300-500 | 300-500 | Medium |
| **S4 — hardening: mixed-mesh, docs, metrics, fault matrix** | 150-300 | 200-350 | Low |
| **Total** | **~3,600-5,800** | **~3,900-5,900** | |

**Relative effort:** pr1 + pr2 (pool-runtime-lifecycle) landed at roughly
**~1.5k production LOC total**. This design is **~2.5-4× that production
volume** across **4 staged features**; S2 (journal + sync) alone is
~2.0-3.2k production LOC — the "minimum viable convergence" is ~1.3-2.1×
pr1+pr2. The dominant effort and the dominant risk are the same: the
co-durability/capture/trim core, not the protocol.

**Risk ranking (descending):** (1) journal co-durability + capture
completeness; (2) trim/watermark safety under crash and race; (3) ownership
churn + bootstrap correctness; (4) hot-path append/fsync cost; (5) mixed-mesh
and epoch semantics; (6) bootstrap wire migration (g8 equivalence);
(7) read-trigger amplification; (8) spot-check key management.

### Staged plan

**S1 — fixes (independent of the journal).** Membership-driven hint
retention, TTL-expiry escalation through the f5 D3 sink, pending-debt gauge,
and the segment-plane unrecoverable signal. Verify: hint unit/integration
suites (expiry escalation, retention vs. Dead/Left, gauge), repair terminal
classification tests. f4: **Pass A** can observe the new gauge and that
rejected-debt behavior is unchanged; TTL escalation itself is unit-level
(7 d TTL never fires in a session). No ADR-0038 machinery is required.

**S2 — journal + sync (the change edge).** Journal, capture, watermarks,
Tier-1 worker, point fetch, config/metrics, kill-switch. Verify: in-process
3-node missed-row and missed-DELETE convergence (no resurrection), crash
between journal fsync and row commit, trim-vs-watermark races, kill-switch
parity. f4: **P4 Pass A** (AE off) records the baseline exactly as today;
**P4 Pass B** (enabled, same fleet session, service restart) must produce
divergence **after enablement** (the change edge does not heal pre-enable
residual) and then assert: mismatches detected, rows pulled/applied, RF
coverage restored, the missed DELETE converges to the tombstone, per-cycle
budgets respected, and no regression of bounded discipline.

**S3 — bootstrap + range iterator (the ownership edge).** Range API,
bootstrap triggers/cursors, g8 replacement, read-trigger. Verify: range
iterator equivalence with the old full scan (golden), interrupted/resumed
bootstrap, new-owner/gap/pool-loss triggers, read-trigger serviced and
negative-cache behavior. f4: **P3** (metadata pool loss) re-run validates the
replacement g8 path end to end; f4's P5 attach does **not** change ring
ownership, so new-owner behavior is not covered there. The N>RF matrix below
is the S3 acceptance gate.

**S4 — spot-check + hardening.** HMAC sampler/verifier and the mixed-mesh,
epoch, and fault-injection hardening. Verify: injected silent row loss
(bypassing the journal) is detected within N cycles and repaired; mixed
enabled/disabled mesh; docs. f4 has no silent-loss injection, so this stage
is validated locally, not on the f4 fleet.

### Mandatory N>RF test matrix

f4 is N=3 with RF=3 full replication: **it cannot validate any partial-ownership
behavior.** These tests are the hard acceptance gate for S3 and require a
fleet with N ≥ 4 (RF = 3) or an equivalent in-process weighted-ring harness:

| # | Test | Assertion |
|---|---|---|
| 1 | New owner joins (N→N+1) | Owned ranges bootstrapped via triggered, windowed reads; node serves reads for those keys; no scheduled full scan appears |
| 2 | Mutation filtering under partial ownership | Only co-owned keys are consumed/applied (`keys_skipped_total{not_owner}` > 0); non-owned keys untouched |
| 3 | Peer down within retention | Watermark catch-up replays exactly the missed change records; no bootstrap; mutation ordering preserved |
| 4 | Peer down beyond cap/age | `gap_detected_total` fires; node bootstraps its owned ranges; no resurrection; bounded per-window reads only |
| 5 | Trim safety with a stuck peer | Trim frontier stalls at the retained-Dead peer; journal ≤ hard cap; after cap, gap→bootstrap |
| 6 | Read-trigger with foreign refs | Row absent on one owner, bytes elsewhere; read served via point fetch + LWW apply; subsequent reads hit locally |
| 7 | Tombstone safety under N>RF | Missed DELETE converges to the tombstone; no stale live row survives any bootstrap path |
| 8 | Mixed kill-switch mesh | Enabled nodes converge among themselves; disabled nodes are skipped; re-enable triggers gap/catch-up; no storm |
| 9 | Crash matrix | Kill between journal fsync and row commit across every capture point; recovery is consistent, no lost acked write |
| 10 | Bootstrap/resume under ring churn | A ring change mid-bootstrap invalidates and recomputes windows; no false repair, no stuck cursor |

---

## References

- Rejected predecessor: `docs/adr/0037-metadata-anti-entropy-detection.md`
  (O3 scan; kept for the gap analysis and correctness rules).
- Gap/design capture: `docs/features/metadata-anti-entropy/design-draft.md`;
  stale spec (O3): `docs/features/metadata-anti-entropy/pr3-metadata-anti-entropy.md`.
- Residual statement: `review/cluster-churn-resolution-2026-09-10.md:72-73`.
- Acceptance harness: `docs/features/fleet-degradation/f4-pool-degradation-under-load.md:155-184,309-311`;
  baseline artifacts: `docs/features/fleet-degradation/artifacts/f3-control-dirty-20260911.json:36-77`,
  `…/f3-full-run3-20260911.json:36-65`.
- Store/choke points: `crates/oceanfs-storage-api/src/metadata_store.rs:78-278`;
  `crates/oceanfs-storage/src/metadata/store.rs:529-692,1133-1178,1481-1529,1569-1616`;
  CF layout `crates/oceanfs-storage/src/metadata/cf.rs:14-148`.
- Apply/LWW: `crates/oceanfs-server/src/grpc/segment_service.rs:655-803`;
  `crates/oceanfs-server/src/write/coordinator.rs:1151-1364`.
- Hints: `crates/oceanfs-durability/src/hinted_handoff/mod.rs:57-61,163-182`;
  `hint_delivery.rs:71-89,246-274,424-455`; `hint_wal.rs:298-330`;
  config `crates/oceanfs-core/src/config/node.rs:387-424,668-682,779-782`.
- Repair/terminal: `crates/oceanfs-durability/src/repair.rs:332-363`,
  `crates/oceanfs-durability/src/reconcile.rs:105-162`.
- g8 rebuild/lister: `crates/oceanfs-node/src/modules/metadata_recovery.rs:42-89,210-291,332-397`;
  wire `proto/oceanfs/healing.proto:67-128,248-266,376-406`.
- Scheduler/keyspace_fraction: `crates/oceanfs-durability/src/scheduler/mod.rs:15-34`,
  `adaptors.rs:183-228`.
- ADRs: 0015, 0017 (2026-09-06 amendment), 0023, 0025, 0027, 0028, 0029,
  0030, 0033, 0034, 0035.
