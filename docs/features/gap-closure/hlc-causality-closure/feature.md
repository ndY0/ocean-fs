---
feature: "HLC Causality Closure — Wall Clock, Receive-Merge, and Cross-Node Propagation"
epic: "gap-closure"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/correctness-gaps
    reason: Read-repair and quorum comparison are the primary consumers of receive-merge; they land first so this feature's merge sites are live
adr:
  - 0004-tiered-segment-sizing
perf:
  - "11.1 Atomic counters on hot paths"
  - "2.5 Sharded segment buffers / lock-free hot paths"
created: 2026-08-13
updated: 2026-08-13
---

# HLC Causality Closure — Wall Clock, Receive-Merge, and Cross-Node Propagation

## Summary

Every OceanFS object write carries an HLC timestamp (spec §7.6) and the
default conflict policy is Last-Write-Wins by HLC. But the HLC
*implementation* is a hollow shell: the wall clock is frozen at boot,
the receive-merge rule is dead code, and **every cross-node data path
drops the timestamp** — replicated metadata, tombstones, and hinted
handoff all persist `Hlc::zero()`. Single-node tests cannot see any of
this; in a multi-node cluster it means LWW picks the *most recently
booted node's* writes instead of the *most recent* write, deletes lose
to resurrected writes (or vice versa, nondeterministically), and
concurrent-write tests like T45 pass only by accident.

This feature closes the causality gap end to end: a correct clock, a
live receive-merge rule at every remote-HLC reception site, and
propagation of the original HLC through replication, deletion, and
hinted handoff.

**Foundational.** Phases 3 and 4 (cluster churn, degraded mode) make
assertions that are only meaningful if this lands first:
"timestamps never move backward for the same key" and "HLC resolves to
a single winner" (load-test-campaign.md §4).

> **Relationship to other features:** this feature supersedes work item
> F1 of `refactoring/load-test-harness-fidelity` (which only patched the
> frozen wall clock). Do not implement F1 from that doc independently —
> the authoritative fix is G1 below. `gap-closure/correctness-gaps`
> provides the *consumers* (read repair, quorum comparison); this
> feature provides the *causality substrate* they compare with.

---

## Gap Inventory (all confirmed by code reading, 2026-08-13)

| # | Gap | Evidence | Impact |
|---|---|---|---|
| G1 | `HlcClock::now()` never re-reads the OS clock; wall frozen at boot | `oceanfs-core/src/hlc.rs:123-138`; traced node log: identical `hlc_wall=1786604124760` on every write for 55 s | LWW biased by node boot time |
| G2 | `HlcClock::update()` (receive-merge) has **zero production call sites** | grep: only unit tests call `update()` | Receive rule violated at every remote-HLC reception site |
| G3 | `append_segment` persists replicated metadata with `hlc: Hlc::zero()`, dropping the coordinator's timestamp carried in the request | `oceanfs-server/src/grpc/segment_service.rs:213`; sender sends it (`write/replication.rs:130`, `write/coordinator.rs:507`) | Every replicated object loses its version on the replica → replica-local writes always win LWW |
| G4 | Tombstones are written with `hlc: Hlc::zero()` | `oceanfs-storage/src/metadata/store.rs:418` (`delete_object`); `MetadataOps::delete_object` has no HLC parameter; `DeleteObjectRequest` proto has no hlc field (`proto/oceanfs/segment.proto:81-84`) | Delete-vs-write LWW is undecidable across replicas |
| G5 | Hinted handoff drops the original write's HLC: proto `HintInline`/`HintSegmentRef` have no hlc field; `HintRecord::new_inline/new_segment_ref` take no timestamp; the coordinator calls them without the `hlc` it holds in scope; `apply_inline_hint` persists `Hlc::zero()` | `proto/oceanfs/hinted_handoff.proto:14-52`; `oceanfs-durability/src/hinted_handoff/hint_wal.rs:383,404`; `oceanfs-server/src/write/coordinator.rs:355-386`; `oceanfs-durability/src/healing_service.rs` (`apply_inline_hint`, batched handler builds `timestamp: Hlc::zero()`) | A hint delivered after a newer direct write wins LWW on delivery — late writes resurrect stale data |
| G6 | No `HlcClock` in the read path or gRPC services — they *receive* remote HLCs but cannot merge them | `ReadCoordinator` has no clock field; `SegmentGrpcService::new(3 args)`, `HealingGrpcService::new(3 args)` | Receive rule unfixable without wiring |
| G7 | `LwwResolver` tie-break contradicts spec: doc comment says "tie-break by `node_id`" but equal HLCs accept-local; the trait has no node ids | `oceanfs-core/src/conflict.rs:78-108`; hlc-versioning feature doc: "tie-break by node_id" | Two nodes can mint identical HLCs (same ms, logical 0) for *different* data; resolution is then arbitrary |
| G8 | Delete path sends `DeleteObjectRequest` without HLC; remote tombstones get no timestamp | `write/coordinator.rs:759-791` (`delete`), proto above | Consequence of G4 across the wire |

---

## Design Decision: HlcClock State Layout (replaces G1 + fixes latent update races)

`HlcClock` currently keeps `wall` and `logical` in two independent
atomics. Beyond the frozen wall, this is *racially broken*: `update()`
does a CAS loop on `wall` then a plain `store` on `logical`, so two
concurrent updates can interleave as `CAS(wall)` / `CAS(wall)` /
`store(logical)` / `store(logical)` and **the logical counter moves
backward**. The receive rule is about to become live (G2), which turns
this latent race into a production bug. Fix the layout first.

**Decision:** pack the 96-bit HLC (u64 wall ms + u32 logical) into a
single `AtomicU128` and rewrite `now()`/`update()` as CAS loops. This:

- makes `now()` refresh the wall from the OS clock (closes G1),
- makes `(wall, logical)` advance atomically (closes the race),
- keeps `now()` lock-free (perf guideline: atomic on hot path),
- guarantees every call yields a *strictly greater* timestamp than any
  previously returned one, even across threads,
- keeps the existing `Hlc` struct (96-bit, serde, ordering) unchanged —
  zero API breakage outside `hlc.rs`.

```rust
// crates/oceanfs-core/src/hlc.rs
#[repr(align(64))]
pub struct HlcClock {
    /// Packed: wall_time (u64 ms) in the high bits, logical (u32) low.
    state: AtomicU128,
}

impl HlcClock {
    pub fn new() -> Self {
        let wall = current_time_millis() as u128;
        Self { state: AtomicU128::new(wall << 32) } // logical = 0
    }

    /// HLC local-event rule: l.w = max(l.w, pt), l.c = l.c + 1.
    pub fn now(&self) -> Hlc {
        let physical = current_time_millis();
        loop {
            let cur = self.state.load(Ordering::Acquire);
            let wall = (cur >> 32) as u64;
            let logical = cur as u32;
            let new_wall = wall.max(physical);
            // Overflow guard (4.3e9 events in one ms — practically unreachable,
            // but correctness requires it): bump wall instead of wrapping.
            let new_logical = logical.wrapping_add(1);
            let (w, l) = if new_logical < logical {
                (new_wall.saturating_add(1), 0u32)
            } else {
                (new_wall, new_logical)
            };
            let next = ((w as u128) << 32) | l as u128;
            if self
                .state
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Hlc { wall_time: w, logical: l };
            }
        }
    }

    /// HLC receive rule (local merge of a remote timestamp), plus the
    /// physical merge so the local wall never lags the OS clock.
    pub fn update(&self, received: Hlc) -> Hlc {
        let physical = current_time_millis();
        loop {
            let cur = self.state.load(Ordering::Acquire);
            let wall = (cur >> 32) as u64;
            let logical = cur as u32;
            let new_wall = wall.max(received.wall_time).max(physical);
            let new_logical = if received.wall_time > wall {
                (received.logical as u64).wrapping_add(1)
            } else {
                (logical as u64).max(received.logical as u64).wrapping_add(1)
            };
            // Cap at u32::MAX; bump wall on overflow (same guard as now()).
            let (w, l) = if new_logical > u32::MAX as u64 {
                (new_wall.saturating_add(1), 0u32)
            } else {
                (new_wall, new_logical as u32)
            };
            let next = ((w as u128) << 32) | l as u128;
            if self
                .state
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Hlc { wall_time: w, logical: l };
            }
        }
    }
}
```

Semantics notes:

- `update()` with a *stale* received timestamp still advances the local
  counter (`max(local, remote) + 1`) — every call yields a fresh,
  strictly greater local timestamp, exactly like the current
  implementation, so no caller semantics change.
- `AtomicU128` is stable since Rust 1.72. If the workspace MSRV is
  lower, fall back to a `parking_lot::Mutex<Hlc>` around the whole
  struct — but verify MSRV first; do **not** keep the two-atomics
  layout.

### Considered Alternatives

| Alternative | Verdict |
|---|---|
| Two atomics + `fetch_max` on wall only (fidelity F1 patch) | **Rejected as insufficient.** Closes the frozen wall but leaves the `update()` store race and cross-thread non-atomicity, both of which become live in G2. |
| `Mutex<Hlc>` for everything | Rejected: `now()` is on every-write hot path; guideline §11.1 demands lock-free. A mutex is only acceptable if MSRV forbids `AtomicU128`. |
| Seqlock over two atomics | Rejected: more complex than a single CAS loop for the same guarantees. |

---

## Work Item G1 — Rewrite `HlcClock` (as designed above)

> **Partial implementation already landed (2026-08-13, parallel implementer):**
> the wall-clock refresh half of this design is already in the working
> tree — `now()` calls `self.wall.fetch_max(physical, AcqRel)` per the
> pre-supersession F1 patch of `load-test-harness-fidelity`, and the two
> wall-refresh tests (`clock_wall_tracks_physical_time_after_sleep`,
> `clock_now_refreshes_wall_repeatedly`) exist in `hlc.rs` and pass
> (24 hlc tests green). **The remaining work is still mandated:** the
> two-atomics layout with its `update()` store race is unchanged, and
> G2 makes `update()` live. The implementer must now perform the
> **AtomicU128 rewrite** below; the fetch_max behavior is a property the
> rewrite must preserve (covered by the two existing tests, which must
> keep passing).

**Files:** `crates/oceanfs-core/src/hlc.rs` only. Keep `Hlc`,
`PartialOrd`/`Ord`, `Default`, doc comments, and the existing public
surface.

**Required unit tests** — two of the six already exist (the wall-refresh
pair above). Add the remaining four:

- `clock_wall_never_goes_backward_under_update` — `update(Hlc::new(
  old_wall, 0))` then `now()`; assert strictly increasing.
- `clock_update_merges_remote_wall` — `update(Hlc::new(now+10000,
  42))`; assert next `now()` has wall ≥ now+10000.
- `clock_concurrent_now_all_unique` — 8 threads × 50k `now()` each,
  collect into a `Mutex<Vec<Hlc>>`; assert 400k **distinct** values and
  per-thread strict monotonicity.
- `clock_concurrent_update_and_now_never_duplicate` — 4 threads doing
  `now()`, 2 doing `update(Hlc::new(rand_wall, rand_logical))`,
  100k iterations; assert zero duplicates and per-thread monotonicity.
- `clock_update_equal_wall_bumps_logical_past_remote` —
  `update(Hlc::new(wall, 5))` after clock is at `(wall, 2)`; assert
  result `(wall, 6)` and next `now()` is `> (wall, 6)`.

---

## Work Item G2 — Wire `Arc<HlcClock>` Into Every Receive Site

The clock already exists at the composition root
(`crates/oceanfs-node/src/node.rs:852`, `Arc<HlcClock>`). Distribute
it, and call `update()` wherever a remote HLC enters the node.

| Site | Change |
|---|---|
| `oceanfs-server/src/read/coordinator.rs` | Add field `hlc_clock: Option<Arc<HlcClock>>` + builder `with_hlc_clock(...)`. In `compare_with_quorum` (line ~506, after `let remote_hlc = ...`) and `run_read_repair` (line ~700, same spot): `if let Some(c) = &self.hlc_clock { c.update(remote_hlc); }` |
| `oceanfs-server/src/grpc/segment_service.rs` | `SegmentGrpcService` gains `hlc_clock: Arc<HlcClock>` (constructor arg; update node.rs:1102 call and all test constructors). In `append_segment`, after extracting the first chunk's hlc (see G3): `self.hlc_clock.update(hlc)`. In `put_object_metadata`, after parsing `hlc` (line ~468): `self.hlc_clock.update(hlc)`. |
| `oceanfs-durability/src/healing_service.rs` | `HealingGrpcService` gains `hlc_clock: Arc<HlcClock>` (constructor arg; update node.rs:1110 and test constructors). In `hinted_handoff_single` (line ~122, after parsing hlc): `update(hlc)`. In the batched `hinted_handoff` handler, for each hint with a non-zero hlc (see G5): `update(...)`. |
| `crates/oceanfs-node/src/node.rs` | Pass `hlc_clock.clone()` to the three constructors above (and keep the existing `WriteCoordinator` wiring). |

**Blast radius:** the three constructors' unit-test call sites
(`segment_service.rs` tests, `healing_service.rs` tests, `node.rs`
tests). Signature changes are additive (new parameter); no proto
change in this item.

---

## Work Item G3 — Replicated Metadata Must Persist the Coordinator's HLC

**File:** `crates/oceanfs-server/src/grpc/segment_service.rs`,
`append_segment` (~lines 134-226).

The stream loop already captures `bucket_id`, `object_key`, `size`,
hash, and chunks "from the first chunk that carries them" (lines
155-164) — but **not** `chunk.hlc`. Then metadata is persisted with
`hlc: Hlc::zero()` (line 213).

**Correction:**

1. In the capture block, add:
   `let mut first_hlc: Option<Hlc> = None;` — set from the same first
   chunk: `first_hlc = chunk.hlc.as_ref().map(|p| Hlc::new(p.wall_time, p.logical));`
2. Use it in the metadata construction:
   `hlc: first_hlc.unwrap_or_else(Hlc::zero),` (zero only when the
   request legitimately carried none — e.g. legacy senders).
3. Merge before persisting (G2): `self.hlc_clock.update(hlc)`.
4. Add a `warn!` when a chunk carrying metadata has `hlc: None` — a
   sender that omits HLC is a bug; make it visible.

**Tests** (`segment_service.rs` `#[cfg(test)]`): stream an
`SegmentAppendRequest` with `hlc: Some((1234567, 89))` through the
service backed by an in-memory metadata store; assert the persisted
`ObjectMetadata.hlc == Hlc::new(1234567, 89)` and the service clock
subsequently returns `wall_time >= 1234567`. Also assert `hlc: None`
degrades to `Hlc::zero()` with a warning logged.

---

## Work Item G4 — Tombstones Carry the Delete's HLC

Deletes are local HLC events and must stamp the tombstone (spec
metadata table: `Tombstone: deletion_time, hlc`).

| File | Change |
|---|---|
| `crates/oceanfs-storage-api/src/metadata_store.rs` | `fn delete_object(&self, bucket, key, hlc: Hlc)` — signature change. Add `fn get_tombstone(&self, bucket, key) -> io::Result<Option<Tombstone>>` (needed by G6). |
| `crates/oceanfs-storage/src/metadata/store.rs` | `delete_object` persists `Tombstone { deletion_time, hlc }` with the provided hlc (drop the hardcoded `Hlc::zero()` at :418). Implement `get_tombstone` from `CF_DELETIONS`. |
| `crates/oceanfs-server/src/metadata_ops.rs` | `MetadataOps::delete_object` gains `hlc: Hlc`. |
| `crates/oceanfs-node/src/metadata_adapter.rs` | Adapter `delete_object` passes `hlc` through to the store. |
| `crates/oceanfs-server/src/s3_handler/handlers.rs` | `delete_object` handler stamps `let hlc = state.write.hlc_clock().now();` and passes it to `state.metadata.delete_object(&bucket, &key, hlc)` **and** to the remote replication path (G8). |
| `proto/oceanfs/segment.proto` | `DeleteObjectRequest` gains `oceanfs.common.HlcTimestamp hlc = 3;` |
| `crates/oceanfs-server/src/write/coordinator.rs` | `delete()` (lines 746-791) includes `hlc` in the `DeleteObjectRequest` (it already has `self.hlc_clock`). The S3 handler passes the same `hlc` it stamped locally so all replicas converge on one tombstone timestamp. |
| `crates/oceanfs-server/src/grpc/segment_service.rs` | `delete_object` handler parses `req.hlc`, merges it (G2), passes it to `md_store.delete_object(bucket, key, hlc)`. |
| Test implementors | `MockMetadata` (s3_handler tests), `TestMetadata` (node/tests/read_write_roundtrip.rs), `InMemoryMetadata` (node/tests/e2e_single_node.rs): update signatures. |

**Tests:** tombstone persists the stamped hlc; remote delete via gRPC
carries the hlc; deleting twice stamps monotonically increasing hlc.

---

## Work Item G5 — Hinted Handoff Preserves the Original Write's HLC

| File | Change |
|---|---|
| `proto/oceanfs/hinted_handoff.proto` | `HintInline` gains `oceanfs.common.HlcTimestamp hlc = 5;`; `HintSegmentRef` gains `... hlc = 7;` (field numbers 1-4 and 1-6 are taken). Regenerate stubs via the existing build.rs pipeline. |
| `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs` | `HintRecord::new_inline(..., hlc: Hlc)` and `new_segment_ref(..., hlc: Hlc)` gain the parameter and populate the proto field. |
| `crates/oceanfs-server/src/write/coordinator.rs` | The hint-creation block (lines ~353-386) passes the write's `hlc` (already in scope at line 354's `let hlc = self.hlc_clock.now();`) to both constructors. |
| `crates/oceanfs-durability/src/healing_service.rs` | Batched `hinted_handoff` handler: extract hlc from each proto hint (`hlc: Option<HlcTimestamp>`); build the legacy `crate::HintRecord` with `timestamp: hlc.unwrap_or_default()` (zero for replayed legacy records); `apply_inline_hint` gains an `hlc: Hlc` parameter and persists it in the `ObjectMetadata` instead of `Hlc::zero()`; merge into the clock (G2). |
| `hinted_handoff/mod.rs` | The legacy in-memory `HintRecord.timestamp` already exists and is already shipped in `HintRequest.hlc` (mod.rs:437) — no change beyond what the healing service consumes. |

**Migration:** WAL files written before this change contain records
without hlc → proto3 default zero → delivered as `Hlc::zero()`
(today's behavior, safe but unordered). Document in the HintWal replay
comment. No on-disk format bump needed.

**Tests:**

- `hint_wal.rs`: roundtrip preserves hlc; a pre-change-format record
  (constructed without hlc) replays as zero.
- `healing_service.rs`: batched inline hint intended for self applies
  with the original hlc; assert the metadata store received
  `hlc == original`.
- `coordinator.rs` (write): the enqueued hint record carries the
  write's hlc (capture via a mock delivery client).

---

## Work Item G6 — Delete-vs-Write LWW at the Repair-Push Boundary

**File:** `crates/oceanfs-server/src/grpc/segment_service.rs`,
`put_object_metadata` (lines 438-513).

Today the tombstone guard is boolean: any tombstone rejects the push
(t19 hardening). With real tombstone HLCs (G4) the decision becomes
order-aware:

1. Fetch `get_tombstone(bucket, key)` instead of `has_tombstone`.
2. If a tombstone exists:
   - incoming hlc **>** tombstone hlc → the write happened after the
     delete: **allow** the push and remove the tombstone (the object is
     legitimately resurrected by a newer write).
   - otherwise (incoming ≤ tombstone, or incoming is zero) → **reject**
     with `failed_precondition`, exactly as today.
3. Zero-HLC pushes (legacy senders, un-migrated hints) keep today's
   reject-if-tombstoned behavior — a zero timestamp never resurrects.

**Tests:** push-newer-than-tombstone succeeds and clears the tombstone;
push-older/equal rejected; push-zero rejected; t19 scenario (delete
after read, repair push of pre-delete data) still rejected — run the
full t19 e2e test.

---

## Work Item G7 — LWW Tie-Break by Node ID (Spec Compliance)

**File:** `crates/oceanfs-core/src/conflict.rs`.

Spec (hlc-versioning feature, spec §7.6): equal HLCs tie-break by
node id. The current `ConflictResolver::resolve(&Hlc, &Hlc)` cannot do
that, and `LwwResolver` silently accepts-local on ties.

**Correction (minimal blast radius):** change the trait method to

```rust
fn resolve(
    &self,
    local: &Hlc,
    remote: &Hlc,
    local_node: &NodeId,
    remote_node: &NodeId,
) -> Resolution;
```

`LwwResolver` compares HLCs; on equality, the **lexicographically
greater node id** wins (`AcceptRemote` when `remote_node > local_node`
by `NodeId` ordering — document the exact comparison used, e.g.
`remote_node.as_str() > local_node.as_str()`).

Call sites to update:

- `read/coordinator.rs` `compare_with_quorum` (line ~506) and
  `run_read_repair` (line ~700): pass `&self.node_id` and `&target`.
- `read/repair.rs` `perform_read_repair` (dead code, `#[allow(dead_code)]`):
  **grep for references first**; if none (expected — superseded by
  `run_read_repair`), delete the module per the perf-network audit M8
  recommendation; if referenced, update the signature.

Update the doc comment on `LwwResolver` to state the tie-break
explicitly. All existing `conflict.rs` tests update to pass node ids;
add `lww_resolver_equal_hlc_higher_node_id_wins` and its inverse.

---

## Work Item G8 — Delete Replication Carries HLC (folded into G4)

See G4 table: `DeleteObjectRequest.hlc` + coordinator + gRPC handler.
No separate work beyond G4 — listed separately in the inventory for
auditability.

---

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `hlc.rs`: AtomicU128 rewrite (now/update); `conflict.rs`: trait signature + LwwResolver tie-break |
| `oceanfs-storage-api` | `delete_object` signature; +`get_tombstone` |
| `oceanfs-storage` | RocksDB tombstone hlc + `get_tombstone` |
| `oceanfs-server` | `segment_service` (G2/G3/G4/G6), `read/coordinator` (G2), `read/repair` (G7 delete), `write/coordinator` (G4/G5/G8), `s3_handler/handlers.rs` (G4), `metadata_ops.rs` trait (G4) |
| `oceanfs-durability` | `healing_service` (G2/G5/G6-adjacent), `hinted_handoff/hint_wal.rs` (G5), proto stubs regen |
| `oceanfs-node` | Composition root wiring (G2); `metadata_adapter.rs` (G4) |
| `proto/` | `segment.proto` (G4), `hinted_handoff.proto` (G5) |

## Migration Path & Breakage

- **Wire compatibility:** all proto changes are additive optional
  fields — mixed-version clusters interoperate. During a rolling
  upgrade, older nodes ignore the new hlc fields (zero-timestamp
  behavior = today's). Full causality guarantees apply only once all
  nodes run fixed code. Document this window in the ADR notes.
- **Trait changes** (`MetadataOps`, `MetadataStore`, `ConflictResolver`)
  are compile-time breaking; all implementors are in-workspace and
  updated in the same change (list in G4/G7).
- **Stored data:** existing object metadata and tombstones with
  zero HLC remain zero. Zero loses LWW against new writes (strictly
  greater timestamps) — acceptable; a backfill is not feasible.
- **Hint WAL:** old records replay as zero (see G5).

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` workspace-wide
- [ ] **Code:** `cargo clippy --lib -- -D warnings` clean on
      `oceanfs-core`, `oceanfs-storage-api`, `oceanfs-storage`,
      `oceanfs-server`, `oceanfs-durability`, `oceanfs-node`
- [ ] **Tests:** `cargo test -p oceanfs-core` green including the 6 new
      HLC tests and updated `conflict.rs` tests
- [ ] **Tests:** `cargo test -p oceanfs-server --lib` green (write
      coordinator, segment service hlc round-trip, quorum comparison)
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib` green (hint
      WAL hlc round-trip, healing service hint-apply hlc)
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      green (tombstone hlc persistence, get_tombstone)
- [ ] **Integration:** 3-node e2e: PUT on node A; assert node B's
      replicated `ObjectMetadata.hlc` equals the coordinator's stamped
      hlc (not zero), and node B's clock afterwards returns wall ≥
      the write's wall
- [ ] **Integration:** T45-style concurrent same-key writes from two
      nodes: all nodes converge on the same winner; assert winner hlc
      is the max across replicas
- [ ] **Integration:** t19 (delete vs read repair) and t21 (hinted
      handoff delivery) e2e tests still green
- [ ] **Integration:** full cluster e2e suite green
      (`cargo test -p e2e --test cluster_*`)
- [ ] **Observability:** node log during a 2-node run shows `hlc_wall`
      values that advance over wall-clock time on both nodes
- [ ] **Docs:** `HlcClock` doc comments document the receive rule and
      the tie-break; `LwwResolver` doc states node-id tie-break;
      HintWal replay comment documents legacy zero-hlc records

## Open Questions

1. **Merge/CRDT resolution:** `Resolution::Merge` remains unimplemented
   everywhere. Out of scope — only LWW needs the substrate built here.
2. **Anti-entropy object-level conflict:** AE compares segment Merkle
   trees, not object HLCs; divergent object metadata across replicas is
   reconciled by read repair, not AE. If AE should also reconcile
   metadata, that is a separate feature.
3. **MSRV:** confirm `AtomicU128` availability (stable ≥ 1.72) against
   the workspace MSRV; fallback design documented in the Design
   Decision section.
