---
feature: "Membership Stability Fixes — SWIM, Rejoin Address Change, Delete Replication"
epic: "refactoring"
status: done
priority: critical
owner: ""
dependencies: []
adr:
  - 0022-rejoin-changed-address-incarnation-bump
perf: []
created: 2026-08-12
updated: 2026-08-13
---

# Membership Stability Fixes — SWIM, Rejoin Address Change, Delete Replication

## Summary

Result of the 2026-08-12 e2e debug session (full suite run with
`--no-fail-fast` and per-node debug tracing). The session produced:

- 7 failing test targets / 10 failing tests, reduced by isolation re-runs to
  **6 deterministic failures**: `t24` (SWIM suspect on kill), `t21` (hinted
  handoff delivery), `t43` (crash rejoin), `t19` (delete propagation),
  `garbage_collection`, `segment_lifecycle` — plus 3 contention-only flaky
  tests (`t5`, `t23`, `t26`) that pass in isolation.
- Full root-cause analysis with log evidence (see
  [debug-session-2026-08-12.md](../../audits/debug-session-2026-08-12.md) if
  present, otherwise the per-fix evidence below; raw node logs under
  `e2e/target/e2e-logs/`, run log at `/tmp/opencode/e2e-debug-run.log`).
- A new decision: [ADR-0022 — Rejoin with Changed Address —
  Incarnation Bump on Restart](../../adr/0022-rejoin-changed-address-incarnation-bump.md).

This feature bundles five work items (F1–F5) that together restore a green
e2e suite. **F1 and F2 are the critical path** — every cluster-level failure
traces back to the membership state machine.

> **Note on gap-closure claims:** `docs/features/gap-closure/README.md:277-280`
> lists T21/T43/T24/T26 as "→ Pass". The debug session proved T21/T43/T24
> fail deterministically at the time; those rows are now accurate for the
> fixed code (see Definition of Done item 7, updated 2026-08-13).

---

## Work Item F1 — SWIM State Machine Correctness (critical)

**Evidence.** Three distinct defects, each with direct log proof:

1. **Join-time false Suspect.** `.tmp07Toa7-node-0-*.log` (t5): node-1 joins
   `Alive` at `18:41:54.085`; **8 ms later** the first gossip push fails with
   a transient `transport error` and node-0 immediately marks node-1 Suspect,
   logging `WARN mark_suspect: node not found in alive_nodes, using default
   incarnation`. The `AddNode(Alive)` had not been applied yet.
2. **Suspect never recovers on successful ping.** `.tmp3PBoxu-node-0-*.log`
   (t19): node-1 marked Suspect at join (`18:43:11.334`) and **stays Suspect
   despite dozens of successful pings** (`12.37–12.68`, `direct ping succeeded
   — target is alive`). No Suspect→Alive transition exists.
3. **Dead↔Alive oscillation loop.** `.tmpGFlgrw-node-0-*.log` (t24): Suspect
   at `31.387` is clobbered by `enqueued AddNode Alive` **40 ms later** (a
   gossip merge from node-1, whose view still lists the killed node-2 as
   Alive); after `node declared DEAD` at `33.43`, `ring: removed dead/left
   node` and `enqueued AddNode Alive` alternate every ~6 ms forever. The
   Suspect window (40 ms) is invisible to the test's 500 ms poll → t24 fails
   deterministically. Commit `f9e62ec` ("prevent SWIM timer reset and gossip
   DEAD revival") is demonstrably incomplete.

**Changes.**

| # | File / function | Change |
|---|---|---|
| F1a | `crates/oceanfs-membership/src/failure_detector/suspicion.rs:19` `mark_suspect` | **Guard:** if the node is not present in `alive_nodes` (a new joiner whose `AddNode` has not been applied, or a node already removed as Dead), do **not** create a suspicion timer and do **not** emit a Suspect event. Drop the `Incarnation::new(1)` fallback and its WARN. A node that was never known-Alive cannot be suspected. |
| F1b | `crates/oceanfs-membership/src/failure_detector/mod.rs:112-128` (`PingResponse`/`IndirectPingResult` success branches) | On a **successful** ping of a node currently in `Suspect`, emit `MembershipEvent { old: Suspect, new: Alive }` (in addition to removing the suspicion timer) so membership and gossip reflect the recovery. Success today only removes the timer — the state stays Suspect forever. |
| F1c | `crates/oceanfs-membership/src/failure_detector/mod.rs` (probe scheduling, `on_ping_tick`) + `crates/oceanfs-membership/src/membership/manager.rs:541` (Dead → ring removal) | When a node is declared Dead and removed from the ring, the failure detector must **drop it from its probe set** (and `alive_nodes`). Today the detector keeps probing the removed node; combined with F1a's old fallback, each failed probe re-created a suspicion timer → `node declared DEAD` re-fires forever. |
| F1d | `crates/oceanfs-membership/src/gossip.rs:346` `merge_delta` | Verify and fix the re-admission guard: a node absent from `state.nodes` (previously removed) may only be re-added when the incoming incarnation is **strictly greater** than the recorded incarnation. Trace the t24 oscillation source (candidates: the `enqueued AddNode Alive` producer on the membership-manager side vs. merge rejection not being honored) and enforce: **after a Dead removal, no path may re-apply Alive for that node id at incarnation ≤ the dead entry's.** |

**Invariant to hold (add as a unit test):**
*If a node id is absent from `state.nodes` and present in `incarnations` with
value `N`, only an entry with incarnation `> N` may (re)insert it.*

**Acceptance criteria.**
- New unit tests in `oceanfs-membership` for each of F1a–F1d (there is an
  existing pattern at `failure_detector/mod.rs:296`).
- `t23`, `t24`, `t25`, `t26`, `t27`, `t5` pass 3× consecutively in isolation
  **and** in the full parallel suite (`--no-fail-fast`).

---

## Work Item F2 — ADR-0022: Rejoin with Changed Address (critical)

**Evidence.** `.tmpN7VHKA-*` (t21): restarted node-2's gRPC port changed
`32987 → 33547`; node-0 accepted the self-announcement but hint delivery
dialed the stale address — `WARN batched hint delivery failed …
forward failed to 127.0.0.1:32987: Connection refused` → object stays 404.
`.tmpsDHLXJ-*` (t43): restarted node-0's gRPC port changed `42455 → 42409`;
node-0 (the bootstrap node, no seeds) starts an empty cluster while peers
dial the dead address → `reports 1 nodes` forever.

**Changes (implements ADR-0022 Decisions 1–4).**

| # | File / function | Change |
|---|---|---|
| F2a | `crates/oceanfs-membership/src/membership/manager.rs:352-406` (join/announce path) | The self-announcement hardcodes `incarnation: 1` (`manager.rs:370`) and `upsert_node(…, Incarnation::new(1), …)` (`manager.rs:406`). Replace with the **persisted incarnation**: on startup read the last incarnation from durable state, announce with `persisted + 1`; first boot (nothing persisted) keeps `1` per spec §13.1. |
| F2b | `crates/oceanfs-membership` (durable state) | Persist the node's last-used incarnation in local durable storage (RocksDB; propose a key, e.g. `membership/self_incarnation` in the existing metadata store, co-located with WAL metadata). Write-through on every bump. Keep the write small and out of the hot path (bump happens once per start, not per ping). |
| F2c | `crates/oceanfs-membership/src/gossip.rs:346` `merge_delta` | Confirm and unit-test: an entry with **strictly higher** incarnation updates both `state` and **`address`** (the insert already carries the address — ensure the removed-node guard of F1d doesn't block a legitimate self-rejoin with `incarnation > N`). |
| F2d | `crates/oceanfs-membership/src/membership/manager.rs` (join loop, ~`manager.rs:348` `JoinFailed("could not contact any seed")`) | **Fallback seeds:** persist the last-known member addresses (same durable state as F2b, updated on membership change) and re-contact them on startup when configured `seed_nodes` are unreachable or empty. This covers the seedless bootstrap-node restart (t43). The primary configured seeds are still tried first. |
| F2e | Call sites (no change needed — verify only) | Hinted handoff, `WriteCoordinator::delete` (`crates/oceanfs-server/src/write/coordinator.rs:681`), and write replication already resolve `membership.address_of()` at send time (`accessors.rs:39`). Once the membership entry updates (F2c), all paths converge. Add a unit test asserting a hint enqueued against a stale address is delivered after the address update. |

**Acceptance criteria.**
- `t21` and `t43` pass 3× consecutively in isolation and in the full suite.
- New unit tests: incarnation persistence round-trip; bump-on-restart; merge
  accepts higher-incarnation address update; fallback-seed rejoin.
- `t8` (incarnation monotonicity) still passes.
- No changes to ring placement or the bootstrap flow (per ADR scope).

---

## Work Item F3 — Delete Replication Honesty (high)

**Evidence.** `.tmp3PBoxu-*` (t19): DELETE on node-0 at `18:43:12.38` logs
`DELETE object success` with **zero replication activity** (compare the PUT
path, which logs `replicating write via gRPC AppendSegment target=…`). No
tombstone ever reaches node-1/2; after 5 s, node-1 returns `200` with the
original body. Contributing factors: (a) node-1 was stuck in Suspect (F1b)
but `address_of` still resolves Suspect members, so the silent-skip must be
elsewhere; (b) `WriteCoordinator::delete` swallows every failure:
`coordinator.rs:702-707` `None => continue`, `Err(_) => continue`, and the
only warn is on the gRPC error itself (`coordinator.rs:725`); (c) the handler
discards the result: `handlers.rs:468` `let _ = state.write.delete(…)`, so
clients always get 204 even when no replica was deleted.

**Changes.**

| # | File / function | Change |
|---|---|---|
| F3a | `crates/oceanfs-server/src/write/coordinator.rs:681` `delete` | Return the **number of successful replica deletions** (or a `Result<usize>`): count `deleted` responses plus the local delete. Log at `debug!` for every replica attempt (target, resolved addr, outcome) and at `warn!` for every skip — remove the silent `continue`s. |
| F3b | `crates/oceanfs-server/src/s3_handler/handlers.rs:456` `delete_object` | Check the result of `write.delete`: when the number of confirmed deletions + local is **below `write_quorum`**, return `503 Service Unavailable` with the existing S3 error envelope instead of `204`. Keep idempotency (an already-tombstoned key still answers `204`). |
| F3c | `crates/oceanfs-server/src/s3_handler/handlers.rs:477-478` | Remove the **duplicated** `invalidate_cache_on_replicas` call (it is invoked twice consecutively — copy-paste). |
| F3d | `crates/oceanfs-server/src/write/coordinator.rs` tests | Unit tests: (1) all replicas reachable → `deleted == replica count`; (2) one replica unreachable → partial count returned, warn logged; (3) ring returns empty replica set → error (existing `Routing` path preserved). |

**Acceptance criteria.**
- `t19` passes 3× consecutively in isolation and in the full suite.
- DELETE against a down replica returns 503 (quorum not met), not a silent 204.
- The duplicate cache-invalidation call is gone.

---

## Work Item F4 — E2E Tests vs Inline-Tier Design (medium)

**Evidence.** Deterministic, not contention-related. Both tests assume "every
PUT creates a segment", but inline-tier objects (≤ 4 KB,
`crates/oceanfs-core/src/types/config.rs:92` `classify`) are stored in
metadata with empty `chunks` — the write path intentionally registers no
segment for them (`crates/oceanfs-server/src/s3_handler/handlers.rs:110-163`),
so `/admin/segments` (built from `list_segments()`) never sees them.

- `garbage_collection.rs`: PUTs 16-byte objects → `baseline.total >= 3` fails
  (0 segments; the log shows 3 successful PUTs each with a generated
  `segment_id`, but no segment records).
- `segment_lifecycle.rs`: `by_tier={"small":1,"standard":2}` — the 13-byte
  inline blob correctly creates no segment, so `by_tier.contains_key("inline")`
  fails. (Secondary observation: the 1.5 MB blob lands in `standard`, since
  `multi` starts > 4 MB — the test should not assume a `multi` entry.)

**Changes.**

| # | File | Change |
|---|---|---|
| F4a | `e2e/tests/garbage_collection.rs:25-30` | Use bodies **> 4 KB** (e.g. 8 KB) so each PUT creates a real segment; keep 3 objects; `baseline.total >= 3` stays. Update the stale comment "each creates its own segment". |
| F4b | `e2e/tests/segment_lifecycle.rs:72-77` | Replace `by_tier.contains_key("inline")` with an assertion that **documents** the inline design: inline blobs create no segment, so assert `by_tier.get("inline").copied().unwrap_or(0) == 0` **and** that the inline object is still readable (already covered by the read-back loop). Keep the `small`/`standard` assertions. Update the file's doc comment to cite the 4 KB threshold. |

**Acceptance criteria.**
- Both tests pass in isolation and in the full suite.
- The assertions encode the design (ADR-0001 four-tier storage), not the old
  "one segment per PUT" assumption.

---

## Work Item F5 — Shard Memory Budget False Positive (low)

**Evidence.** Every node logs
`WARN Shard memory budget exceeds 25% of system memory …
shard_count=8 pool_size_bytes=65536 segment_size_bytes=4194304
total_shard_memory_bytes=2199023255552 system_memory_bytes=16594505728` —
i.e. 2.2 TB "planned" vs 16.5 GB RAM. The arithmetic at
`crates/oceanfs-node/src/startup.rs:42` multiplies `shard_count ×
pool_size_bytes × segment_size_bytes`, which treats `pool_size_bytes` as a
count of segments per shard. 8 shards × 64 KB pool = 512 KB of actual pool
memory; the warning is a config-validation false positive on every boot.

**Changes.**

| # | File | Change |
|---|---|---|
| F5a | `crates/oceanfs-node/src/startup.rs:42` | Correct the budget formula to the intended semantics (confirm with storage config: `shard_count × pool_size_bytes` for the buffer pool, and, if a segment-data estimate is wanted, `shard_count × segments_per_shard × segment_size_bytes` where `segments_per_shard` is a real config value — do not reuse `pool_size_bytes` as that count). Keep the 25% threshold and the warn text. |

**Acceptance criteria.**
- The false-positive warn no longer appears on default config; the warning
  still fires when the budget genuinely exceeds 25% (unit test with an
  absurd config).

---

## Crate Impact

**No crate dependency graph changes.** All edits are internal to existing
crates. The final implementation touched `oceanfs-membership`,
`oceanfs-node`, `oceanfs-server`, `e2e`, plus — added during implementation
(see [Accepted Deviations](#accepted-deviations) §b) — `oceanfs-storage-api`,
`oceanfs-storage`, and `oceanfs-durability`. The DAG constraint in
`guidelines/architecture.md` is unaffected.

| Crate | Change |
|---|---|
| `oceanfs-membership` | F1 (SWIM state machine: suspect guard, Suspect→Alive recovery, dead-node probe cleanup, re-admission guard); gossip push handler routed through `GossipCommand::ReceiveDelta` → `merge_delta`; event emission on address/incarnation change and for new nodes |
| `oceanfs-node` | F2b (incarnation + fallback-seed persistence as TOML in `membership_state.rs`, atomic write); join moved after gRPC bind; hinted-handoff delivery retry (5×500 ms); F5 (budget arithmetic) |
| `oceanfs-server` | F3 (delete quorum + logging + duplicate-call removal); read-repair sender-side re-validation in `run_read_repair`; tombstone gate in segment-service `put_object_metadata` |
| `oceanfs-storage-api` | New `MetadataStore::has_tombstone` (with default impl) for the read-repair resurrection gate |
| `oceanfs-storage` | `RocksDbMetadataStore::put_object_in_bucket` clears the tombstone on a fresh PUT |
| `oceanfs-durability` | Inline hinted-handoff apply in `HealingGrpcService`; F2e hint-apply test |
| `e2e` | F4 (test expectation fixes); debug-tracing harness already delivered |

## Migration Path

- **No data migration.** The incarnation persistence state (F2b — TOML file
  `{data_dir}/membership_state.toml`) is created on first run after upgrade;
  a node that has never persisted it starts at 1.
- **Behavioral change (visible):** after any node restart, peers may observe
  that node's address updated and its incarnation increased by ≥ 1. This is
  the intended semantic per ADR-0022 and is monotonic (T8 unaffected).
- **Rollback:** reverting the merge-rule change (F1d/F2c) restores the old
  (buggy) behavior with no state damage; the persisted membership-state file
  is inert if unused.
- **No spec changes required.** Spec §13.1 stays as-is; ADR-0022 records the
  clarification. If the spec team wants §13.1 to mention rejoin semantics,
  that is a follow-up edit outside this feature.

## Accepted Deviations

Deviations from the plan as written on 2026-08-12, recorded at completion
(2026-08-13).

### a. F2b persistence backend: TOML file instead of RocksDB key

F2b is implemented as a small TOML file (`{data_dir}/membership_state.toml`,
written atomically via a temp-file rename) in `oceanfs-node`
(`src/membership_state.rs`) instead of a RocksDB key. Rationale:
`RocksDbMetadataStore` has no generic KV API, and the user chose a no-trait
design: the node loads the persisted incarnation + fallback seeds at startup
and passes them into `Membership::join(incarnation, fallback_seeds)`; it
persists the incarnation bump **before** announcing; and it keeps fallback
seeds fresh via the membership event watcher. No new trait, no storage-crate
KV API, no dependency-graph change.

### b. Crate impact expansion: read-repair resurrection fix

Verifying t19 exposed a read-repair resurrection bug (a read repair fired by
a pre-delete GET re-pushed the object to replicas **after** the tombstone
landed). Fixing it required:

- a `has_tombstone` method on `oceanfs_storage_api::MetadataStore` (with a
  default impl);
- a tombstone gate in the segment service's `put_object_metadata`;
- tombstone clearing in `RocksDbMetadataStore::put_object_in_bucket`;
- an F2e test + hint-apply logic in `oceanfs-durability`.

As a result, `oceanfs-storage-api`, `oceanfs-storage`, and
`oceanfs-durability` were also touched (no dependency-graph changes).

### c. Additional root causes fixed beyond the original F1–F5 list

Each traced with log evidence:

- **Read repair resurrected deleted objects** (t19): fixed with sender-side
  re-validation in `run_read_repair` + the receiver-side tombstone gate
  (authoritative, race-free).
- **gRPC gossip push bypassed merge guards** (t24 oscillation): the handler
  called `Membership::upsert_node` directly, so a peer's stale `Alive`
  clobbered the local `Suspect`. Now routed through
  `GossipCommand::ReceiveDelta` → `merge_delta` (incarnation/terminality
  guards apply).
- **Suspect nodes were never probed again**: the detector + gossip push only
  targeted `Alive` peers, so the F1b Suspect→Alive recovery could never
  fire, and join-time false Suspects escalated to DEAD. Now `Alive|Suspect`
  nodes are probed during the suspicion window.
- **`merge_delta` only emitted events on state change** (t21 stale
  address): a higher-incarnation rejoin keeping Alive→Alive with a fresh
  address never propagated the address. Now emits on address/incarnation
  change too, and new nodes emit events.
- **Join before gRPC bind**: the node joined the cluster at startup step 4
  but bound its gRPC server at step 15, causing join-time false Suspects
  and refused hint deliveries. Join now happens after the gRPC bind.
- **Hinted handoff had no retry and no apply path**: hints were delivered
  only on Alive events with one attempt, and hints stored on the receiver
  were never written to the metadata store. Added bounded retry (5×500 ms)
  in the node's delivery watcher and self-intended inline hint application
  in `HealingGrpcService`.
- **Fallback-seed persistence flaws**: it included the node's own
  (stale-after-restart) address and raced the membership apply step. Now
  incremental (event-address based), self excluded.

### d. Known limitations left for follow-up

- Segment-ref hints (large objects) are still buffered on the receiver but
  not applied — the hint protocol lacks the segment data + HLC.
- Applied inline hints use `Hlc::zero()`.
- The open-gossip trust model (a peer can fabricate a high incarnation) is
  unchanged per ADR-0022.

### e. Baseline note

- `oceanfs-ec` has 3 pre-existing clippy errors
  (`--all-targets --all-features -- -D warnings`) unrelated to this
  feature; all crates touched by this feature are clippy-clean.
- The TSAN load-test CI command's `-Z build-std` variant was not re-run
  (user aborted); the plain `load_concurrency` test passes.

## Definition of Done

- [x] **Full e2e suite green** — `E2E_NODE_LOG_LEVEL=debug
  E2E_CAPTURE_NODE_LOGS=1 cargo test -p e2e --no-fail-fast` passes: 24 test
  binaries, 0 failures (verified 2026-08-13), including `t8`, `t45`, `t19`,
  `t21`, `t24`, `t43`, `garbage_collection`, `segment_lifecycle`, and
  `load_concurrency`.
- [x] **Previously failing tests pass 3× consecutively in isolation** —
  `t24`, `t21`, `t43`, `t19`, `garbage_collection`, `segment_lifecycle`
  verified green.
- [x] **Contention-only flaky tests** (`t5`, `t23`, `t26`) pass in the full
  parallel suite as well as in isolation.
- [x] **New unit tests** for F1a–F1d, F2a–F2e, F3a/F3d, F5a pass — including
  the F2e tombstone/hint-apply tests in `oceanfs-durability` (see Accepted
  Deviations §b).
- [x] **fmt + clippy + docs clean for the affected crates** — `cargo fmt
  --all -- --check`, `cargo clippy --all-targets --all-features -- -D
  warnings`, and `cargo doc` are clean for every crate touched by this
  feature. Caveats recorded in Accepted Deviations §e: `oceanfs-ec`
  (untouched by this feature) has 3 pre-existing clippy errors, and the
  TSAN CI command's `-Z build-std` variant was not re-run (user aborted)
  while the plain `load_concurrency` test passes.
- [x] **`t8` (incarnation monotonicity) and `t45` (HLC concurrent writes)
  show no regressions** — both pass in the final full-suite run.
- [x] **`docs/features/gap-closure/README.md` rows for T21/T43/T24/T26
  updated** to reflect this feature's resolution (rows read "→ Pass"
  citing membership-stability-fixes).
