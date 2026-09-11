---
epic: "fleet-degradation"
status: proposed
priority: high
created: 2026-09-11
updated: 2026-09-11
---

# Fleet Degradation Testing (Phase 4 class) — Epic Plan

Epic: `fleet-degradation`
ADRs: [ADR-0026](../../adr/0026-phase3-dedicated-node-vms.md) (fleet topology),
[ADR-0029](../../adr/0029-storage-pools-disk-resilience.md) (pools),
[ADR-0030](../../adr/0030-re-replication-target-pull.md) (target-pull),
[ADR-0031](../../adr/0031-remove-single-datadir-legacy-mode.md) (pools mandatory),
[ADR-0035](../../adr/0035-replicated-segment-lifecycle-state.md) (wal/metadata
recovery), [ADR-0036](../../adr/0036-phase-c-scale-ops-drain.md) (drain/detach),
[ADR-0019](../../adr/0019-test-harness-topology-cost-guardrails.md) (guardrails,
retained; Decisions 1 & 4 superseded for Phase 3+).
Related: [test-harness README](../test-harness/README.md),
[disk-resilience-scale epic](../disk-resilience-scale/epic.md).

## Goal

Make the **fleet itself the failure-injection substrate** and prove that
OceanFS survives real-hardware-class degradation under sustained load:

1. **Real block devices for every pool role.** Phase 4+ runs on Hetzner Cloud
   Volumes — one per role (data / wal / metadata / hints), mounted at a stable
   mount path per node (opt-in `--volume-pools` provisioning flag; Phases 2/3
   stay on local disk). This turns "kill the disk" from a `dd`-file trick into
   a real device disappearance and makes pool role isolation a physical fact,
   not a config fact.
2. **SSH black-box fault injection.** The harness drives remote nodes over
   SSH (`echo 1 > /sys/block/<dev>/device/delete`, rescan, `fallocate`,
   `tc qdisc` on the internal interface) — **no test-only product hooks**.
3. **Hard failures first.** Device yank/replug + disk-full for all four pool
   roles, plus the live and boot WAL-recovery paths, metadata-pool loss
   recovery (objects + deletions), data-pool loss with replica-served reads
   and orphan-reaper/GC safety, and the Phase C dynamic ops under load
   (attach / drain / pause / resume / detach / node-drain→leave).
4. **Turn Phase C correctness into fleet evidence.** The d0–d5 drain/detach
   machinery has only in-process node integration coverage. This epic
   produces the fleet data that the deferred C2a/C2b decision
   (`disk-resilience-capacity`) is explicitly gated on, and closes the
   recorded `oceanfs_drain_*` counter residual where the data requires it.

**Explicit non-goal of this epic (deferred, not forgotten):** *soft*
degradation injection (`dm-flakey` error injection, `dm-delay` latency on a
pool device) and any network-partition (`iptables`) work. Rationale: hard
failures exercise the *existing* recovery machinery (g7/g8, ADR-0035,
ADR-0036, orphan reaper) with zero new DM targets or kernel-module
dependencies; soft-degradation semantics (Degraded vs Dead detection,
trend-based health) deserve their own feature on top of a proven hard-failure
baseline. See [Sequencing rationale](#sequencing-rationale-locked-with-the-user-recorded) below.

**No performance claims.** Volume-backed runs cross the network-block-storage
path. Every result in this epic is a **correctness / degradation** result.
Throughput and latency numbers from these runs are **not comparable** to
local-disk or Phase 2/3 runs and are never asserted on. The `dm-delay` /
`dm-flakey` soft-degradation follow-up, if built, inherits the same rule.

## Code-grounding facts (verified 2026-09-11)

These are the facts this epic's features are written against. Each feature
re-verifies its own subset at implementation start.

1. **Fleet topology is live and parametric.** `scripts/vm-provision.sh`
   provisions N node VMs + CX43 harness for phases 3-4 (ADR-0026), records
   `.hetzner/provision-<prefix>.json` with `sut_nodes[]` (the `sut` single
   object is Phase 2 legacy), and `scripts/sut-deploy.sh --cluster` deploys one
   `oceanfs` per VM. **No volume support exists in either script.**
2. **Pool roots are currently directories on the node's local disk** —
   `${DATA_DIR}-pools/pool-{data,wal,meta,hints}` under `/var/lib/oceanfs`
   (`sut-deploy.sh`), with `data_dir = /var/lib/oceanfs`. The e2e harness
   (`NodeProcess::spawn_with_data_dir_and_options`) injects a role-complete
   sibling-dir pool block for local spawns.
3. **`vm-provision.sh --destroy` deletes servers only** — volumes are not
   touched (they do not exist yet). The cleanup-on-failure path likewise only
   tracks servers (`CREATED_VM_NAMES`). A volume-aware `vm-down` is therefore
   a **hard requirement**, because Hetzner volumes are billed while they
   exist **even when detached**.
4. **`RemoteCluster` has no POST-with-body and no general SSH exec.**
   `RemoteNode::post(path)` sends an empty body; the only SSH helper is the
   private `run_ssh(args)` used by `kill_and_restart_node_via_ssh`. The admin
   API's `POST /admin/pools` (attach) requires a `StoragePoolConfig` JSON
   body, and drain begin accepts an optional `{"mode": …}` body — both
   unreachable from the current harness surface.
5. **Local injectors (`e2e/src/load/degrade.rs`) are all local-process.**
   `inject_latency`/`remove_latency` run `tc` on `lo` (affects every node on
   the same host); `fill_disk` fills the node's local `data_dir` via
   `dd`/`df`; `corrupt_shard` walks the local filesystem and overwrites 64
   random bytes; `corrupt_and_verify_heal` orchestrates write→corrupt→AE→poll.
   `FailureInjectionRecord { timestamp, injection_type, node_index, success,
   detail }` already exists and serializes into the report.
6. **`GET /admin/segments` returns aggregate counts only** (`total`, `sealed`,
   `unsealed`, `encoding`, `by_tier`) — **it does not list segment ids**, so
   segment-id targeting cannot come from it unchanged. On-disk segment files
   are `{data-pool-root}/{segment_id}.dat` (`data_store.rs`, `segment_reader.rs`,
   `sealer.rs`), so a remote listing of `.dat` basenames on the target role
   volume is the black-box way to obtain a live segment id.
7. **The pool/drain admin surface is implemented** — `GET /admin/pools`,
   `POST /admin/pools` (attach), `POST /admin/pools/{id}/drain` (+ pause/
   resume), `POST /admin/pools/{id}/detach`, `POST /admin/nodes/{node}/drain`
   (+ pause/resume), `POST /admin/wal-remount`, `GET /admin/segments`,
   `POST /admin/scrub`, plus `/admin/health|cluster|ring|metrics`.
8. **Sea-of-tests context.** Pool-ops coverage is in-process node integration
   only: `crates/oceanfs-node/tests/{cluster_drain,intra_node_drain,pool_detach,
   runtime_attach,pool_drain_state,wal_pool_recovery,metadata_pool_recovery}.rs`.
   None of it runs under fleet load, and `load_degraded.rs` **does not exist**
   (`e2e/tests/` has no degraded-mode test).
9. **WAL recovery has two distinct paths and only the boot variant exercises
   the interesting machinery.** `wal_pool_recovery.rs` (module doc) states the
   live `POST /admin/wal-remount` does **not** exercise rebuild-from-holders;
   the boot-with-replaced-device variant is the one that matters
   (ADR-0035 D1/D2).
10. **Metadata loss recovery rebuilds objects AND deletions from peers** over
    the node's owned ring ranges (`metadata_pool_recovery.rs`:
    `metadata_loss_rebuilds_fresh_store_from_peers`, rebuild counters cover
    both; the pre-kill DELETE is honored after rebuild). Segment data is not
    re-replicated by g8.
11. **The orphan-reaper/GC bug class is real and closed by a known fix.**
    Commit `4c0d216` ("fix(gc): orphan reaper double-counted dead chunks —
    stop reaping live segments") is the class of bug the data-pool-loss
    scenario must actively try to re-trigger: a failed/absent pool must not
    make the reaper delete live data.
12. **Phase C residuals are recorded and open:** the d4 `oceanfs_drain_*`
    throughput counters were **not registered** (`DrainCycleStats` is the
    observability; "close or document before the epic DoD"), the no-op
    `leave(None)` ending was not exercised by the integration tests, and the
    C2a/C2b decision is **gated on fleet/load-test data**.
13. **The existing Phase 4 feature doc is written for a superseded
    topology.** `docs/features/test-harness/test-phase-implementations/
    phase4-degraded-mode-test/feature.md` describes 3 co-located processes on
    one SUT VM (ADR-0019 Decisions 1/4), which ADR-0026 supersedes for
    Phase 3+, and it was `status: proposed` with no code ever landed
    (`load_degraded.rs` absent). This epic marks it `cancelled` and replaces
    it with f3 (2026-09-11). Two drift findings carried into f3: it cites
    `POST /admin/trigger-anti-entropy`, which does not exist (use
    `POST /admin/scrub`), and unsuffixed metric names that are registered with
    `_total` suffixes in code (verified 2026-09-11).
14. **PIPELINE §6.** Load suites run **only** on the cloud harness:
    `vm-up → vm-deploy → vm-test-phase`. No local run of any `load_*` suite,
    ever. This epic's tests are load suites.
15. **The hints path silently loses debt and its health has no producer
    (verified 2026-09-11).** Hint WAL uses raw `std::fs` with no
    `IoObserver` (`hinted_handoff/hint_wal.rs:21-22`;
    `oceanfs-node/src/modules/durability.rs:480`); pool health only reflects
    observed I/O, so an idle/dead/full hints device can stay `Healthy`;
    `hints_pool_accepts()` rejects only `Dead` and the rejection is a
    counter-less warn (`write/coordinator.rs:1279-1307`), and
    `enqueue_write_hint` swallows enqueue failures (`:1354-1366`). The
    2026-09-11 grounding session resolved OQ c and produced **f0** (the
    conditional gate + observability pre-epic). Full citations in f0's
    Grounding section.

## Feature DAG

```
f0 hints-durability-gate  (pre-epic, product — DONE; gates f4's P4 only)

f1 volume-backed-fleet-topology  (DONE 2026-09-11, review iteration 3 PASS)
 └── f2 remote-fault-injectors
      ├── f3 load-degraded-fleet
      └── f4 pool-degradation-under-load   ◀── f0

f4 rerun ──▶ metadata-anti-entropy (deferred, post-epic; own directory)
```

Implementation order: **f0 landed first (done 2026-09-11, review iteration 3
PASS; it had no fleet dependency and could run in parallel with f1/f2, but
MUST land before f4's hints scenario); f1 landed second (done 2026-09-11,
review iteration 3 PASS), so the remaining order is f2 → f3 → f4.** f3 and
f4 both build on f2; f3 proves the injector substrate on the four adapted
scenarios, f4 spends it on the pool-role matrix. The sysfs-yank gate and the
volume-record/destroy path were
**f1-internal early gates** — both cleared (see
[f1 Deviations](f1-volume-backed-fleet-topology.md#deviations-accepted));
they no longer block the epic.
Metadata anti-entropy is **not an epic feature**: it is a deferred product
workstream (own directory) whose acceptance is the f4 rerun after f0.

| # | Feature | Status | Priority | Depends on | Deliverable in one line |
|---|---|---|---|---|---|
| f0 | [hints-durability-gate (pre-epic)](f0-hints-durability-gate.md) | done | critical | — | **Product pre-epic**: hints-root observability (observer/probe; ENOSPC detected; `/admin/pools` truthful) + conditional admission gate (no ack without durable debt) + `hinted_handoff_hints_rejected_total` + boot/reopen verdict |
| f1 | [volume-backed-fleet-topology](f1-volume-backed-fleet-topology.md) | done | critical | — | `vm-provision.sh` creates/attaches/records 5 volumes/node (4 pool roles + spare), `sut-deploy.sh` roots pools on their mounts, `vm-down` deletes them; sysfs yank/replug validation gate PASS (hcloud fallback not adopted) |
| f2 | [remote-fault-injectors](f2-remote-fault-injectors.md) | done | critical | f1 | SSH device yank/replug, per-volume disk fill, segment discovery + corruption, network latency on the internal interface, `RemoteCluster` POST-with-body + SSH exec, `FailureInjectionRecord` wiring |
| f3 | [load-degraded-fleet](f3-load-degraded-fleet.md) | proposed | critical | f1, f2 | `e2e/tests/load_degraded.rs` fleet-ready (mid-write VM kill, slow node, disk-full, corruption+heal) + `scripts/run-phase4.sh` (supersedes the old phase4 feature doc) |
| f4 | [pool-degradation-under-load](f4-pool-degradation-under-load.md) | proposed | critical | f1, f2, **f0** | Pool-role hard-failure matrix + dynamic ops under load; produces the C2a/C2b fleet data, targets the d4 counter residual; its P4 asserts the f0 gate contract and its rerun is the AE acceptance harness |
| AE | [metadata-anti-entropy (design draft, deferred)](../metadata-anti-entropy/design-draft.md) | deferred | high | f4 rerun data + detection ADR | The index plane's eventual-repair path: hints are the only row backfill today; segment reconcile guards bytes, not key→segment rows. Detection under ADR-0034 is the ADR-gated hard question. |

## Everything is grounded in a real product surface

| Epic feature | Product surface it exercises (already implemented) | Source |
|---|---|---|
| f1 | Pool topology config (`[[storage.pools]]`, ADR-0031), systemd unit | `sut-deploy.sh`, `vm-provision.sh` |
| f2 | `/admin/pools`, `/admin/segments`, `/admin/scrub`, `/admin/wal-remount`, `/admin/health` | `oceanfs-server/src/admin.rs:907-939` |
| f3 | Crash recovery (WAL), heal/AE, `GET /admin/segments`, `/admin/scrub` | `wal_pool_recovery.rs`, `metadata_pool_recovery.rs`, `cluster_drain.rs` |
| f4 | `POST /admin/pools` attach, pool/node drain (pause/resume), detach, replicated lifecycle (g7/g8) | `crates/oceanfs-node/tests/{runtime_attach,pool_detach,cluster_drain,pool_drain_state,wal_pool_recovery,metadata_pool_recovery}.rs` |

There are **no test-only product hooks** in this epic. Where a black-box
limitation exists (segment id discovery, fact #6), the injector adapts
(remote `.dat` listing) rather than adding a product endpoint.

## Sequencing rationale (locked with the user, recorded)

- **Hard failures first.** Sysfs device death + disk-full exercise the
  existing recovery machinery end to end; they need no new kernel modules,
  no device-mapper configuration, and no changes to the product's health
  classifier. `dm-flakey`/`dm-delay` soft degradation is a **follow-up
  feature** (not in this epic), because (a) Degraded-state semantics in
  ADR-0029 §D3 are trend-based and deserve their own calibrated scenarios,
  and (b) mixing "error injection" and "confirmed loss" in one first pass
  makes failures hard to attribute.
- **Sysfs yank is a gate, not an assumption.** Real Hetzner volumes may not
  support `delete` + host rescan cleanly; f1 validates this on a real volume
  **before** any scenario depends on it and records the fallback decision
  (`hcloud volume detach`/`attach`, which requires the Hetzner token on the
  harness — currently the harness has no token; see f1's decision point).
- **Volume ≠ performance.** Volume-backed runs cross the network block
  storage. A test that asserts a latency bound would be measuring the wrong
  thing. Every feature doc restates the non-comparability rule.

## Failure semantics: hard yank vs graceful path (per scenario)

The distinction below is a **contract**, not guidance. Each scenario doc
states which path it uses and why:

| Path | Mechanism | Filesystem result | Use for |
|---|---|---|---|
| **Hard yank** | `echo 1 > /sys/block/<dev>/device/delete` (or hcloud detach fallback) with the fs mounted | **Unclean fs / I/O errors / stale mount** — intended | Data-pool loss where recovery = heal/replace/format (ADR-0029 D3 Dead semantics) |
| **Graceful** | `umount` → detach → reattach → `mount` | Clean fs (or fs replaced deliberately) | Scenarios where data must **survive** the operation: WAL/metadata device replacement where the boot rebuild must read a fresh/empty device, spare-volume attach/detach, node-level drain cleanup |

Hard yank **must not** be used where the scenario's correctness claim is
"the data on this device survives". Those scenarios use the graceful path and
the device replacement is explicit.

## Cost & quota guard

- **Billing rule (HARD):** Hetzner volumes are billed while they exist,
  **including when detached**. `vm-down` (and `--destroy`) MUST delete every
  recorded volume id, and the provisioning record MUST carry them
  (`volumes[node].{name,id,device,mount,size_gb}`), because there is no other
  way to enumerate them safely later.
- **Quota:** the user's project quota is **1 TB total volumes, no per-volume
  limit**. As implemented by f1 (spare approved — OQ a resolved):
  data 120 GB + wal 20 GB + meta 20 GB + hints 10 GB + spare 60 GB =
  **230 GB/node**; a 3-node fleet = **690 GB** (inside 1 TB with ~330 GB
  headroom); a 5-node fleet = **1150 GB**, over the default 1024 GB cap, so
  per-role sizes or the cap must be chosen explicitly.
- Servers remain governed by the ADR-0019/ADR-0026 guardrails (TTL, cap,
  internal-network-only traffic). Volumes get a destroy-path guard.

## Cross-links: what this epic unblocks / closes

| Consumer | What fleet-degradation provides |
|---|---|
| `disk-resilience-capacity` (backlog epic; C2a vs C2b decision) | **f4** produces the fleet data the deferral is gated on: free-capacity reclaim behavior after attach→drain→detach, capacity skew across heterogeneous volumes, and observable placement/repair-target distribution. This is the missing input for the d6 decision (see `d6-capacity-weighted-ownership.md` §Deferred). |
| d4 residual (`oceanfs_drain_*` counters "close or document before the epic DoD") | **f4** selects observables for drain throughput under load: if drain progress is only observable through `DrainCycleStats` and pool status, f4 records that as the closing documentation; if throughput observability is required for the fleet assertions, f4 registers the counters (its Integration DoD carries the close-or-document verdict into the epic record). |
| d4 residual (no-op `leave(None)` not exercised) | **f4**'s node-drain→leave scenario runs the retirement workflow on a real fleet and asserts the post-leave heal/dispatch behavior, replacing the in-process substitute with fleet evidence. |
| ADR-0035 (wal/metadata recovery) | f4 provides the first **fleet** coverage of both variants (live remount + boot-with-replaced-device) and the metadata objects+deletions rebuild under load. |
| `disk-resilience` ADR-0029 §D3 | f4's hard yanks are the real-world confirmation that Dead is confirmed-loss (device unplug), not a latency artifact. |
| metadata anti-entropy (deferred) | **f4's rerun is the AE acceptance harness.** f0 bounds new divergence (no ack without durable debt) and the P4 run measures residual cold-key divergence after hints-death + peer outage; that data defines the AE acceptance bound. See [metadata-anti-entropy design draft](../metadata-anti-entropy/design-draft.md). |

## Non-goals (explicit, recorded)

- **No product changes for tests.** No test-only endpoints, no `--features
  testing`, no harness hooks in `oceanfs-server`/`oceanfs-node`. If a black-box
  path is impossible, the injector reports it and records the limitation.
  The one product workstream here is **f0**, a runtime *fix* validated by
  the fleet scenarios (honest hints observability + admission) — not a hook
  added for tests.
- **No soft degradation.** `dm-flakey` error injection and `dm-delay` latency
  are a later follow-up feature (recorded here, deferred with rationale).
- **No network partitions / clock skew / multi-failure overlap.** Out of
  scope for this epic; may be separate features later.
- **No performance assertions.** No throughput/latency thresholds of any
  kind, and a stated non-comparability with local-disk runs in every doc.
- **No migration of Phases 2/3 to volumes.** The volume topology is opt-in
  (`--volume-pools`); Phases 2/3 stay on local disk.
- **No per-node fleet tests on the dev machine.** PIPELINE §6.

## Acceptance bar (epic DoD)

- [ ] **f0 (pre-epic):** hints-pool health has a producer (yank/ENOSPC →
      `/admin/pools` non-Healthy within the detection window); the
      conditional gate rejects a write/delete whose debt cannot be durably
      recorded (503 + `hinted_handoff_hints_rejected_total`, never a silent
      ack); writes not needing hints are unaffected; the missing-root/
      reopen verdict is recorded and exercised by f4's P4.
      **Status:** the f0 product work is `done` (review iteration 3 PASS,
      2026-09-11) — this item stays open only for the f4 P4 fleet exercise;
      recorded verdicts are in
      [f0 Deviations (accepted)](f0-hints-durability-gate.md#deviations-accepted).
- [ ] **f1:** a `--volume-pools` provision creates, attaches, mounts and
      records 5 volumes/node (4 pool roles + spare); `sut-deploy.sh` roots
      the four pools on those mounts; `vm-down` deletes every volume;
      `vm-status` shows them; the sysfs yank/replug gate verdict is recorded
      with the fallback decision.
- [ ] **f1:** no orphaned volume can survive a destroy path (record-driven
      delete; verified against `hcloud volume list`).
- [ ] **f2:** every injector is SSH black-box; `RemoteCluster` can POST JSON
      and exec SSH; each injection produces a `FailureInjectionRecord` in the
      report; local-spawn degradations are either supported or explicitly
      skipped with a warning.
- [ ] **f3:** `load_degraded` runs on the ADR-0026 fleet in harness mode and
      passes all four adapted scenarios with zero data loss; the old
      phase4-degraded-mode spec is superseded with no conflicting doc left.
- [ ] **f4:** the pool-role hard-failure matrix runs under sustained load
      with live reads throughout; WAL boot-variant and metadata
      objects+deletions rebuilds are asserted; orphan-reaper safety is
      asserted; attach/drain/pause/resume/detach/node-drain→leave all run
      live with zero restarts and manifest re-gossip observed.
- [ ] **f4:** the result artifact contains the fleet data for the C2a/C2b
      decision and the close-or-document verdict for the d4
      `oceanfs_drain_*` counters.
- [ ] **All:** no performance assertion, no load suite on the dev machine
      (PIPELINE §6), every scenario records its injection records and its
      hard-yank/graceful classification.

## Open Questions (recorded, not silently chosen)

| # | Question | Owner | Blocks |
|---|---|---|---|
| a | ~~**5th spare data volume per node vs 4/node** for runtime attach/detach scenarios. Recommend 5/node (+60 GB spare data → ~690 GB for a 3-node fleet); **PENDING user quota check**. Without it, f4's attach/detach uses loopback files on the local disk (deviation recorded) or is limited to pool-role replacement.~~ **RESOLVED 2026-09-11 (user approved).** Implemented as a 5th volume, role `spare`, default 60 GB (230 GB/node total), mounted at `/mnt/oceanfs-spare` and recorded but not deployed as a pool. A 5-node fleet is 1150 GB — over the default 1024 GB cap, so fleet size/cap must be chosen explicitly. Recorded in [f1 Deviations (accepted)](f1-volume-backed-fleet-topology.md#deviations-accepted). | user (quota) | — (resolved; f4 attach/detach uses the real spare) |
| b | ~~**Filesystem choice** (xfs vs ext4). Proposal: ext4 (Ubuntu default, `e2fsprogs` present, simple) for all four volumes; xfs as an option if large-file/data-volume behavior warrants.~~ **RESOLVED 2026-09-11.** ext4 for all roles + the spare, verified live on Ubuntu 24.04; xfs not adopted. Recorded in [f1 Deviations (accepted)](f1-volume-backed-fleet-topology.md#deviations-accepted). | user + implementer | — (resolved) |
| c | ~~**Hints-pool degradation semantics.** What is "correct" when the hints device dies/reappears is **not yet verified**… All f4 hints assertions must be marked **grounding-required before implementation**…~~ **RESOLVED 2026-09-11.** The grounding session traced the hint WAL, the health producer gap, and the coordinator admission path; the documented D3 hints-Dead consequence had no producer and the ack path silently dropped debt. The answers are frozen into [f0](f0-hints-durability-gate.md) (gate + observability contract) and the rewritten [f4 P4](f4-pool-degradation-under-load.md) (assertions grounded in the gate contract, no longer grounding-required). The implementation-shape decisions were resolved at f0 implementation close (review iteration 3 PASS, 2026-09-11) and are recorded in [f0 Deviations (accepted)](f0-hints-durability-gate.md#deviations-accepted). | implementer (spec grounding) | — (resolved; f4 P4 now depends on f0) |
| d | **Where the new injectors live:** extend `RemoteCluster` in `e2e/src/remote.rs` (recommended, matches existing SSH crash control) vs a new `e2e/src/fleet.rs` module. Recommendation: extend `RemoteCluster` for SSH/POST plumbing + a new `e2e/src/load/fleet_degrade.rs` for scenario-grade injectors. | implementer | f2 code layout |
| e | **Runner shape:** extend `run-phase4.sh` to run f4 (recommendation, matches run-phase3.sh lineage) vs a separate `run-phase4-pools.sh`. | implementer | f3/f4 runner |
