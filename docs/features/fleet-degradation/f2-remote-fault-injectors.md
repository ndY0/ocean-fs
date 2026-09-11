---
feature: "Remote Fault Injectors (SSH Black-Box, Real Volumes)"
epic: "fleet-degradation"
status: done
priority: critical
owner: ""
dependencies:
  - feature: f1-volume-backed-fleet-topology
    reason: Injectors address devices by the record's volume id/device/mount; sysfs yank needs f1's validation gate verdict
adr:
  - 0026-phase3-dedicated-node-vms
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0035-replicated-segment-lifecycle-state
  - 0036-phase-c-scale-ops-drain
perf:
  - "1.3 Pre-size collections with known capacity (per-node volume/injection maps)"
  - "2.6 Bounded channels for inter-task communication (worker ↔ injection orchestration)"
  - "8.3 spawn vs spawn_blocking (SSH and file I/O never block the async executor)"
created: 2026-09-11
updated: 2026-09-11
---

# Remote Fault Injectors (SSH Black-Box, Real Volumes)

## Summary

Extend the `e2e` harness with the injectors the fleet-degradation scenarios
need, all driven **over SSH against the remote node** (Option A — no
test-only product hooks):

1. `RemoteCluster`/`RemoteNode` plumbing: POST-with-JSON-body and a general
   SSH command helper (the current `post()` sends no body and `run_ssh` is
   private), so the harness can drive `POST /admin/pools`, drain/detach and
   `POST /admin/wal-remount` remotely.
2. A new scenario-grade injector module (`e2e/src/load/fleet_degrade.rs`,
   OQ d) providing: device yank/replug (sysfs delete + rescan per f1's gate
   verdict, hcloud fallback), per-volume disk fill (targeted at one role
   mount, never the node's root fs), segment discovery + corruption targeting
   a pool-role volume (listing `{root}/{id}.dat` over SSH —
   `GET /admin/segments` does not list segment ids), network latency via
   `tc` on the **internal interface** (not `lo`, which is useless on
   dedicated node VMs), and hooks that turn each operation into a
   `FailureInjectionRecord`.

Every injector is a **fleet injector when remote targets + SSH list are
configured** and degrades explicitly (skip with warning, recorded) when the
topology cannot support it — never a silent success.

## Scope

### In Scope

- **`RemoteCluster` / `RemoteNode` extensions (`e2e/src/remote.rs`):**
  - `RemoteNode::post_json(path, body: &serde_json::Value)` and
    `RemoteCluster::post_json(i, path, body)` — content type
    `application/json`; needed for `POST /admin/pools` (attach body is
    `StoragePoolConfig`) and drain mode `{"mode":"cluster"|"intra-node"}`.
  - `RemoteNode::get_text` / a small JSON-decode helper if not already
    covered by `LoadTarget::get` + existing response helpers.
  - `RemoteCluster::run_ssh_command(node_idx, ssh_target, command)` (or a
    public `SshRunner` value): execute a single shell command on node
    `node_idx`'s VM, return `{status, stdout, stderr}` on the blocking pool.
    **Security boundary:** commands are built by the harness from typed
    parameters (device path, mount, pct, segment id) — never a
    caller-supplied arbitrary string from a report/env var.
  - `RemoteCluster::ssh_targets()` accessor + per-node mapping (mirrors the
    existing `node_idx → i-th TARGET_HOST_SSH` contract used by
    `kill_and_restart_node_via_ssh`).
- **New `e2e/src/load/fleet_degrade.rs` (module layout per OQ d):**
  - **Device yank / replug:**
    - `yank_volume(node_idx, role)` — resolve device from the provisioning
      record (or env-provided mapping when running on the harness against a
      record copied by the runner), `echo 1 > /sys/block/<dev>/device/delete`;
      records a `FailureInjectionRecord { injection_type: "device_yank", … }`.
    - `replug_volume(node_idx, role)` — SCSI host rescan (or hcloud
      detach/attach per f1's recorded verdict); verify the device reappears
      with the **same volume id/serial** before returning; record
      `replug_ok`/detail.
    - `device_gone(node_idx, role)` / `mount_usable(node_idx, mount)` probes
      used by scenario assertions.
    - **Fallback branch is a decision, not duplicated logic:** f1 records
      whether sysfs or hcloud is authoritative; this module implements both
      behind one function and selects by the recorded verdict (or an env
      override for the gate run).
  - **Per-volume disk fill:**
    - `fill_volume(node_idx, role, target_pct)` — compute free space of
      `/mnt/oceanfs-<role>` over SSH (`df`), create
      `/mnt/oceanfs-<role>/.fill.bin` with `fallocate` (fallback `dd`);
      target the **role mount only** — never the node's root filesystem and
      never a tmpfs. Returns the fill path for cleanup; `remove_fill`.
    - The harness's own report path stays on the Harness VM tmpfs
      (`/tmp/oceanfs-reports`) — the ADR-0019 Decision 4 rationale still
      applies in the fleet topology.
  - **Segment discovery + corruption:**
    - `list_segment_ids(node_idx, role) -> Vec<String>` — over SSH, list
      `{mount}/{id}.dat` basenames (the on-disk naming contract,
      `data_store.rs`/`segment_reader.rs`/`sealer.rs`); used where the
      scenario needs a live segment id (feature fact: `/admin/segments`
      returns aggregate counts only).
    - `corrupt_segment_bytes(node_idx, role, segment_id, n_bytes)` — resolve
      the path under the role mount, overwrite `n_bytes` random bytes at a
      random offset (reuse `overwrite_random_bytes` semantics; prefer a
      remote `dd`/`python3` one-liner or stream bytes over SSH — decide at
      implementation and document); record `segment_corrupt`.
    - `corrupt_and_verify_heal_remote(...)` — fleet counterpart of the local
      `corrupt_and_verify_heal`: write a known blob, wait for replication,
      corrupt a discovered segment on a chosen node, trigger
      `POST /admin/scrub` and/or wait for AE, poll reads on the corrupted
      node until the original bytes return. **No latency bounds** — this is
      a correctness loop with a generous timeout.
  - **Network latency via `tc` on the internal interface:**
    - `inject_latency(node_idx, iface, delay_ms)` — `tc qdisc add dev
      <internal-iface> root netem delay <delay_ms>ms`; **not `lo`**.
    - Interface resolution: derive from `TARGET_HOSTS`/record (`ip route get
      <peer-ip>`), or accept an explicit `LOAD_TEST_LATENCY_IFACE`. Fail
      loudly if the derived device is `lo` or the default route device is
      wrong.
    - `remove_latency(node_idx, iface)` — `tc qdisc del` (not an error when
      absent).
  - **`FailureInjectionRecord` wiring:** every operation above returns a
    record (or takes `&mut Vec<FailureInjectionRecord>`); the existing type
    gains no new required fields in this feature (backward-compatible
    `injection_type` strings: `device_yank`, `device_replug`, `disk_fill`,
    `segment_corrupt`, `latency`, `latency_remove`, plus existing).
  - **Local-spawn degradations (where feasible):** local-spawn paths remain
    for CI:
    - disk fill → local `data_dir` fill (existing `fill_disk` semantics,
      re-targeted to a role root when configured);
    - corruption → local `corrupt_shard`/`corrupt_and_verify_heal`;
    - device yank → **not feasible** locally (no per-node block device);
      skipped with a recorded warning;
    - latency → not feasible on a shared loopback without contaminating all
      nodes; skipped with a recorded warning (kept local-only behind an
      explicit opt-in, not part of fleet scenarios).
- Tests:
  - unit: `post_json` builds a JSON body with the right content type
    (mockito-free check via a local receiver server or a reqwest
    `httpmock`-style shim if present — decide at implementation);
  - unit: SSH command construction is quoting-safe for device paths,
    mounts and segment ids; no shell injection from report/record content;
  - unit: interface resolution rejects `lo`;
  - integration (fleet): yank → `device_gone` true → replug → device
    present with same serial; fill → `df` on that mount reaches target;
    corrupt one discovered `.dat` → read on that node fails/verifies → heal
    restores; latency → peer-observed RTT increases and is removed
    (functional, no perf threshold);
  - gate: the injector module refuses to claim success when SSH is not
    configured (records `skipped`, `success=false`).

### Out of Scope (for this feature)

- Scenario logic and assertions (`load_degraded`, pool matrix) — **f3/f4**.
- WAL/metadata/data pool *semantics* (what should happen after the
  injection) — **f4**; this feature only moves/deforms the hardware.
- `dm-flakey` / `dm-delay` soft degradation — follow-up feature.
- Network partitions (`iptables`), clock skew, multi-failure overlap.
- Adding any admin/product endpoint for tests (Option A forbids it).
- Volume provisioning/mounting — **f1**.

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | `remote.rs`: `post_json` (node + cluster), public SSH exec returning output, ssh-target accessor. New `src/load/fleet_degrade.rs` (scenario injectors + record wiring); exports from `load/mod.rs`. |
| `scripts` | None in this feature (f1 owns provisioning). `run-phase4.sh` (f3) forwards the record path/env the injectors need. |

## Interface (Public API)

New/changed `pub` items in the `e2e` crate facade (`src/lib.rs` already
re-exports `harness`, `load`, `remote`):

- `RemoteNode::post_json(&self, path: &str, body: &serde_json::Value) ->
  Result<reqwest::Response, Error>` — JSON POST with body.
- `RemoteCluster::post_json(&self, i: usize, path: &str, body:
  &serde_json::Value) -> Result<reqwest::Response, Error>`.
- `RemoteCluster::ssh_targets(&self) -> &[String]` (or the parsed list) —
  per-node SSH targets aligned with `TARGET_HOST_SSH`.
- `RemoteCluster::ssh_exec(&self, node_idx: usize, command: &str) ->
  Result<SshOutput, Error>` where `pub struct SshOutput { pub status: i32,
  pub stdout: String, pub stderr: String }` — runs on the blocking pool.
- New module `e2e::load::fleet_degrade`:
  - `pub struct FleetTarget { node_idx: usize, ssh: String, volumes:
    BTreeMap<String, VolumeRef> }` — built from the provisioning record/env.
    `pub struct VolumeRef { pub role: String, pub id: u64, pub device:
    String, pub mount: String, pub size_gb: u64 }`.
  - `pub struct FleetInjector<'a> { cluster: &'a RemoteCluster, targets:
    Vec<FleetTarget>, records: Vec<FailureInjectionRecord> }` with
    `yank_volume`, `replug_volume`, `device_gone`, `fill_volume`,
    `remove_fill`, `list_segment_ids`, `corrupt_segment_bytes`,
    `inject_latency`, `remove_latency`, `records()` accessor.
  - `pub async fn corrupt_and_verify_heal_remote(...)` — the fleet heal
    loop.
- Existing `FailureInjectionRecord` unchanged (backward-compatible
  `injection_type` strings; no new fields required).
- Local `Cluster` extensions in `degrade.rs` are untouched except where a
  scenario needs a role-root-targeted fill (additive).

## Data Flow

```
scenario (f3/f4)
  └─ FleetInjector::new(&remote_cluster, provisioning_record_or_env)
       ├─ resolve per-node ssh target + volume map (role → {id, device, mount})
       ├─ yank_volume(node, "data"):            ssh "echo 1 > /sys/block/sdb/device/delete"
       │    └─ FailureInjectionRecord{injection_type:"device_yank", success, detail}
       ├─ fill_volume(node, "data", 95):        ssh "df/fallocate /mnt/oceanfs-data/.fill.bin"
       ├─ list_segment_ids(node, "data"):       ssh "ls /mnt/oceanfs-data/*.dat"
       ├─ corrupt_segment_bytes(node, "data", id, 64): ssh overwrite under that mount
       ├─ inject_latency(node, iface, 500):     ssh "tc qdisc add dev <internal-iface> ..."
       └─ records() → LoadReport.injection_records (via scenario)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `e2e`; `post_json`
      and `ssh_exec` are implemented on the real client paths; `fleet_degrade`
      compiles with no `unsafe`, no test-only product hooks, and all shell
      command construction is parameter-typed and quoting-safe.
      *Reviewer: verified independently — `cargo build -p e2e --all-targets`,
      `cargo clippy -p e2e --lib -- -D warnings`, `cargo fmt -p e2e --check`
      all clean; `post_json` serializes via `serde_json::to_vec` + explicit
      `Content-Type: application/json` (remote.rs:63-75); `ssh_exec_on`
      validates the target and runs `ssh` under `spawn_blocking`
      (remote.rs:599-603, 676-695); no `unsafe`/`#[allow]`; only `e2e/`
      changed (no product crates, no test-only hooks).*
- [x] **Tests:** `cargo test -p e2e --lib` passes (unit: JSON body, quoting,
      `lo` rejection, skipped-injection records); the fleet-only integration
      items run on the cloud harness (PIPELINE §6 — never locally) and are
      listed in the feature's review: yank/replug, per-role fill, segment
      discovery + corruption + remote heal, latency add/remove on the internal
      interface.
      *Reviewer (iteration 2, commands re-run): `cargo test -p e2e --lib` =
      163 passed / 0 failed; `cargo test -p e2e --doc` = 57 passed / 0 failed;
      `--test fleet_injectors` skips locally (no `TARGET_HOSTS`) and passes;
      `cargo build -p e2e --all-targets`, `cargo clippy -p e2e --lib --
      -D warnings`, `cargo fmt -p e2e -- --check`, and
      `RUSTDOCFLAGS="-D warnings" cargo doc -p e2e --no-deps` all clean.
      Judged the fleet run from `artifacts/fleet-injectors-2026-09-11.json` +
      code (cloud destroyed, no re-run attempted): yank/replug with
      same-serial verification, fill on the spare mount (21% for a 20%
      target), newest data segment discovered + corrupted, netem add/remove on
      `enp7s0`, and peer-observed RTT 1.44→100.56→0.58ms
      (`latency_observable_on_wire`, `latency_removal_restores_rtt` PASS) —
      the iteration-1 gaps are closed. Remaining recorded caveat (deviation
      #22): the heal loop proves the node keeps serving correct bytes (repair
      or replica failover), not that the corrupted segment held the blob;
      precise repair assertions belong to f4's role scenarios.*
- [x] **Tests:** every injector emits exactly one `FailureInjectionRecord`
      per attempt (success or failure), and a skipped injector records
      `success=false` with a reason — asserted in at least one unit test and
      one fleet run.
      *Reviewer (iteration 2): `finish()` appends exactly one record for
      success/failure (fleet_degrade.rs:491-518) and is unit-tested for both
      (`finish_records_success_and_failure_exactly_once`); skip paths are
      unit-tested (`injector_without_volumes_records_skipped_failure`,
      `injector_without_ssh_records_skipped_failure`); the fleet run asserts
      `exactly_one_record_per_attempt` (7 records / 7 attempts) **and** now
      exercises the skip path in the same run (`skip_path_records_failure`
      PASS); the artifact serializes 8 records = 7 successes + 1 expected
      skip. The iteration-1 LOW gap is closed.*
- [x] **Docs:** every new `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `e2e`; the module doc states the Option-A SSH-black-box rule
      and the no-performance-assertion rule.
      *Reviewer (iteration 3, commands re-run): the two remaining items now
      carry `no_run` examples — `Cluster::fill_role_root`
      (degrade.rs:185-196) and `Cluster::local_role_root` (degrade.rs:225-236);
      every new `pub` item carries an example — `fleet_degrade.rs` 28/28,
      `remote.rs` 9/9, `report.rs` 2/2, and the two `degrade.rs` additions.
      `cargo test -p e2e --doc` = 59 passed / 0 failed (57 + the 2 new
      examples); `RUSTDOCFLAGS="-D warnings" cargo doc -p e2e --no-deps` clean;
      `#![deny(missing_docs)]` at e2e/src/lib.rs:23; fleet_degrade.rs:17-34
      states both the Option-A SSH-black-box rule and the
      no-performance-assertion rule. The iteration-2 gap is closed.*
- [x] **ADR:** ADR-0026 (per-node SSH mapping contract preserved), ADR-0029
      §D3 (injectors realize confirmed-loss/device-unplug semantics for the
      scenarios), ADR-0030/0035 (heal loop uses the real recovery machinery,
      no shortcut), ADR-0036 (drain/detach driven via the real admin API)
      satisfied.
      *Reviewer (iteration 2): ADR-0026 — `TARGET_HOST_SSH` remains the
      index-aligned per-node list; `ssh_targets`/`ssh_target_for`/`ssh_exec`
      map node index to the same target and `kill_and_restart_node_via_ssh` is
      unchanged. ADR-0029 §D3 — yank is real sysfs `device/delete`
      (device-unplug confirmed-loss path; f1 gate PASS 11/11, fallback not
      adopted, f1 doc:368-375). ADR-0030/0035 — the heal loop is HTTP-only
      (`corrupt → POST /admin/scrub → read-back poll`,
      fleet_degrade.rs:1032-1101), no product hook or data shortcut. ADR-0036
      — `post_json` reaches body-carrying admin APIs (`POST /admin/pools`,
      drain mode); no admin endpoint added; actually driving drain/detach
      belongs to f3/f4. `git status` confirms no product crate was modified
      (no test-only product hooks).*
- [x] **Perf:** no perf assertions in this feature; frontmatter rules honored
      — bounded worker/injector orchestration (2.6), `spawn_blocking` for SSH
      and file work (8.3), pre-sized maps for the fixed 4-role volume set
      (1.3); remote commands are one-shot per injection, never per-request.
      *Reviewer (iteration 2): no throughput/latency threshold asserted
      anywhere — the latency checks are functional RTT observations
      (`latency_observable_on_wire`, `latency_removal_restores_rtt` PASS in
      the artifact); 2.6 N/A (no channels introduced — injectors are
      sequential one-shot calls); `ssh` runs on `spawn_blocking` with
      justifying comments (remote.rs:647-695); `fill_dir` now runs on
      `spawn_blocking` from both `fill_disk` and `fill_role_root`
      (degrade.rs:170-172, :197-199), closing the iteration-1 LOW caveat;
      `Vec::with_capacity` used for targets and records
      (fleet_degrade.rs:314, :385). Residual (LOW, pre-existing, non-gating):
      the opt-in local `tc` path still spawns `tc` synchronously inside an
      async fn — one-shot and gated behind `E2E_ALLOW_LOOPBACK_LATENCY`, no
      remote work blocked.*
- [x] **Integration:** one fleet run drives all five injector families
      successfully against the f1 topology and the report contains the
      injection records with correct node/role attribution. **No load suite
      runs on the dev machine** (PIPELINE §6).
      *Reviewer (iteration 2): judged from the artifact + code (cloud fleet
      destroyed; no re-run attempted).
      `artifacts/fleet-injectors-2026-09-11.json`: 8 records = 7 successful
      injector attempts + 1 expected skip-gate exercise; all 7 attempts
      `node_index=1` and machine-asserted
      (`records_attributed_to_injection_node`), role prefixes
      (`spare:`/`data:`/`enp7s0:`) machine-asserted
      (`records_role_attribution`), 18/18 assertions, 22.2s, result `pass`.
      `post_json` is not exercised live (drain/detach is f3/f4's job per the
      ADR-0036 note). Locally only `--lib`, `--doc`, and the `fleet_injectors`
      skip were run (PIPELINE §6).*
- [x] **Deviations:** module location (OQ d), SSH byte-transfer method for
      corruption, interface-resolution rule, and the fallback-injector
      selection (sysfs vs hcloud) are recorded here.
      *Reviewer (iteration 2): verified — `## Deviations (accepted)` records
      all four required decisions (1 OQ d layout; 2 corruption transfer: local
      `stat` offset + remote `dd` with `python3` fallback, no byte streaming;
      3 interface resolution: `LOAD_TEST_LATENCY_IFACE` else `ip -o route
      get`, `lo` always rejected; 4 sysfs-only yank per f1's gate verdict
      "PASS (11/11) — the fallback decision is NOT needed", f1 doc:368-375),
      matching the code (`corrupt_command` :1525-1538, `latency_iface`
      :1132-1172 + `validate_iface` :1383-1398, `yank_command` :1405-1418;
      record/env mapping :334-420). The implementation-shape and
      live-run deviations (5–25) are recorded too, including the re-scoped
      heal claim (#22), the corrected fill arithmetic (#19), the opt-in local
      latency (#16), and the artifact-commit note (#25). The iteration-1 gap
      is closed.*

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions (resolved)

- **OQ d (module layout).** Recommendation: `RemoteCluster`/`RemoteNode`
  plumbing in `e2e/src/remote.rs`; scenario-grade injectors in
  `e2e/src/load/fleet_degrade.rs`. Alternative: a single new
  `e2e/src/fleet.rs` carrying both. Decide at implementation; record the
  decision.
  **RESOLVED 2026-09-11:** implemented as recommended — plumbing in
  `e2e/src/remote.rs`; scenario-grade injectors in
  `e2e/src/load/fleet_degrade.rs` (re-exported from `load/mod.rs`); recorded
  in [Deviations (accepted)](#deviations-accepted) #1.
- **Interface resolution.** How to derive the internal interface on a node
  (`ip route get <harness-ip>` vs `ip -o addr show to 10.0.0.0/24`)? Must be
  deterministic and must reject `lo`. Decide and document; the harness can
  pass `LOAD_TEST_LATENCY_IFACE` as an override.
  **RESOLVED 2026-09-11:** `LOAD_TEST_LATENCY_IFACE` override wins; else
  `ip -o route get <next-node internal ip>` parsed for `dev`; `lo` rejected
  for both; single-node fleet errors out. No default-route heuristic; no
  `ip -o addr show` alternative. Recorded in
  [Deviations (accepted)](#deviations-accepted) #3.
- **Corruption transfer mechanism.** Stream bytes over SSH vs a remote
  `dd`/`python3` one-liner: choose based on what is guaranteed present on
  Ubuntu 24.04 (`dd`/`python3` are; keep the fallback order explicit) and
  record it.
  **RESOLVED 2026-09-11:** offset computed locally from `stat -c %s`; the
  write is a remote `dd if=/dev/urandom … seek=… conv=notrunc`, with an
  explicit `python3` fallback branch; no SSH byte streaming. Recorded in
  [Deviations (accepted)](#deviations-accepted) #2.
- **Volume map source on the harness.** Does the runner copy the
  provisioning record to the Harness VM, or does the injector accept a
  `LOAD_TEST_VOLUMES_JSON` env payload? (Recommendation: runner copies the
  record; injector also accepts the env override for gate/debug runs.)
  **RESOLVED 2026-09-11:** the runner copies the f1 provisioning record and
  sets `LOAD_TEST_RECORD_FILE`; `LOAD_TEST_VOLUMES_JSON` inline is accepted
  as an override; per-node SSH targets come from `TARGET_HOST_SSH` first,
  record `internal_ip` second; missing configuration records `skipped:`
  failures, never a silent success. Recorded in
  [Deviations (accepted)](#deviations-accepted) #5.
- **Segment-id selection policy.** For generic corruption scenarios: pick
  the lexicographically-first `.dat` on the target role? Prefer a segment
  the harness just wrote (if discoverable)? State the rule per scenario in
  f3/f4; the injector offers both an explicit id and a discovery helper.
  **RESOLVED 2026-09-11:** the injector takes an explicit id parameter plus
  two discovery helpers — `list_segment_ids` (name-sorted) and
  `newest_segment_id` (mtime-sorted); f3/f4 own their per-scenario rule; the
  f2 suite uses `newest_segment_id`. Recorded in
  [Deviations (accepted)](#deviations-accepted) #6.
- **Hints-role corruption validity.** Corrupting a `.dat` on the hints role
  may not be meaningful (hints are not segment data). **Mark any hints-file
  injector as grounding-required** until f4's OQ c is resolved.
  **RESOLVED 2026-09-11:** documented as not meaningful in the module doc
  (hints are a WAL, not segments; `list_segment_ids` returns empty there);
  f4 P4 asserts the f0 gate contract for hints. Recorded in
  [Deviations (accepted)](#deviations-accepted) #7.

## Deviations (accepted)

Implementation close 2026-09-11. None of these change product behavior; all
are recorded against the f2 scope as reviewed.

Review closed **PASS** on iteration 3 (2026-09-11); final fleet evidence:
`artifacts/fleet-injectors-2026-09-11.json` — 8 records = 7 successes + 1
expected skip, 18/18 assertions, 22.2s.

### Required decisions (OQ / scope)

1. **OQ d — module layout.** As recommended: `RemoteCluster`/`RemoteNode`
   plumbing in `e2e/src/remote.rs`; scenario-grade injectors in
   `e2e/src/load/fleet_degrade.rs` (re-exported from `load/mod.rs`).
2. **Corruption transfer.** Offset computed locally from `stat -c %s`; the
   write is a remote `dd if=/dev/urandom … seek=… conv=notrunc`, with an
   explicit `python3` fallback branch when `dd` is unavailable
   (`corrupt_command`). No bytes stream over SSH.
3. **Interface resolution.** `LOAD_TEST_LATENCY_IFACE` override wins; else
   `ip -o route get <next-node internal ip>` parsed for `dev`. `lo` is always
   rejected (both override and derived) and a single-node fleet errors out
   rather than guessing. No default-route heuristic.
4. **Fallback injector selection (sysfs vs hcloud).** Sysfs only. The f1 gate
   passed 11/11 and recorded the hcloud detach/attach branch as **not
   adopted** (no Hetzner token/CLI on the Harness). A hcloud branch in the
   Rust injector would be unreachable dead code; the injector instead fails
   loudly and records a failed attempt when `/sys/block/<dev>/device/delete`
   is missing or unwritable. No env override for the method.
5. **Volume map source.** The runner copies the f1 provisioning record to the
   harness and sets `LOAD_TEST_RECORD_FILE`; the injector also accepts
   `LOAD_TEST_VOLUMES_JSON` inline for gate/debug runs. Missing configuration
   is not an error: every injection records `success=false` with a
   `skipped:` reason.
6. **Segment-id selection.** The injector accepts an explicit id and offers
   two discovery helpers: `list_segment_ids` (name-sorted) and
   `newest_segment_id` (mtime-sorted). f3/f4 state their per-scenario rule;
   the f2 suite uses `newest_segment_id`.
7. **Hints-role corruption.** Documented as not meaningful in the module doc
   (hints are a WAL, not segments): `list_segment_ids` returns empty on the
   hints mount; f4 P4 resolves hints semantics per the f0 gate contract.

### Interface additions / shape

8. **`run_ssh_command` realized as two methods.** `RemoteCluster::ssh_exec`
   (index-based, per-node `TARGET_HOST_SSH`) and
   `RemoteCluster::ssh_exec_on` (explicit target, needed when SSH targets
   come from the provisioning record rather than the env list).
9. **`connect_with_ssh_targets`** added so tests/runner can supply the
   per-node SSH mapping explicitly; `connect` reads `TARGET_HOST_SSH`.
   `ssh_targets()`/`ssh_target_for()` expose it. SSH targets are validated
   (no option-shaped/whitespace values) because they are argv elements to
   `ssh`.
10. **`SshOutput` (status/stdout/stderr) + `SshOutput::success`.** A non-zero
    remote exit status is returned in `SshOutput`, not as `Error::Ssh`;
    crash-control keeps its strict behavior through `run_ssh`.
11. **`LoadReport` gains `injection_records` + `record_injections`/
    `injection_records`** (skip-when-empty, additive schema) — the
    `records() → LoadReport.injection_records` wire-up the f2 Data Flow
    requires.
12. **`post_json` body construction.** reqwest is compiled without its `json`
    feature, so the body is `serde_json::to_vec` + explicit
    `Content-Type: application/json`; the wire shape is identical.
13. **`corrupt_and_verify_heal_remote` is a method** on `FleetInjector` (not
    a free function) so it can reach the record buffer.
14. **Extra record type `disk_fill_remove`** for fill cleanup (the doc lists
    the new `injection_type` strings as backward-compatible examples, not an
    exhaustive enum; no consumer parses them exhaustively). Probes
    (`device_gone`, `mount_usable`) and discovery helpers
    (`list_segment_ids`, `newest_segment_id`, `measure_rtt_ms`) are `&self`
    reads and intentionally do not record.
15. **`fill_volume` returns `Result<(), Error>`** and cleanup is
    `remove_fill(node, role)` (the path is a documented constant,
    `<mount>/.fill.bin`); the In-Scope prose "returns the fill path for
    cleanup" is satisfied by the pair.
16. **Local latency is explicitly opt-in.** `Cluster::inject_latency`/
    `remove_latency` (loopback `tc`) now require
    `E2E_ALLOW_LOOPBACK_LATENCY=1` and otherwise warn + return a `skipped:`
    error, keeping loopback contamination out of fleet scenarios. Local runs
    have no `FailureInjectionRecord` sink; the record-bearing skip contract is
    the fleet injector's (exercised in the fleet run and unit tests).

### Live-run findings / hardening

17. **Sysfs yank waits for disappearance (bounded 10s).** Removal is
    asynchronous (the f1 gate observed the by-id link lingering); without the
    wait the `device_gone` probe is flaky.
18. **`tc qdisc replace` instead of `add`** for latency (idempotent; sets the
    requested delay instead of stacking/keeping an older one) and the
    injector verifies the qdisc via `tc qdisc show`.
19. **Fill arithmetic uses df's percent denominator (`used + avail`, which
    excludes reserved blocks).** The first fleet run landed at 16% for a 20%
    target using `size - avail`; the corrected command re-validated live at
    21% for 20%. This matters for f3/f4 near-full scenarios.
20. **`device_gone` probe inversion fixed.** The first fleet run exposed
    `test -e` + the shared `0 = condition true` convention; the probe now
    runs `test ! -e` and has a dedicated unit test.
21. **Hard yank leaves the fs stale/unmounted; `replug_volume` does not
    remount.** Scenario-owned recovery per the epic failure-semantics table
    (f4 P1b formats fresh; P2a/P3 mount explicitly). The f2 suite probes
    `mount_usable` and fails with a clear message if a prior yank was not
    recovered.
22. **Heal-loop claim re-scoped.** `corrupt_and_verify_heal_remote` corrupts
    a real segment, triggers the real scrub/repair machinery, and proves the
    node keeps serving correct bytes (local repair or replica failover). It
    does **not** prove the corrupted bytes were local to the served copy —
    the product exposes no key→segment map — so precise repair assertions
    (local-copy restoration, repair counters, manifest state) are f4's role
    scenarios. The method docstring states this explicitly.
23. **Latency is observed on the wire in the f2 suite:** `measure_rtt_ms`
    (ICMP `ping`; functional probe, no perf assertion) before → inject →
    after asserts RTT increases by ≥50ms (injected +100ms) and returns below
    the delayed value after removal.
24. **Local role-root support (additive, no product change):**
    `Cluster::local_role_root`/`fill_role_root`, and `find_segment_files` now
    searches the ADR-0031 sibling `pool-data` root — without it, local
    `corrupt_shard` could not find any `.dat` after pools became mandatory.
    `fill_disk`/`fill_role_root` run their blocking `dd`/`df` work on
    `spawn_blocking` (perf 8.3), matching the fleet path.
25. **Fleet evidence.** 3-node Phase 4 fleet with `--volume-pools`,
    `sut-deploy.sh --cluster --pools-on-mounts`; `fleet_injectors` ran on the
    Harness (PIPELINE §6 — never locally) and passed in 22.2s: 7/7 injector
    attempts `success=true` (fill 21% for a 20% target; yank/replug with the
    same serial; newest-segment corruption; RTT 1.4ms → 100.6ms with +100ms
    injected → 0.6ms after removal), **18/18 assertions**, and the report
    serializes 8 records (7 successes + 1 expected `skipped` gate exercise).
    Post-run health was 200 with zero non-healthy pools on all nodes. The
    fleet was destroyed afterwards (0 servers / 0 volumes). Artifact:
    [fleet-injectors-2026-09-11.json](artifacts/fleet-injectors-2026-09-11.json).
    Committing the artifact with the feature is the user's call per the
    project workflow (the file is staged in the working tree).
