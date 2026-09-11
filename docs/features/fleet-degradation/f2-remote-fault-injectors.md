---
feature: "Remote Fault Injectors (SSH Black-Box, Real Volumes)"
epic: "fleet-degradation"
status: proposed
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

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e`; `post_json`
      and `ssh_exec` are implemented on the real client paths; `fleet_degrade`
      compiles with no `unsafe`, no test-only product hooks, and all shell
      command construction is parameter-typed and quoting-safe.
- [ ] **Tests:** `cargo test -p e2e --lib` passes (unit: JSON body, quoting,
      `lo` rejection, skipped-injection records); the fleet-only integration
      items run on the cloud harness (PIPELINE §6 — never locally) and are
      listed in the feature's review: yank/replug, per-role fill, segment
      discovery + corruption + remote heal, latency add/remove on the internal
      interface.
- [ ] **Tests:** every injector emits exactly one `FailureInjectionRecord`
      per attempt (success or failure), and a skipped injector records
      `success=false` with a reason — asserted in at least one unit test and
      one fleet run.
- [ ] **Docs:** every new `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `e2e`; the module doc states the Option-A SSH-black-box rule
      and the no-performance-assertion rule.
- [ ] **ADR:** ADR-0026 (per-node SSH mapping contract preserved), ADR-0029
      §D3 (injectors realize confirmed-loss/device-unplug semantics for the
      scenarios), ADR-0030/0035 (heal loop uses the real recovery machinery,
      no shortcut), ADR-0036 (drain/detach driven via the real admin API)
      satisfied.
- [ ] **Perf:** no perf assertions in this feature; frontmatter rules honored
      — bounded worker/injector orchestration (2.6), `spawn_blocking` for SSH
      and file work (8.3), pre-sized maps for the fixed 4-role volume set
      (1.3); remote commands are one-shot per injection, never per-request.
- [ ] **Integration:** one fleet run drives all five injector families
      successfully against the f1 topology and the report contains the
      injection records with correct node/role attribution. **No load suite
      runs on the dev machine** (PIPELINE §6).
- [ ] **Deviations:** module location (OQ d), SSH byte-transfer method for
      corruption, interface-resolution rule, and the fallback-injector
      selection (sysfs vs hcloud) are recorded here.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **OQ d (module layout).** Recommendation: `RemoteCluster`/`RemoteNode`
  plumbing in `e2e/src/remote.rs`; scenario-grade injectors in
  `e2e/src/load/fleet_degrade.rs`. Alternative: a single new
  `e2e/src/fleet.rs` carrying both. Decide at implementation; record the
  decision.
- **Interface resolution.** How to derive the internal interface on a node
  (`ip route get <harness-ip>` vs `ip -o addr show to 10.0.0.0/24`)? Must be
  deterministic and must reject `lo`. Decide and document; the harness can
  pass `LOAD_TEST_LATENCY_IFACE` as an override.
- **Corruption transfer mechanism.** Stream bytes over SSH vs a remote
  `dd`/`python3` one-liner: choose based on what is guaranteed present on
  Ubuntu 24.04 (`dd`/`python3` are; keep the fallback order explicit) and
  record it.
- **Volume map source on the harness.** Does the runner copy the
  provisioning record to the Harness VM, or does the injector accept a
  `LOAD_TEST_VOLUMES_JSON` env payload? (Recommendation: runner copies the
  record; injector also accepts the env override for gate/debug runs.)
- **Segment-id selection policy.** For generic corruption scenarios: pick
  the lexicographically-first `.dat` on the target role? Prefer a segment
  the harness just wrote (if discoverable)? State the rule per scenario in
  f3/f4; the injector offers both an explicit id and a discovery helper.
- **Hints-role corruption validity.** Corrupting a `.dat` on the hints role
  may not be meaningful (hints are not segment data). **Mark any hints-file
  injector as grounding-required** until f4's OQ c is resolved.

## Deviations (accepted)

_None yet — filled at implementation close._
