---
feature: "Volume-Backed Fleet Topology (Provision → Mount → Deploy → Destroy)"
epic: "fleet-degradation"
status: done
priority: critical
owner: ""
dependencies:
  - epic: fleet-degradation
    reason: First feature of the linear DAG; all degradation scenarios need real devices and stable mount paths
  - epic: disk-resilience
    reason: Pool config/attach/detach runtime (ADR-0029/ADR-0031) is the deployment surface the volume roots feed
adr:
  - 0026-phase3-dedicated-node-vms
  - 0029-storage-pools-disk-resilience
  - 0031-remove-single-datadir-legacy-mode
  - 0036-phase-c-scale-ops-drain
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-09-11
updated: 2026-09-11
---

# Volume-Backed Fleet Topology (Provision → Mount → Deploy → Destroy)

## Summary

Give the Phase 4 fleet **real block devices**: `scripts/vm-provision.sh`
gains an opt-in `--volume-pools` mode that creates 4 Hetzner Cloud Volumes
per node VM — `data`, `wal`, `meta`, `hints` — attaches them, records every
volume id in the provisioning record (`.hetzner/provision-<prefix>.json`),
and `scripts/sut-deploy.sh` gains a matching mode that points the four
`[[storage.pools]]` roots at stable mount paths on those volumes instead of
sibling directories on the node's local disk. `vm-down`/`--destroy` learns to
delete the recorded volumes (volumes are billed while they exist, **including
detached**). The first deliverable after wiring is a **sysfs yank/replug
validation gate on a real Hetzner volume** — the hard-failure mechanism F2/F3/
F4 depend on — with an explicit go/fallback decision recorded; the fallback
is the Hetzner API `volume detach/attach` path, which requires deciding how
the harness gets token access (today it has none).

Volume-backed runs are **correctness/degradation tests, not performance
tests**: every result crosses network block storage and is explicitly **not
comparable** to local-disk Phase 2/3 runs. No performance assertion appears
in this feature or anything that consumes it.

## Scope

### In Scope

- **`vm-provision.sh --volume-pools` (opt-in; Phases 2/3 unchanged):**
  - Create one Hetzner volume per node per role: `data`, `wal`, `meta`,
    `hints`, named `{prefix}-vol-{role}` (e.g.
    `oceanfs-loadtest-4-sut-0-vol-data`), sized by parameters with generous
    defaults (proposal, OQ a): **data 120 GB, wal 20 GB, meta 20 GB,
    hints 10 GB = 170 GB/node**. Sizes are CLI/env-overridable
    (`LOAD_TEST_VOLUME_DATA_GB`, etc.).
  - Attach each volume to its node's VM, wait for `available`/`in-use`
    transitions, and discover the resulting device node (`/dev/sdX` or
    `/dev/vdX`) **by volume id/serial**, never by enumeration order.
  - **Provisioning record schema extension (normative):** add a
    `volumes[]` entry to each `sut_nodes[]` element:
    ```json
    "volumes": [
      { "role": "data",  "name": "...-vol-data",  "id": 12345,
        "device": "/dev/sdb", "mount": "/mnt/oceanfs-data",  "size_gb": 120 },
      { "role": "wal",   "name": "...-vol-wal",   "id": 12346,
        "device": "/dev/sdc", "mount": "/mnt/oceanfs-wal",   "size_gb": 20 },
      { "role": "meta",  "name": "...-vol-meta",  "id": 12347,
        "device": "/dev/sdd", "mount": "/mnt/oceanfs-meta",  "size_gb": 20 },
      { "role": "hints", "name": "...-vol-hints", "id": 12348,
        "device": "/dev/sde", "mount": "/mnt/oceanfs-hints", "size_gb": 10 }
    ]
    ```
    The `id` is **mandatory** — it is the only safe handle for teardown.
  - **Quota/budget guard:** preflight the total requested volume GB against
    a configurable cap (`LOAD_TEST_VOLUME_QUOTA_GB`, default 1024 per the
    user's 1 TB quota) and refuse to provision over it. Cost estimate output
    includes volume GB (volumes bill per GB-hour independent of power state).
  - `vm-status` reports the recorded volumes and warns if any recorded volume
    is missing from `hcloud volume list` (drift detection).
- **`sut-deploy.sh --pools-on-mounts` (or `--volume-pools`):**
  - New pool-root mode: `root = /mnt/oceanfs-{data,wal,meta,hints}` for the
    role-complete 4-pool block, replacing the current
    `${DATA_DIR}-pools/pool-*` directory roots.
  - A **pre-flight assertion** on each node: every required mountpoint is a
    mountpoint of a block device of the expected size class, is writable, and
    is **not** the root filesystem; fail the deploy with a clear message if
    not. This prevents a silent fallback onto the node's local disk (the
    single most likely volume-provisioning failure mode).
  - Filesystem format/mount step (idempotent; never formats an already-marked
    volume). **Filesystem proposal (OQ b):** ext4 for all four volumes —
    Ubuntu default, tooling present, simple; xfs remains an option for the
    data volume if large-file behavior warrants it. Record the decision in
    the Deviations section.
  - Mount options recorded: `noatime` for all roles (a test-harness
    optimization, not a product claim).
- **`vm-down` / `vm-provision.sh --destroy` volume teardown (HARD
  constraint):**
  - Read the provisioning record, detach (if in use) then **delete every
    recorded volume id**. Deletion is idempotent and reports per-volume
    outcome.
  - If the record is missing/corrupt, fall back to prefix-scan
    (`hcloud volume list` name prefix) and delete, logging loudly; never
    leave a volume silently.
  - The record's `volumes[].id` is what makes teardown safe — this is why
    the schema extension is normative.
- **Sysfs yank/replug validation gate (early, blocking the rest of the epic):**
  - On a real provisioned volume with a mounted ext4 fs, validate:
    1. The volume exposes a usable device path and serial;
    2. `echo 1 > /sys/block/<dev>/device/delete` removes the device;
    3. A SCSI host rescan (`echo "- - -" > /sys/class/scsi_host/host*/scan`)
       — or the documented equivalent — re-adds the device and it matches
       the same volume serial;
    4. The mount can be re-established (or is correctly reported as needing
       `mount` after a clean umount, depending on the harness procedure);
    5. `hcloud volume describe <id>` still reports the volume attached.
  - **Decision point recorded in Deviations:** if sysfs yank/replug is not
    reliable on Hetzner volumes, the fallback is
    `hcloud volume detach` + `hcloud volume attach` driving the device away
    and back. That fallback requires the **Hetzner token on the Harness VM**
    (or the harness shelling to the laptop — rejected): record which option
    is chosen and update the harness provisioning to carry the token
    (scoped, revocable) if needed.
- Tests:
  - Script-level dry-run tests (`vm-provision.sh --dry-run`) assert the
    record schema and the quota guard;
  - A live topology smoke: provision 1 node `--volume-pools` → deploy →
    `GET /admin/pools` shows all four roots as distinct mount paths →
    `--destroy` deletes servers **and** volumes (verified against
    `hcloud volume list`);
  - The sysfs gate script produces a pass/fail artifact attached to the
    feature's DoD.

### Out of Scope (for this feature)

- Fault injectors over SSH (device yank as a *test operation*, disk fill,
  corruption, `tc`) — **f2**. f1 validates the mechanism; f2 turns it into
  harness injectors.
- Any scenario logic (`load_degraded`, pool-degradation scenarios) — **f3/f4**.
- The spare 5th data volume (runtime attach/detach realism) — **OQ a**;
  if approved, it is a small extension of this feature's sizing/record
  schema, not a new feature.
- Soft degradation (`dm-flakey`/`dm-delay`) — follow-up feature, explicit
  non-goal.
- Migrating Phases 2/3 to volumes — they stay on local disk.
- Volume resizing / online fs grow — not needed by any scenario; record as a
  non-goal.

## Crate Impact

| Crate | Change |
|---|---|
| `scripts/vm-provision.sh` | `--volume-pools`, volume create/attach/record, quota preflight, `--destroy` volume deletion, `--status` volume reporting |
| `scripts/sut-deploy.sh` | `--pools-on-mounts` pool-root mode + mountpoint pre-flight assertion |
| `scripts/lib/` | Shared volume helpers if needed (device discovery by serial, mount/umount idempotence) |
| `.opencode/skills/vm-up|vm-down|vm-status|vm-deploy` | Document the new flag, record schema, teardown guarantee |
| `e2e` | No code in this feature. (The record schema is consumed by f2.) |

## Interface (Public API)

Script-level surface (no Rust `pub` items in this feature):

- `vm-provision.sh --phase 4 --volume-pools [--volume-data-gb N]
  [--volume-wal-gb N] [--volume-meta-gb N] [--volume-hints-gb N]`
  — provisions the fleet with per-role volumes; `--dry-run` prints the
  create/attach plan and the record JSON without calling `hcloud`.
- `vm-provision.sh --status <prefix>` — per-node volume section:
  `{role, name, id, device, mount, size_gb, attached}` plus a `drift` flag
  when a recorded volume is absent from `hcloud volume list`.
- `vm-provision.sh --destroy <prefix>` — deletes servers **and** all
  volumes recorded (or prefix-matched on record loss).
- **Provisioning record schema** (normative, consumed by f2/f3/f4):
  `sut_nodes[i].volumes[] = { role, name, id, device, mount, size_gb }`.
- `sut-deploy.sh --cluster <targets> --pools-on-mounts` — writes
  `[[storage.pools]]` roots `/mnt/oceanfs-{data,wal,meta,hints}`; fails the
  deploy if any mount is absent/not-a-mount/not-writable.
- `scripts/lib/volume-helpers.sh` (proposed) —
  `volume_device_by_serial(serial)`, `ensure_mount(device, mount, fs)`,
  `is_mountpoint(path)` used by provision/deploy.

## Data Flow

```
./scripts/vm-provision.sh --phase 4 --volume-pools --nodes 3
   ├─ quota preflight: 3 nodes × 170 GB = 510 GB ≤ LOAD_TEST_VOLUME_QUOTA_GB
   ├─ per node VM:
   │    create 4 volumes (role-sized) → attach → wait in-use
   │    discover /dev/<dev> by volume id/serial
   │    mkfs.ext4 (fresh volumes only) → mount /mnt/oceanfs-<role> → noatime
   │    (idempotent: skip format if fs signature present; remount if needed)
   ├─ write .hetzner/provision-<prefix>.json with sut_nodes[].volumes[]
   └─ sysfs gate (first run): delete + rescan + serial match + remount
        ├─ PASS → f2/f3/f4 may rely on sysfs yank as the hard-failure mechanism
        └─ FAIL → record fallback decision (hcloud detach/attach + token path)

./scripts/sut-deploy.sh --cluster "root@10.0.0.2,..." --pools-on-mounts
   ├─ per node: assert /mnt/oceanfs-{data,wal,meta,hints} are block-device mounts
   ├─ write config with pool roots = those mounts
   └─ restart systemd unit → GET /admin/pools shows 4 healthy pools (distinct roots)

./scripts/vm-provision.sh --destroy oceanfs-loadtest-4
   ├─ hcloud volume delete <id> for every recorded volume (detach first if in use)
   ├─ fallback prefix scan if record missing
   └─ delete servers (existing behavior)
```

## Definition of Done

- [x] **Code:** `vm-provision.sh --volume-pools --dry-run` prints the create/
      attach/quota plan and a record matching the normative schema; `sut-deploy.sh
      --pools-on-mounts` writes the mount-rooted pool block; `--destroy` deletes
      recorded volumes; `--status` reports volumes; shellcheck clean on the
      touched scripts.
      *Reviewer (iteration 2): iteration-1 gaps 1–11 all independently verified
      fixed — dry-run suite 18/18; shellcheck clean on 5 scripts; mocked-cloud
      full lifecycle (phase 2 two-VM + phase 4 3-node fleet, 5 volumes/node)
      records and destroys to {servers:0, volumes:0}; volume-delete failure
      exits 1 with MANUAL CLEANUP; record-write failure deletes all volumes+VMs;
      budget gate includes volume GB (5×230 GB at €0.90 refused: €1.116 vs
      €0.84 VM-only). Two residual destroy-failure detection defects found (see
      below).*
      *Reviewer (iteration 3): both iteration-2 defects independently verified
      fixed by running the real `--destroy` under an independent adversarial
      mock (`hcloud volume list` failing / present / absent × delete+describe
      failing / detach failing). Failed delete with the volume still in a
      successful inventory → exit 1, `still present`, `MANUAL CLEANUP
      REQUIRED`; failed delete with the inventory down → exit 1, `absence could
      not be confirmed`; no record + list down → exit 1, `Cannot enumerate
      volumes` before any completion claim; genuinely absent volume (list
      succeeds without the reference) → exit 0 `already absent`; detach failure
      is inert (delete success → drained, delete failure → exit 1). The
      provisioning-failure `cleanup()` path re-verified end-to-end with a
      mocked cloud (create volume → later setup step fails): delete failure
      exits 1 + `MANUAL CLEANUP REQUIRED`; delete+list down exits 1 + `absence
      could not be confirmed`; delete success drains silently. Regression suite
      10/10, dry-run suite 18/18, shellcheck clean on all 6 touched scripts.
      Residual LOW (non-blocking): if `hcloud volume list` exits 0 while
      emitting no parseable array **and** there is no record, the prefix scan
      yields an empty set and destroy can still claim completion (see
      Deviations) — not reachable with a truthful CLI, whose failures surface
      as non-zero exits.*
- [x] **Tests:** live smoke on a 1-node `--volume-pools` fleet — provision →
      deploy → `GET /admin/pools` shows four healthy pools whose roots are the
      four mounts → `--destroy` → `hcloud volume list` contains none of the
      recorded ids; the quota guard refuses an over-quota request; the
      mountpoint pre-flight assertion fails the deploy when a mount is absent
      (positive and negative cases).
      *Reviewer: cloud is destroyed; verified via
      `artifacts/live-smoke-2026-09-11.md` plus independent non-cloud
      verification of the quota boundary (≤cap passes, >cap refuses) and the
      deploy pre-flight (negative aborts before scp; positive mocked path
      succeeds).*
- [x] **Tests:** sysfs yank/replug gate executed on a real Hetzner volume;
      result artifact (device delete → rescan → serial match → remount)
      recorded pass/fail, with the fallback decision recorded in Deviations.
      *Reviewer (iteration 2): artifact has 11 steps, all PASS, `result: pass`;
      the committed file's `pass_count: 10` predates the count fix and carries
      an explicit `note`; `scripts/volume-sysfs-gate.sh:195-200` now derives
      pass/fail counts only after appending `api_attached` (re-verified against
      the mock shim: exactly one step, `pass_count` 1). Deviations record the
      PASS verdict and the unused fallback.*
- [x] **Docs:** every new flag documented in the script headers and the
      `vm-up`/`vm-down`/`vm-status`/`vm-deploy` skills; the provisioning-record
      schema extension and the volume-billing destroy guarantee are stated in
      each; the "volume-backed ≠ performance measurement" rule is stated in
      this doc and in the epic.
      *Reviewer (iteration 2): iteration-1 gap 1 closed —
      `.opencode/skills/vm-deploy/SKILL.md:93-100` now states the `volumes[]`
      field list ({role,name,id,device,device_id,mount,size_gb}) and the
      per-GB-hour billing / delete-first destroy guarantee; `vm-up` (lines
      104–116), `vm-down` (lines 50–56, 106–112) and `vm-status` (lines 47–60)
      state the record/billing guarantee as well; this doc (Summary, Scope) and
      the epic state the "volume-backed ≠ performance measurement" rule.*
- [x] **ADR:** ADR-0026 (fleet topology; record stays array-based), ADR-0029
      §D8 (mountpoints not device paths — pool roots are mount paths),
      ADR-0031 (role-complete pools mandatory; one root per role),
      ADR-0036 (volume roots are what attach/drain/detach mutate), ADR-0019
      (TTL/cap guardrails retained; volume billing is the new guard) satisfied;
      no unaddressed constraint.
      *Reviewer: `sut_nodes[]` retained and extended per-node; phase 2 keeps the
      legacy `sut` object; deploy writes four `[[storage.pools]]` with distinct
      mount roots; spare volume recorded but not a deployed pool; TTL/size
      guardrails untouched.*
- [x] **Perf:** no Rust hot paths touched; no performance assertion is made
      anywhere in this feature; mount/attach/manifest work happens only on
      topology events (never per-request).
      *Reviewer: no Rust files reference volume-pools/mounts; grep of the f1
      scripts/skills/docs finds no throughput/latency assertion (only
      background rationale such as the pre-existing "~150 ops/s" comment).*
- [x] **Integration:** the full lifecycle (provision volumes → deploy pools →
      run one smoke write/read → destroy including volumes) is exercised
      end-to-end on the cloud, and PIPELINE §6 is honored (no load suite on
      the dev machine; this feature runs scripts only).
      *Reviewer: evidenced by `artifacts/live-smoke-2026-09-11.md` (provision,
      pools healthy, 200 write/read, destroy, zero resources). No load suite
      was run locally for this review; only the dry-run script + shellcheck.*
- [x] **Deviations:** filesystem choice (OQ b), sysfs-gate verdict +
      fallback decision, and the spare-volume disposition (OQ a) are recorded
      here or explicitly left PENDING with the reason.
      *Reviewer: all three recorded (ext4; gate PASS/no fallback; spare
      approved + not deployed), plus device-identity, fstab, cleanup-registry,
      phase-2 opt-in, `--status` fixes, size floors, deploy-path and smoke-scope
      deviations.*

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **OQ a (user quota).** Add a 5th data volume per node (+60 GB spare) so
  `POST /admin/pools` attach and detach exercise a **real** spare device?
  **RESOLVED (user approved)** — implemented as role `spare`; see
  Deviations. Quota note: 5×230 GB = 1150 GB exceeds the default 1024 GB
  cap, so 5-node fleets need explicit sizing/cap changes.
- **OQ b (filesystem).** ext4 (proposal) vs xfs. **RESOLVED** — ext4 for
  all roles (+spare), verified live on Ubuntu 24.04; see Deviations.
- **Device discovery.** Verify whether Hetzner volume serials are readable via
  `/dev/disk/by-id/` and/or the `hcloud volume describe` device path; the
  record must store a stable device identity, not `/dev/sdb` guesswork.
  **RESOLVED** — by-id serial confirmed and letters shown to shuffle; see
  Deviations.
- **Mount persistence.** Are mounts re-established on VM reboot (fstab entry
  vs explicit step in setup-harness)? Crash-control tests restart the
  *service*, not the VM, but a VM-level reboot must not silently drop pools.
  **RESOLVED** — UUID fstab entry with `nofail`; see Deviations.
- **Record migration.** Phase 2 records (`sut` object) and Phase 3/4 records
  (`sut_nodes[]`) both exist; the volume extension must be additive and
  consumers (skills/scripts) must tolerate its absence. **RESOLVED** —
  implemented additively (`sut.volumes[]` / `sut_nodes[].volumes[]`); the
  dry-run tests assert both shapes and the empty case.

## Deviations (accepted)

Every item below is an **accepted** close-out verdict, recorded after the
independent review iteration 3 returned **PASS** (2026-09-11). They close the
[Open Questions for the Implementer](#open-questions-for-the-implementer)
above.

- **OQ b — filesystem: ext4 for all four roles + the spare.** Ubuntu 24.04
  ships `mkfs.ext4`/`mount`; verified live on all five volumes. xfs not
  adopted (no large-file behavior need identified).
- **OQ a — spare volume approved and implemented (user decision).** Each node
  gets a 5th volume, role `spare`, default 60 GB, mounted at
  `/mnt/oceanfs-spare` and recorded, but **not** part of the deployed
  `[[storage.pools]]` block: it is the real device for runtime
  attach/detach scenarios. Defaults are now 230 GB/node; note a 5×230 =
  1150 GB fleet exceeds the default 1024 GB quota cap, so the quota guard
  refuses it — the operator must lower per-role sizes or raise the cap
  explicitly.
- **Device identity: stable by-id path, letters never trusted.** Volumes are
  virtio-scsi devices whose udev identity is
  `/dev/disk/by-id/scsi-0HC_Volume_<id>` (matches `hcloud volume
  linux_device`). The sysfs gate observed the device letter **change**
  (`/dev/sdb` → `/dev/sdc`) across delete+rescan, so the record stores
  `device` (display only) **and** `device_id` (stable handle), and f2's
  injectors must resolve devices by id. `volume_device_by_serial` waits for
  the by-id path (attach/rescan are asynchronous).
- **Mount persistence: fstab UUID entry (`noatime,nofail,
  x-systemd.device-timeout=10`).** `nofail` keeps a missing/detached volume
  from stalling boot; the deploy pre-flight then fails loudly instead of
  silently falling back to local disk. `mount /mnt/oceanfs-<role>` after an
  umount verified the fstab path live; a VM reboot was not separately
  exercised, but the entry is the boot mechanism.
- **Sysfs gate verdict: PASS (11/11) — sysfs yank is the hard-failure
  mechanism; the fallback decision is NOT needed.** Artifact:
  `docs/features/fleet-degradation/artifacts/sysfs-gate-2026-09-11.json`
  (delete attribute present, device gone, rescan re-adds same serial,
  remount + marker read, `hcloud volume describe` still attached). No
  Hetzner token is placed on the Harness; `hcloud volume detach/attach`
  remains the documented fallback only if a future platform change breaks
  sysfs yank.
- **Failure cleanup is file-based, not array-based.** The volume
  create/format helpers run in command substitutions (subshells), where
  shell array mutations are lost; the first live run leaked a volume when a
  later step failed. A per-run temp registry (`VOLUME_TRACK_FILE`) now
  records every created volume (even when id resolution fails, so cleanup
  can delete by name) and `cleanup()` deletes them before deleting VMs;
  every provisioning call site has an explicit `|| die` so the parent
  process performs the cleanup.
- **`--volume-pools` is allowed on Phase 2** (single SUT VM, with or
  without `--single-vm`) as the 1-node topology for the live smoke; Phases
  2/3 without the flag are unchanged (local disk).
- **`--status` fixes required by this feature's DoD (drive-by, same
  function):** the fleet/phase-2 record builder applied `sort_by` to each
  server object (jq error with any server present) and rendered
  `.server_type` as the whole server-type object. Both are fixed in the
  branch that `--status` needs to report volumes; no behavior change
  otherwise.
- **Pre-flight size floors are sanity classes, not exact sizing:** data
  50 GB, wal 8 GB, meta 8 GB, hints 4 GB (generous fractions of the
  defaults). The assertion's job is to catch a wrong/absent/tiny device on
  the right path, which the live negative case confirmed.
- **Volume deploys use `sut-deploy.sh --pools-on-mounts` directly** (from
  the laptop with a binary copied off the Harness). `setup-harness.sh`'s
  remote `sut-deploy.sh` call is pinned to the Harness clone and does not
  yet pass `--pools-on-mounts`; documented in the `vm-deploy` skill. No
  change to `setup-harness.sh` in this feature.
- **Live smoke scope:** `--phase 2 --single-vm --volume-pools` (1-node),
  provision → status → deploy (mounts healthy) → write/read 200s →
  negative pre-flight → drift detection → destroy with zero volumes left.
  Evidence:
  `docs/features/fleet-degradation/artifacts/live-smoke-2026-09-11.md`.
  Full-fleet (3-node) volume provisioning is exercised by f4.
- **Review iteration-1 hardening (mock-cloud verified):** the volume
  quota output and the budget gate now include the volume GB cost
  (`VOLUME_HOURLY_COST_PER_GB`, ~€0.00006/GB-h; detached volumes are in
  the count); `--destroy` exits non-zero when any volume survives instead
  of reporting completion; the final record build/write has explicit
  `|| die` so a failure there still runs cleanup; the record path resolves
  from the script location (cwd-independent for `--status`/`--destroy`);
  failure cleanup tracks VMs in a file registry as well — this fixed the
  pre-existing subshell bug where `CREATED_VM_NAMES` was lost and failed
  runs left VMs behind; quota/size values are validated as positive
  integers; the disjointness assertion checks both nesting directions; the
  sysfs-gate pass/fail counts include the API step (the committed artifact
  was generated before that fix and carries a `note`).
- **Review iteration-2 hardening (silent-leak fixes):** `volume_delete`
  no longer treats a failed lookup as proof of absence — only a successful
  `hcloud volume list` that does **not** contain the reference returns
  "already absent"; a failed delete with a present or unavailable inventory
  is a hard failure with `MANUAL CLEANUP REQUIRED`. The destroy
  prefix-scan likewise refuses to claim a clean destroy when there is no
  record **and** `hcloud volume list` fails. Both paths are pinned by
  `scripts/tests/test-vm-provision-destroy-guards.sh` (mock `hcloud`, no
  cloud calls; 10 assertions: delete-fails, unprovable absence,
  enumeration-down, control).
- **Review iteration-3 verification (independent adversarial mock):** the
  iteration-2 fixes are confirmed against the real `--destroy` and the
  provisioning-failure `cleanup()` (no cloud calls): failed delete + volume
  present in a successful inventory → exit 1 `still present`; failed delete +
  inventory down → exit 1 `absence could not be confirmed`; no record + list
  down → exit 1 `Cannot enumerate volumes` before any completion claim;
  genuinely absent → `already absent` exit 0; detach failure is inert. One
  residual LOW was found and accepted as non-blocking: `destroy_vms`
  (`scripts/vm-provision.sh:1266-1273`) treats a `hcloud volume list` that
  **exits 0** with empty or unparseable stdout as an empty inventory (the
  scan's `jq … || true` swallows parse failures), so with no record the run
  can print `Destroy complete` and exit 0 while a prefix-matched volume
  survives. The real CLI cannot report success while omitting the JSON array;
  `volume_delete`'s own absence check already fails closed on the same input
  (`jq` rc 4/5 → "absence could not be confirmed"). Reproduction (mock only):
  make `hcloud volume list` print `this is not json` and exit 0 with no
  provisioning record; `--destroy <prefix>` exits 0 and leaves the volume. If
  ever observed, require `jq -e 'type == "array"'` on the inventory before
  trusting an empty scan (or abort when the scan's `jq` fails and the record
  yields no ids).
- **Cloud state at iteration-3 review:** not re-queried (review instruction:
  do not touch the cloud). The committed live-smoke artifact asserts zero
  servers and zero volumes after destroy.

