---
name: vm-test-phase
description: "Run a load test phase against the OceanFS cloud VMs (two-VM Phase 2 topology or Phase 3-4 fleet). Use when the user asks to run the phase 2 sustained load test on the harness, the phase 3 cluster churn test, the phase 4 degraded-mode test, execute a load test against the SUT, or start a cloud test run. Triggers: \"vm-test-phase\", \"run phase 2\", \"run phase 3\", \"run phase 4\", \"run the load test\", \"start the test on the VMs\", \"run the sustained load test\", \"run the degraded-mode test\"."
---

# vm-test-phase

Run a load test phase in the cloud: the payload runs on the
**Harness VM** (load must originate on the internal network — the SUT
firewall only accepts :9000 from there), targeting the already-running
OceanFS fleet, and the report is fetched back to the laptop.

## Supported phases

| Phase | Test | How it runs | Status |
|---|---|---|---|
| 1 | `load_concurrency` | CI runner, local spawn | in CI — no VMs, no skill invocation |
| 2 | `load_sustained` | Harness VM → `TARGET_HOST=<sut-internal>:9000`, `run-phase2.sh` | **implemented** |
| 3 | `load_cluster_churn` | Harness VM → fleet `TARGET_HOSTS` (ADR-0026), `run-phase3.sh` | **implemented** |
| 4 | `load_degraded` | Harness VM → fleet `TARGET_HOSTS` + per-node SSH + real volumes, `run-phase4.sh` | **implemented** — this skill |

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `phase` | Test phase to run | required |
| `mode` | `quick` or `full` (phase 2: 300/3600s; phases 3-4: 120/300s) | `quick` |
| `seed` | Deterministic seed | `42` |
| `duration-secs` | Override run duration — exported as `LOAD_TEST_DURATION_SECS` | mode default |
| `report-dir` | Report output dir on the harness (tmpfs) and laptop | `/tmp/oceanfs-reports` |
| `record` | Phase 4: f1 provisioning record (volumes/mounts/internal_ip) — copied to the harness for the volume injectors | — |

## Prerequisites

- VMs provisioned (**vm-up**) and deployed (**vm-deploy**)
- SUT healthy: `http://<sut-internal>:9000/admin/health` returns 200
- (Optional, to watch the run live) Grafana up: `docker compose -f mcps/docker-compose.yml up -d prometheus grafana` — the tunnel is ensured automatically by run-phase2.sh, so the run's metrics land in the persistent store

## Procedure

1. **Resolve the topology** from the provisioning record
   (`.hetzner/provision-*.json`):

   ```bash
   SUT_INT=$(jq -r '.sut.internal_ip' "$PROVISION_FILE")
   HARNESS_PUB=$(jq -r '.harness.public_ip' "$PROVISION_FILE")
   ```

   Verify the VMs are running first (**vm-status**) — the TTL may have
   powered them off.

2. **Phase 2 — run via `scripts/run-phase2.sh` in `--harness` mode**:

   ```bash
   # run-phase2.sh reads LOAD_TEST_DURATION_SECS for both modes; export it
   # to override the mode default (it is forwarded to the harness-side
   # invocation automatically).
   export LOAD_TEST_DURATION_SECS="${DURATION_SECS:-}"

   ./scripts/run-phase2.sh \
     --harness "root@${HARNESS_PUB}" \
     --${MODE:-quick} \
     --sut "${SUT_INT}:9000" \
     --ssh "root@${SUT_INT}" \
     --service oceanfs \
     --seed "${SEED:-42}" \
     ${REPORT_DIR:+--report-dir "$REPORT_DIR"}
   ```

   What this does:
   - Ensures the observe.sh tunnel (best-effort) so the run's metrics are
     federated into the **persistent laptop Prometheus** (localhost:9091,
     365-day retention) — the durable copy that survives VM teardown.
   - SSHes to the harness, runs the local flow there with
     `TARGET_HOST=<sut-internal>:9000`, `TARGET_HOST_SSH=root@<sut-internal>`,
     `TARGET_SERVICE=oceanfs`, seed, duration.
   - Crash recovery runs over SSH (`systemctl kill -s KILL` →
     `systemctl restart` — the SUT unit is `Restart=no` on purpose).
   - Pushes the `load_test.prom` textfile into the SUT Prometheus
     (best-effort) so the Grafana "Test Phase" panel reflects the run.
   - Fetches `2_load_sustained_*.json` back to the laptop `report-dir`.

   Do NOT hand-roll the SSH + env-var invocation — `run-phase2.sh`
   handles report fetching, textfile pushing, and exit codes.

3. **Phase 3 — run via `scripts/run-phase3.sh` in `--harness` mode**:

   ```bash
   PROVISION_FILE=".hetzner/provision-<prefix>.json"   # from vm-up
   NODES=$(jq -r '[.sut_nodes[].internal_ip] | join(",")' "$PROVISION_FILE")
   SSH_TARGETS=$(jq -r '[.sut_nodes[].internal_ip] | map("root@\(.)") | join(",")' "$PROVISION_FILE")

   ./scripts/run-phase3.sh \
     --harness "root@${HARNESS_PUB}" \
     --${MODE:-quick} \
     --nodes "$NODES" \
     --ssh "$SSH_TARGETS" \
     --seed "${SEED:-42}" \
     ${REPORT_DIR:+--report-dir "$REPORT_DIR"}
   ```

   Fetches `3_load_cluster_churn_*.json`. Without `--ssh`, churn is
   skipped (recorded in the report) — pass it for a real run.

4. **Phase 4 — run via `scripts/run-phase4.sh` in `--harness` mode**:

   Phase 4 drives the f2 SSH black-box injectors (mid-write SIGKILL,
   internal-interface `tc netem`, data-volume fill, segment corruption +
   `POST /admin/scrub`) against the **volume-backed** fleet
   (ADR-0029/0031), so the fleet must have been provisioned with
   `--volume-pools` and the provisioning record must reach the harness
   (the runner copies it and sets `LOAD_TEST_RECORD_FILE`):

   ```bash
   PROVISION_FILE=".hetzner/provision-<prefix>.json"
   NODES=$(jq -r '[.sut_nodes[].internal_ip] | join(",")' "$PROVISION_FILE")
   SSH_TARGETS=$(jq -r '[.sut_nodes[].internal_ip] | map("root@\(.)") | join(",")' "$PROVISION_FILE")

   # Full degraded-mode run (all four scenarios):
   ./scripts/run-phase4.sh \
     --harness "root@${HARNESS_PUB}" \
     --${MODE:-full} \
     --nodes "$NODES" \
     --ssh "$SSH_TARGETS" \
     --record "$PROVISION_FILE" \
     --seed "${SEED:-42}"

   # Control run (background load + manifest only, zero injections):
   ./scripts/run-phase4.sh \
     --harness "root@${HARNESS_PUB}" --quick --no-injections \
     --nodes "$NODES"
   ```

   Fetches `4_load_degraded_*.json`. The runner fails fast when the
   record is missing/has no volumes, when per-node SSH is missing, or
   when harness mode is used without a fleet. The report is
   correctness-only: it contains an explicit `perf_assertions_none`
   marker and no latency/throughput threshold (volume-backed numbers are
   not comparable to local-disk runs).

   The fleet stays up after the run (volumes are billed while they
   exist); when finished, tear it down with **vm-down** /
   `./scripts/vm-provision.sh --destroy <prefix>` so every recorded
   volume is deleted.

5. **Report the outcome** (see schema below). If the exit code is
   non-zero, fetch the last 20 lines of the harness run stderr
   (`ssh root@${HARNESS_PUB} "journalctl -u oceanfs --since '10 min ago' --no-pager | tail -20"`)
   and include them, plus the local report if one was fetched.

## Returns

```json
{
  "phase": 2,
  "mode": "quick",
  "seed": 42,
  "duration_secs": 300,
  "topology": "two-vm",
  "exit_code": 0,
  "report_path": "/tmp/oceanfs-reports/2_load_sustained_20260816T101500.json",
  "harness_report_path": "/tmp/oceanfs-reports/2_load_sustained_20260816T101500.json",
  "grafana_url": "http://localhost:3000/d/oceanfs-load-test",
  "stderr_tail": []
}
```

`exit_code` 0 means the test binary passed (result `pass` in the report);
non-zero means the assertions failed — run **vm-results** for the details
and **vm-metrics**/**vm-logs** for the evidence. Phases 3-4 return the
same schema with their own `report_path` (`3_load_cluster_churn_*.json` /
`4_load_degraded_*.json`) and `topology: "fleet"`.

## Notes

- `--sut` must be the **internal** IP as seen from the harness
  (10.0.0.x). The public IP does not work — the firewall denies :9000
  from the internet.
- Mode durations are phase-specific: phase 2 `quick`/`full` =
  300s/3600s; phases 3-4 = 120s/300s (background load; Phase 4's
  scenarios take their own bounded time on top). `LOAD_TEST_DURATION_SECS`
  overrides both. Respect the user's choice of mode.
- Phase 4 needs the `--volume-pools` provisioning record; without it the
  runner (and the test) fail fast instead of skipping scenarios silently.
