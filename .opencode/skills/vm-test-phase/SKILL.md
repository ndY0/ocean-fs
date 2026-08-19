---
name: vm-test-phase
description: "Run a load test phase against the OceanFS cloud VMs (two-VM topology). Use when the user asks to run the phase 2 sustained load test on the harness, execute a load test against the SUT, or start a cloud test run. Triggers: \"vm-test-phase\", \"run phase 2\", \"run the load test\", \"start the test on the VMs\", \"run the sustained load test\"."
---

# vm-test-phase

Run a load test phase in the two-VM topology: the payload runs on the
**Harness VM** (load must originate on the internal network — the SUT
firewall only accepts :9000 from there), targeting the already-running
OceanFS SUT, and the report is fetched back to the laptop.

## Supported phases

| Phase | Test | How it runs | Status |
|---|---|---|---|
| 1 | `load_concurrency` | CI runner, local spawn | in CI — no VMs, no skill invocation |
| 2 | `load_sustained` | Harness VM → `TARGET_HOST=<sut-internal>:9000` | **implemented** — this skill |
| 3-4 | `load_cluster_churn` / degraded | fleet `TARGET_HOSTS` (ADR-0026: N node VMs + CX43 harness, `run-phase3.sh`) | runner/deploy tooling done 2026-08-19; the test itself (`e2e/tests/load_cluster_churn.rs`) is the next implementation item — tell the user, do not fake a run |

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `phase` | Test phase to run | required |
| `mode` | `quick` (300s) or `full` (3600s) | `quick` |
| `seed` | Deterministic seed | `42` |
| `duration-secs` | Override run duration — exported as `LOAD_TEST_DURATION_SECS` | 300 / 3600 by mode |
| `report-dir` | Report output dir on the harness (tmpfs) and laptop | `/tmp/oceanfs-reports` |

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

3. **Report the outcome** (see schema below). If the exit code is
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
and **vm-metrics**/**vm-logs** for the evidence.

## Notes

- `--sut` must be the **internal** IP as seen from the harness
  (10.0.0.x). The public IP does not work — the firewall denies :9000
  from the internet.
- Full mode (3600s) is the cloud-validation run; quick mode (300s) is for
  smoke checks. Respect the user's choice of mode.
