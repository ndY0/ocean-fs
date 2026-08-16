---
feature: "Agent Integration Test — End-to-End Agent Workflow Validation"
epic: "agent-skills"
status: done
priority: medium
owner: ""
dependencies:
  - epic: agent-skills/vm-skills
    reason: Need all vm-* skills
  - epic: agent-skills/test-execution-skills
    reason: Need all vm-* skills (test execution + results + metrics + logs)
  - epic: operational-tooling/vm-provisioning
    reason: Need vm-provision.sh for provisioning
  - epic: operational-tooling/prometheus-grafana-setup
    reason: Need Prometheus on VM for metrics queries
  - epic: test-phase-implementations/phase2-sustained-load-test
    reason: Need Phase 2 test to run during validation
adr:
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-08-05
updated: 2026-08-16
---

# Agent Integration Test — End-to-End Agent Workflow Validation

## Summary

Create `scripts/test-agent-workflow.sh` — a manual integration test that
exercises the full agent workflow end-to-end in the **two-VM topology**
(ADR-0019): (1) provision SUT + Harness, (2) build on the Harness and
deploy to the SUT, (3) run the Phase 2 quick-mode sustained-load test from
the Harness, (4) fetch and validate the LoadReport, (5) tear down both VMs.
It validates that all pieces (vm-provisioning, setup-harness deployment,
test execution, results fetching) work together exactly as the vm-* skills
drive them. It is NOT a CI test (it provisions real Hetzner VMs and runs a
multi-minute load test); a developer or agent runs it before declaring the
test infrastructure ready.

**Gap closure (2026-08-16):** the original spec targeted a Phase 1
single-VM flow. The script now tests the actual two-VM pipeline:
`vm-provision.sh` refuses `--phase 1` ("runs in CI"), and the deploy step
is `setup-harness.sh` (build on Harness → `sut-deploy.sh` → observability
→ health), not an ad-hoc rsync+build.

## Scope

### In Scope

- `scripts/test-agent-workflow.sh` — single shell script
- Workflow steps (each with a timeout and captured timing):
  1. **Provision:** `./scripts/vm-provision.sh --phase 2 --branch {branch}
     --name {prefix} --ttl {ttl}`
     - Assert: script exits 0; `.hetzner/provision-{prefix}.json` exists
       with `sut.public_ip`, `sut.internal_ip`, `harness.public_ip`
  2. **Deploy:** `./scripts/setup-harness.sh --provision-file {record}`
     - Assert: exit 0 (repo sync + release build on the Harness, SUT
       deploy + systemd unit + observability, SUT health over the
       internal network)
  3. **Run Phase 2 quick:** `./scripts/run-phase2.sh --harness
     root@{harness} --quick --sut {sut-internal}:9000 --ssh root@{sut-internal}
     --service oceanfs --seed {seed} --report-dir {report-dir}`
     - Assert: exit 0
  4. **Fetch + validate results:** newest `2_load_sustained_*.json` in
     the report dir; assert `result == "pass"` and
     `manifest.mismatches == 0`
  5. **Teardown:** `./scripts/vm-provision.sh --destroy {prefix}`
     - Assert: exit 0; provisioning record removed
  6. **Summary:** JSON with per-step timing and overall pass/fail
- Configurable via env vars / flags:
  - `--phase` (default 2 — only 2 is supported today)
  - `--branch` (default `main`), `--seed` (default `42`),
    `--duration-secs` (default `300` = quick), `--name`, `--ttl` (default 2h)
  - `WORKFLOW_KEEP_VMS=true` keeps VMs on failure for inspection
  - `WORKFLOW_REPORT_DIR` (default `/tmp/oceanfs-reports-wf`)
- Failure handling: on any step failure, best-effort teardown (unless
  `WORKFLOW_KEEP_VMS=true`), print the failing step, exit non-zero
- `--dry-run` prints the steps without provisioning anything

### Out of Scope

- CI integration (manual test; needs cloud credentials)
- Multi-phase execution (Phase 2 only — phases 3-4 have no remote support yet)
- Performance benchmarking or regression comparison
- Automated retry on transient failures

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Shell script only. |

## Interface (Public API)

```
Usage: ./scripts/test-agent-workflow.sh [--phase 2] [--branch BRANCH] [--seed N]
       [--duration-secs N] [--name PREFIX] [--ttl HOURS] [--dry-run] [-h]

ENVIRONMENT VARIABLES:
  HCLOUD_TOKEN         Hetzner API token (required)
  WORKFLOW_KEEP_VMS    "true" keeps VMs on failure for inspection
  WORKFLOW_REPORT_DIR  Local report dir (default: /tmp/oceanfs-reports-wf)

Output (stdout): JSON summary with per-step timing and overall pass/fail
Exit code: 0 on success, 1 on failure
```

## Data Flow

```
./scripts/test-agent-workflow.sh

  STEP 1: Provision (SUT + Harness, cx23)
    → ./scripts/vm-provision.sh --phase 2 --branch main --name oceanfs-wf-... --ttl 2
    → record: sut={public 1.2.3.4, internal 10.0.0.5}, harness={public 1.2.3.5}
    [OK] Provision: 420s

  STEP 2: Deploy
    → ./scripts/setup-harness.sh --provision-file .hetzner/provision-oceanfs-wf-*.json
    → harness: git sync + cargo build --release -p oceanfs -p e2e
    → harness → SUT: sut-deploy.sh (binary + config + systemd Restart=no)
    → SUT: setup-observability.sh (Prometheus :9090)
    → SUT health verified over the internal network
    [OK] Deploy: 600s (commit abc1234)

  STEP 3: Run Phase 2 (quick)
    → ./scripts/run-phase2.sh --harness root@1.2.3.5 --quick --sut 10.0.0.5:9000 --ssh root@10.0.0.5 --seed 42
    → 300s sustained load from the Harness; report fetched back
    [OK] Run: 420s

  STEP 4: Validate report
    → jq: result=pass, objects_written=1234, mismatches=0
    [OK] Assert: 1s

  STEP 5: Teardown
    → ./scripts/vm-provision.sh --destroy oceanfs-wf-...
    [OK] Teardown: 20s

  OVERALL: PASS (total: 1461s)
```

## Definition of Done

- [x] **Script:** `scripts/test-agent-workflow.sh` is executable
- [x] **Script:** All 5 workflow steps complete in sequence
- [x] **Script:** Step 1 provisions real cloud VMs (two-VM topology, phase 2)
- [x] **Script:** Step 2 deploys via `setup-harness.sh` (build on Harness, deploy to SUT)
- [x] **Script:** Step 3 runs the Phase 2 quick-mode load test from the Harness VM
- [x] **Script:** Step 4 parses the LoadReport JSON and validates `result == "pass"` and `mismatches == 0`
- [x] **Script:** On any step failure, prints error, attempts teardown, exits non-zero
- [x] **Script:** On overall pass, prints JSON summary with per-step timing
- [x] **Script:** `--dry-run` prints all steps without provisioning
- [x] **Docs:** Script header documents all steps, environment variables, and prerequisites (HCLOUD_TOKEN)
- [x] **Integration:** An agent can run this script on the laptop and verify the entire pipeline works end-to-end

## Accepted Deviations (gap closure)

1. **Two-VM Phase 2 pipeline.** The original Phase 1 single-VM workflow is
   replaced by the real topology: `--phase 2` two-VM provisioning,
   `setup-harness.sh` deployment, and the Phase 2 quick-mode run. Phase 1
   cannot be exercised this way — it runs in CI by design.
2. **Per-step timeouts.** The original spec's `timeout` wrappers are
   replaced by the scripts' own bounded waits (vm-provision.sh SSH budget,
   run-phase2.sh harness execution); the workflow's `--dry-run` mode
   covers step listing without real provisioning.
