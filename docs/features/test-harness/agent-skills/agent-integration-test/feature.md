---
feature: "Agent Integration Test — End-to-End Agent Workflow Validation"
epic: "agent-skills"
status: proposed
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
  - epic: test-phase-implementations/phase1-concurrency-test
    reason: Need Phase 1 test to run during validation
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# Agent Integration Test — End-to-End Agent Workflow Validation

## Summary

Create `scripts/test-agent-workflow.sh` — a manual integration test that exercises
the full agent workflow end-to-end: (1) provision a VM, (2) deploy the latest code,
(3) run Phase 1 load test, (4) fetch results, (5) assert the test passed, (6) tear
down the VM. This is a manual smoke test for the entire test infrastructure
pipeline — it validates that all the pieces (vm-provisioning, deployment, test
execution, results fetching) work together correctly. It is NOT a CI test (it
provisions real cloud VMs and runs a multi-minute load test). It's intended to
be run by a developer or agent before declaring the test infrastructure ready.

## Scope

### In Scope

- `scripts/test-agent-workflow.sh` — single shell script
- Workflow steps:
  1. **Provision:** `./scripts/vm-provision.sh --phase 1 --provider hetzner --branch main`
     - Assert: script exits 0, JSON on stdout contains valid IP
     - Capture: VM_IP, VM_NAME
  2. **Deploy:** `rsync` workspace to VM_IP, `cargo build --release -p oceanfs -p e2e`
     - Assert: build exit code 0
     - Capture: build duration
  3. **Setup observability:** `ssh root@{VM_IP} "cd ocean-fs && ./scripts/setup-observability.sh"`
     - Assert: Prometheus is running on :9090
  4. **Run Phase 1:** `ssh root@{VM_IP} "cd ocean-fs && LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=60 cargo test -p e2e -- load_concurrency --nocapture"`
     - Assert: test exit code 0
  5. **Fetch results:** `rsync root@{VM_IP}:~/ocean-fs/target/load-reports/ ./local-results/`
     - Assert: at least one JSON report file exists
     - Parse: verify `report.result == "pass"`, `report.manifest.mismatches == 0`
  6. **Teardown:** `./scripts/vm-provision.sh --destroy {VM_NAME}`
     - Assert: script exits 0
  7. **Summary:** Print overall pass/fail with timing for each step
- Configurable via env vars:
  - `WORKFLOW_PROVIDER` (default `hetzner`)
  - `WORKFLOW_PHASE` (default `1`)
  - `WORKFLOW_BRANCH` (default `main`)
  - `WORKFLOW_DURATION_SECS` (default `60`)
- Timeout per step: 15 minutes for provision, 15 minutes for build, 5 minutes for test, 2 minutes for teardown
- Cleanup on failure: if any step fails, attempt teardown before exiting (best-effort)
- Output: JSON summary to stdout with per-step timing and pass/fail

### Out of Scope

- CI integration (this is a manual test; CI would need cloud credentials)
- Multi-phase test execution (Phase 1 only for the integration test)
- Performance benchmarking or regression comparison
- Automated retry on transient failures

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Shell script only. |

## Interface (Public API)

```
Usage: ./scripts/test-agent-workflow.sh [OPTIONS]

ENVIRONMENT VARIABLES:
  WORKFLOW_PROVIDER       Cloud provider (default: hetzner)
  WORKFLOW_PHASE          Load test phase (default: 1)
  WORKFLOW_BRANCH         Git branch (default: main)
  WORKFLOW_DURATION_SECS  Test duration in seconds (default: 60)

Output (stdout): JSON summary with per-step timing and overall pass/fail
Exit code: 0 on success, 1 on failure
```

## Data Flow

```
./scripts/test-agent-workflow.sh

  STEP 1: Provision VM
    → ./scripts/vm-provision.sh --phase 1 --provider hetzner
    → VM provisioned: IP=1.2.3.4, name=oceanfs-loadtest-1
    [OK] Provision: 180s

  STEP 2: Deploy code
    → rsync workspace to 1.2.3.4
    → ssh 1.2.3.4 "cargo build --release -p oceanfs -p e2e"
    [OK] Deploy: 240s (commit abc1234)

  STEP 3: Setup observability
    → ssh 1.2.3.4 "cd ocean-fs && ./scripts/setup-observability.sh"
    → Prometheus is running
    [OK] Observability: 30s

  STEP 4: Run Phase 1 test
    → ssh 1.2.3.4 "LOAD_TEST_SEED=42 ... cargo test -p e2e -- load_concurrency"
    → Test passed
    [OK] Phase 1: 75s

  STEP 5: Fetch results
    → rsync 1.2.3.4:~/ocean-fs/target/load-reports/ → ./local-results/
    → Parsed: result=pass, objects_written=1234, mismatches=0
    [OK] Results: 5s

  STEP 6: Teardown
    → ./scripts/vm-provision.sh --destroy oceanfs-loadtest-1
    [OK] Teardown: 15s

  OVERALL: PASS (total: 545s)
```

## Definition of Done

- [ ] **Script:** `scripts/test-agent-workflow.sh` is executable
- [ ] **Script:** All 6 workflow steps complete in sequence
- [ ] **Script:** Step 1 provisions a real cloud VM (not mocked)
- [ ] **Script:** Step 4 runs Phase 1 load test and it passes
- [ ] **Script:** Step 5 parses the LoadReport JSON and validates `result == "pass"` and `mismatches == 0`
- [ ] **Script:** On any step failure, prints error, attempts teardown, exits non-zero
- [ ] **Script:** On overall pass, prints JSON summary with per-step timing
- [ ] **Script:** Timeout per step prevents hanging (provision: 15min, build: 15min, test: 5min)
- [ ] **Docs:** Script header documents all steps, environment variables, and prerequisites (HCLOUD_TOKEN)
- [ ] **Docs:** README entry explains when to run this test (before declaring test infra ready for use)
- [ ] **Integration:** A developer or agent can run this script on their laptop and verify the entire pipeline works end-to-end
