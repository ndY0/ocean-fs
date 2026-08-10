---
feature: "VM Skills — Agent Commands for Two-VM Lifecycle Management"
epic: "agent-skills"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: operational-tooling/vm-provisioning
    reason: Need vm-provision.sh script for two-VM provisioning
adr:
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-08-05
updated: 2026-08-10
---

# VM Skills — Agent Commands for Two-VM Lifecycle Management

## Summary

Create four OpenCode skill files under `.opencode/skills/` that agents use to
manage the two-VM test topology (SUT VM + Harness VM, per ADR-0019):
`vm-status`, `vm-up`, `vm-down`, and `vm-deploy`. Each skill is a concise
instruction file that tells the agent what command to execute via SSH and what
to return. These skills abstract away the two-VM complexity, providing a
consistent interface for all agents (Architect, Reviewer, Implementer) to
interact with the load test infrastructure.

## Scope

### In Scope

#### `vm-status.md`
- SSH to **both** VMs (hostnames from `~/.ssh/config` aliases `oceanfs-sut` and `oceanfs-harness`)
- Check if OceanFS process is running on SUT VM: `systemctl is-active oceanfs`
- Check if Prometheus is running on SUT VM: `systemctl is-active prometheus`
- Check if TTL timer is active on both VMs: `systemctl is-active oceanfs-ttl.timer`
- Return structured two-VM status:
  ```json
  {
    "sut": {
      "status": "running",
      "ip": "10.0.0.5",
      "public_ip": "1.2.3.4",
      "type": "cx32",
      "oceanfs": "active",
      "prometheus": "active",
      "ttl_timer": "active",
      "uptime": "2026-08-10T10:00:00Z"
    },
    "harness": {
      "status": "running",
      "ip": "10.0.0.6",
      "public_ip": "1.2.3.5",
      "type": "cx22",
      "ttl_timer": "active",
      "uptime": "2026-08-10T10:00:00Z"
    }
  }
  ```
- If either VM doesn't exist or is unreachable, set `status: "not_found"` with error details

#### `vm-up.md`
- Determine phase → VM types via `scripts/vm-provision.sh --phase {phase} --dry-run` for sizing info
- Invoke `scripts/vm-provision.sh --phase {phase} --branch {branch} [--confirm yes] [--single-vm] [--ttl N]`
- Accepts: `phase` (required, 1-6), `branch` (optional, default main), `provider` (optional), `confirm` (auto-passes `--confirm yes` for phases 3-4), `single-vm` (optional, for Phase 2 budget mode), `ttl` (optional, default 4h)
- Return: two-VM JSON object (see vm-provisioning output format)
- If VMs already running, return existing IPs (idempotent)
- Updates `~/.ssh/config` with two entries:
  - `Host oceanfs-sut` → `HostName <sut-public-ip>`
  - `Host oceanfs-harness` → `HostName <harness-public-ip>`
- For Phase 3-4, skill file instructs agent to always pass `--confirm yes` (since agent intends to provision)
- For Phase 1, prints "Phase 1 runs in CI, no VMs needed" (no provisioning)

#### `vm-down.md`
- Invoke `scripts/vm-provision.sh --destroy {name}` (tears down both VMs)
- Before teardown: rsync Prometheus TSDB snapshot and load reports from both VMs to persistent storage (controlled by `--preserve-data` flag)
- Return: `{destroyed: true, sut: {name: "...", destroyed: true}, harness: {name: "...", destroyed: true}, preserved_data: true|false}`
- If VMs don't exist, return success (idempotent)

#### `vm-deploy.md`
- **Build on Harness VM** (has Rust toolchain): `cd ~/ocean-fs && cargo build --release -p oceanfs -p e2e`
- **Deploy binary to SUT VM**: `scp ~/ocean-fs/target/release/oceanfs oceanfs-sut:~/oceanfs`
- SUT VM does NOT have Rust installed — binary is cross-deployed via `scp` over internal network
- Accepts: `branch` (optional — if provided, `git checkout {branch} && git pull` on Harness VM before build)
- Return: `{commit: "...", build_duration_secs: N, build_success: true|false, deploy_success: true|false}`
- On build failure, return stderr output
- On deploy failure (scp error), return error details
- Also syncs workspace to Harness VM via rsync if needed: `rsync -avz --exclude target . oceanfs-harness:~/ocean-fs/`

### Out of Scope

- Agent authentication or SSH key management (assumes `~/.ssh/config` is pre-configured with both aliases)
- Automated cost reporting beyond `hcloud server describe` (vm-status shows VM type, not cost-to-date in MVP)
- VM performance tuning (kernel params, ulimits) — handled by vm-provision.sh
- `vm-deploy` does NOT install Prometheus on SUT VM (that's feature 3.1: prometheus-grafana-setup)

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/`. |

## Interface (Public API)

Each skill is a Markdown file with a YAML-like structure that an agent can parse:

```markdown
# vm-status

Check the status of both OceanFS load test VMs (SUT + Harness).

## Command
ssh oceanfs-sut "systemctl is-active oceanfs; systemctl is-active prometheus; systemctl is-active oceanfs-ttl.timer; uptime -s"
ssh oceanfs-harness "systemctl is-active oceanfs-ttl.timer; uptime -s"

## Returns
{ sut: { status, ip, public_ip, type, oceanfs, prometheus, ttl_timer, uptime },
  harness: { status, ip, public_ip, type, ttl_timer, uptime } }
```

## Data Flow

```
Agent: vm-up phase=2
  → ./scripts/vm-provision.sh --phase 2 --branch main
  → Script: creates SUT CX22 + Harness CX22 in same Hetzner network
  → Script: installs Rust + builds oceanfs/e2e on Harness VM only
  → returns { sut: { ip: "10.0.0.5", ... }, harness: { ip: "10.0.0.6", ... } }
  → Agent updates ~/.ssh/config:
      Host oceanfs-sut → HostName <sut-public-ip>
      Host oceanfs-harness → HostName <harness-public-ip>

Agent: vm-deploy
  → ssh oceanfs-harness "cd ~/ocean-fs && git pull && cargo build --release -p oceanfs -p e2e"
  → scp oceanfs-harness:~/ocean-fs/target/release/oceanfs oceanfs-sut:~/oceanfs
  → returns { commit: "abc1234", build_duration_secs: 120, build_success: true, deploy_success: true }

Agent: vm-status
  → ssh oceanfs-sut "systemctl is-active oceanfs && systemctl is-active prometheus && uptime -s"
  → ssh oceanfs-harness "uptime -s"
  → returns { sut: { status: "running", oceanfs: "active", ... }, harness: { status: "running", ... } }

Agent: vm-down
  → rsync oceanfs-sut:~/ocean-fs/target/load-reports/ ./local-results/
  → ./scripts/vm-provision.sh --destroy oceanfs-loadtest-2
  → returns { destroyed: true, preserved_data: true }
```

## Definition of Done

- [ ] **Files:** `.opencode/skills/vm-status.md` exists with two-VM return schema
- [ ] **Files:** `.opencode/skills/vm-up.md` exists with parameters (phase, branch, confirm, single-vm, ttl) and two-VM return schema
- [ ] **Files:** `.opencode/skills/vm-down.md` exists with `--preserve-data` parameter and two-VM teardown schema
- [ ] **Files:** `.opencode/skills/vm-deploy.md` exists with build-on-harness + scp-to-sut workflow
- [ ] **Validation:** Each skill file is syntactically valid (can be parsed by an agent)
- [ ] **Validation:** `vm-up` skill correctly passes `--confirm yes` for Phase 3-4
- [ ] **Validation:** `vm-up` skill correctly handles Phase 1 (prints "runs in CI" and exits)
- [ ] **Validation:** `vm-deploy` skill includes both `cargo build` on Harness VM and `scp` to SUT VM
- [ ] **Validation:** `vm-down` skill tears down both VMs and supports `--preserve-data`
- [ ] **Docs:** Each skill file documents its purpose, inputs, outputs, and error conditions
- [ ] **Integration:** An agent can execute the full two-VM lifecycle: vm-up → vm-deploy → vm-status → vm-down using only these skill files
- [ ] **Integration:** SSH config correctly configured with two aliases (`oceanfs-sut`, `oceanfs-harness`)
