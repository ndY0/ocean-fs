---
feature: "VM Skills — Agent Commands for VM Lifecycle Management"
epic: "agent-skills"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: operational-tooling/vm-provisioning
    reason: Need vm-provision.sh script for vm-up and vm-down
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# VM Skills — Agent Commands for VM Lifecycle Management

## Summary

Create four OpenCode skill files under `.opencode/skills/` that agents use to
manage the load test VM lifecycle: `vm-status`, `vm-up`, `vm-down`, and
`vm-deploy`. Each skill is a concise instruction file that tells the agent
what command to execute via SSH and what to return. These skills abstract
away SSH connection details, providing a consistent interface for all agents
(Architect, Reviewer, Implementer) to interact with the load test VM.

## Scope

### In Scope

#### `vm-status.md`
- SSH to VM (hostname from `~/.ssh/config` alias `oceanfs-vm`)
- Check if OceanFS process is running: `systemctl is-active oceanfs`
- Check if Prometheus is running: `systemctl is-active prometheus`
- Return structured status: `{status: "running"|"stopped", ip: "...", oceanfs: "active"|"inactive", prometheus: "active"|"inactive", uptime: "...", cost_to_date: "..."}`
- If VM doesn't exist or is unreachable, return `{status: "not_found"}` with error details

#### `vm-up.md`
- Invoke `scripts/vm-provision.sh --phase {phase} --branch {branch}`
- Accepts: `phase` (required, 1-6), `branch` (optional, default main), `provider` (optional)
- Return: `{ip: "...", name: "...", phase: N, provider: "..."}`
- If VM already running, return existing IP (idempotent)
- Updates `~/.ssh/config` with the VM's IP under alias `oceanfs-vm`

#### `vm-down.md`
- Invoke `scripts/vm-provision.sh --destroy {name}`
- Before teardown: rsync Prometheus TSDB snapshot and load reports to persistent storage (optional, controlled by `--preserve-data` flag)
- Return: `{destroyed: true, preserved_data: true|false}`
- If VM doesn't exist, return success (idempotent)

#### `vm-deploy.md`
- Rsync workspace to VM: `rsync -avz --exclude target . oceanfs-vm:~/ocean-fs/`
- SSH: `cd ~/ocean-fs && cargo build --release -p oceanfs -p e2e`
- Accepts: `branch` (optional — if provided, `git checkout {branch} && git pull` before build)
- Return: `{commit: "...", build_duration_secs: N, build_success: true|false}`
- On build failure, return stderr output

### Out of Scope

- Agent authentication or SSH key management (assumes `~/.ssh/config` is pre-configured)
- Multi-VM cluster deployment (single VM for Phases 1-4)
- Automated cost reporting beyond `hcloud server describe`
- VM performance tuning (kernel params, ulimits) — handled by vm-provision.sh

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/`. |

## Interface (Public API)

Each skill is a Markdown file with a YAML-like structure that an agent can parse:

```markdown
# vm-status

Check the status of the OceanFS load test VM.

## Command
ssh oceanfs-vm "systemctl is-active oceanfs; systemctl is-active prometheus; uptime -s"

## Returns
{
  "status": "running" | "stopped" | "not_found",
  "ip": "...",
  "oceanfs": "active" | "inactive",
  "prometheus": "active" | "inactive",
  "uptime": "2026-08-05T10:00:00Z"
}
```

## Data Flow

```
Agent: vm-up phase=2
  → ./scripts/vm-provision.sh --phase 2 --branch main
  → Script: hcloud server create → wait → install deps → build
  → returns { ip: "1.2.3.4", name: "oceanfs-loadtest-2", phase: 2 }
  → Agent updates ~/.ssh/config: Host oceanfs-vm → HostName 1.2.3.4

Agent: vm-deploy
  → rsync workspace to oceanfs-vm:~/ocean-fs/
  → ssh oceanfs-vm "cd ocean-fs && git log -1 --format='%H' && cargo build --release"
  → returns { commit: "abc1234", build_duration_secs: 120, build_success: true }

Agent: vm-status
  → ssh oceanfs-vm "systemctl is-active oceanfs && systemctl is-active prometheus && uptime -s"
  → returns { status: "running", oceanfs: "active", prometheus: "active", uptime: "..." }

Agent: vm-down
  → rsync oceanfs-vm:~/ocean-fs/target/load-reports/ ./local-results/
  → ./scripts/vm-provision.sh --destroy oceanfs-loadtest-2
  → returns { destroyed: true, preserved_data: true }
```

## Definition of Done

- [ ] **Files:** `.opencode/skills/vm-status.md` exists with full command and return schema
- [ ] **Files:** `.opencode/skills/vm-up.md` exists with parameter and return schema
- [ ] **Files:** `.opencode/skills/vm-down.md` exists with parameter and return schema
- [ ] **Files:** `.opencode/skills/vm-deploy.md` exists with parameter and return schema
- [ ] **Validation:** Each skill file is syntactically valid (can be parsed by an agent)
- [ ] **Validation:** `vm-up` skill correctly invokes `scripts/vm-provision.sh` with all parameters
- [ ] **Validation:** `vm-deploy` skill includes both rsync and cargo build steps
- [ ] **Docs:** Each skill file documents its purpose, inputs, outputs, and error conditions
- [ ] **Integration:** An agent can execute the full lifecycle: vm-up → vm-deploy → vm-status → vm-down using only these skill files
