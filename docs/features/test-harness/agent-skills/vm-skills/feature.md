---
feature: "VM Skills — Agent Commands for Two-VM Lifecycle Management"
epic: "agent-skills"
status: done
priority: high
owner: ""
dependencies:
  - epic: operational-tooling/vm-provisioning
    reason: Need vm-provision.sh script for two-VM provisioning
adr:
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-08-05
updated: 2026-08-16
---

# VM Skills — Agent Commands for Two-VM Lifecycle Management

> **Deviation (2026-08-17):** the `confirm` gate described below is removed —
> CX33 is the standard sizing for phases 2-4 and `--confirm yes` is a no-op
> (see ADR-0019 Corrigendum 2). DoD items referencing the gate reflect the
> state at implementation time.

## Summary

Create four OpenCode skills under `.opencode/skills/` that agents use to
manage the two-VM test topology (SUT VM + Harness VM, per ADR-0019):
`vm-status`, `vm-up`, `vm-down`, and `vm-deploy`. Each skill is an OpenCode
`SKILL.md` instruction file (`name` + `description` frontmatter, body =
exact commands + return schema) telling the agent what to execute and what
to return. These skills abstract away the two-VM complexity, providing a
consistent interface for all agents (Architect, Reviewer, Implementer) to
interact with the load test infrastructure.

**Gap closure (2026-08-16):** The skills are built against the **actual**
script interfaces after the 2026-08-15/16 script refactor, not the original
design. Source of truth per skill:

| Skill | Backing script(s) | Notes vs. original design |
|---|---|---|
| `vm-status` | `vm-provision.sh --status`, provisioning record `.hetzner/provision-*.json`, SSH `systemctl is-active` checks | Reads the record for IPs/type instead of assuming `~/.ssh/config` aliases (aliases are created by vm-up but the record is the source of truth) |
| `vm-up` | `vm-provision.sh --phase N` | VM types are **cx23/cx33** (Hetzner retired cx22/cx32); writes the provisioning record; the skill (not the script) maintains `~/.ssh/config` aliases; idempotency via `--status` check before provisioning |
| `vm-down` | `vm-provision.sh --destroy NAME` | `--preserve-data` implemented in the skill: fetch harness reports + Prometheus TSDB **before** destroy, then clean the record + ssh aliases |
| `vm-deploy` | `setup-harness.sh` → `sut-deploy.sh` + `setup-observability.sh` | Replaces the hand-rolled "build on harness + scp to SUT" recipe: seeds harness→SUT SSH key, syncs repo, builds `oceanfs`+`e2e` on the harness, deploys binary + config + systemd unit (`Restart=no`) to the SUT, ensures Prometheus, verifies health |

## Scope

### In Scope

#### `vm-status` (`.opencode/skills/vm-status/SKILL.md`)
- Locate the newest `.hetzner/provision-*.json` (or by prefix)
- SSH to both VMs with `BatchMode=yes` and check services:
  - SUT: `systemctl is-active oceanfs`, `systemctl is-active prometheus`, `systemctl is-active oceanfs-ttl.timer`, boot time
  - Harness: `systemctl is-active oceanfs-ttl.timer`, boot time
- Check the observe.sh tunnel to the SUT Prometheus (`curl localhost:9090/-/healthy`)
- Return structured two-VM status (see Interface); unreachable/not_found
  states reported explicitly

#### `vm-up` (`.opencode/skills/vm-up/SKILL.md`)
- Idempotency: `vm-provision.sh --status <prefix>` first; if both VMs are
  running, return the existing record instead of re-provisioning
- Invoke `scripts/vm-provision.sh --phase {phase} --branch {branch} [--name PREFIX] [--commit SHA] [--ssh-key PATH] [--single-vm] [--ttl N]`
- Accepts: `phase` (required, 2-4; Phase 1 → "runs in CI" message; 5+ →
  separate-model guidance), `branch` (default main), `commit`, `name`
  (default `oceanfs-loadtest-{phase}`), `ssh-key` (default
  `~/.ssh/id_rsa.pub`), `single-vm`, `ttl` (default 4h),
  `confirm` (accepted for compatibility; no-op — gate removed 2026-08-17,
  CX33 is the standard sizing for phases 2-4)
- The script writes the provisioning record
  `.hetzner/provision-<prefix>.json` (gitignored) — the source of truth
  for every later skill
- The skill then ensures `~/.ssh/config` aliases
  (`oceanfs-sut` → sut public IP, `oceanfs-harness` → harness public IP),
  idempotent
- Return: the provisioning record (two-VM JSON)

#### `vm-down` (`.opencode/skills/vm-down/SKILL.md`)
- Optional `--preserve-data`: before teardown, rsync load reports from the
  Harness (`/tmp/oceanfs-reports/` → `local-results/`) and snapshot the SUT
  Prometheus TSDB (tar over ssh)
- Invoke `scripts/vm-provision.sh --destroy {prefix}` (idempotent)
- Clean local state: remove ssh aliases + the provisioning record
- Return: `{destroyed: true, sut: {...}, harness: {...}, preserved_data, preserved_paths}`

#### `vm-deploy` (`.opencode/skills/vm-deploy/SKILL.md`)
- Invoke `scripts/setup-harness.sh [--provision-file] [--branch] [--commit] [--repo]`
  — the full deploy pipeline:
  1. seed the harness's SSH identity (harness → SUT over internal net)
  2. sync repo on the harness; `cargo build --release -p oceanfs -p e2e`
  3. `sut-deploy.sh`: scp binary to SUT, write `/etc/oceanfs/oceanfs.toml`
     + systemd unit `oceanfs` with **`Restart=no`** (crash-control contract)
  4. `setup-observability.sh` on the SUT (Prometheus :9090 + textfile
     collector; non-fatal on failure)
  5. verify SUT health over the internal network
- Accepts: `branch`, `commit`, `repo` (defaults from the provisioning record)
- Return: `{commit, build: ok, deploy: ok, observability: ok, sut_health}`
- On failure: relay the failing step's stderr

### Out of Scope

- Agent authentication or SSH key management (uses the key recorded at
  provisioning time)
- Automated cost reporting beyond `hcloud server describe` (vm-status shows
  VM type, not cost-to-date in MVP)
- VM performance tuning (kernel params, ulimits) — handled by vm-provision.sh
- `vm-deploy` does not install Prometheus on the SUT separately —
  `setup-harness.sh` ensures it (feature 3.1 stack, idempotent)

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/<name>/SKILL.md`. |

## Interface (Public API)

Each skill is an OpenCode skill: `.opencode/skills/<name>/SKILL.md` with
frontmatter (`name`, `description`) and a markdown body of exact commands,
return schema, error conditions, and examples. The four skills expose:

```
vm-status  → { prefix, record, sut: {name, status, public_ip, internal_ip, type, oceanfs, prometheus, ttl_timer, booted}, harness: {...}, tunnel: {up, url} }
vm-up      → { phase, prefix, record, sut: {name, public_ip, internal_ip, type}, harness: {...}, ttl_hours, ssh_config }
vm-down    → { destroyed, prefix, sut: {name, destroyed}, harness: {name, destroyed}, preserved_data, preserved_paths, record_deleted }
vm-deploy  → { commit, branch, sut: {internal_ip, service, port}, build: {...}, deploy: {...}, observability: {...}, sut_health }
```

## Data Flow

```
Agent: vm-up phase=2
  → vm-provision.sh --status oceanfs-loadtest-2      (idempotency check)
  → ./scripts/vm-provision.sh --phase 2 --branch main
  → Script: creates SUT CX23 + Harness CX23 in the same Hetzner network,
            firewalls, TTL timer, observability on SUT, provisioning record
  → returns record { sut: { public_ip, internal_ip, type: cx23 }, ... }
  → Agent ensures ~/.ssh/config: oceanfs-sut / oceanfs-harness

Agent: vm-deploy
  → ./scripts/setup-harness.sh --provision-file .hetzner/provision-oceanfs-loadtest-2.json
  → harness: git sync + cargo build --release -p oceanfs -p e2e
  → harness → SUT: sut-deploy.sh (binary + config + systemd Restart=no)
  → SUT: setup-observability.sh (Prometheus, idempotent)
  → returns { commit: "abc1234", build: ok, deploy: ok, sut_health: 200 }

Agent: vm-status
  → jq .sut/.harness from the record; ssh systemctl is-active checks
  → returns { sut: { status: "running", oceanfs: "active", ... }, ... }

Agent: vm-down --preserve-data
  → rsync harness:/tmp/oceanfs-reports/ → local-results/
  → ssh sut tar czf - /var/lib/prometheus/data > local-results/prometheus-*.tar.gz
  → ./scripts/vm-provision.sh --destroy oceanfs-loadtest-2
  → remove ssh aliases + record
  → returns { destroyed: true, preserved_data: true, ... }
```

## Definition of Done

- [x] **Files:** `.opencode/skills/vm-status/SKILL.md` exists with two-VM return schema
- [x] **Files:** `.opencode/skills/vm-up/SKILL.md` exists with parameters (phase, branch, commit, name, single-vm, ttl, confirm) and two-VM return schema
- [x] **Files:** `.opencode/skills/vm-down/SKILL.md` exists with `--preserve-data` parameter and two-VM teardown schema
- [x] **Files:** `.opencode/skills/vm-deploy/SKILL.md` exists with the setup-harness build-on-harness + deploy-to-sut workflow
- [x] **Validation:** Each skill file is a valid OpenCode SKILL.md (name + description frontmatter, folder matches name)
- [x] **Validation:** `vm-up` skill correctly passes `--confirm yes` for Phase 3-4
- [x] **Validation:** `vm-up` skill correctly handles Phase 1 (prints "runs in CI" and exits)
- [x] **Validation:** `vm-deploy` skill delegates to `setup-harness.sh` (build on Harness VM + deploy to SUT VM + observability + health)
- [x] **Validation:** `vm-down` skill tears down both VMs and supports `--preserve-data`
- [x] **Docs:** Each skill file documents its purpose, inputs, outputs, and error conditions
- [x] **Integration:** An agent can execute the full two-VM lifecycle: vm-up → vm-deploy → vm-status → vm-down using only these skill files
- [x] **Integration:** Provisioning record (`.hetzner/provision-*.json`) is the source of truth; `~/.ssh/config` aliases (`oceanfs-sut`, `oceanfs-harness`) maintained by vm-up

## Accepted Deviations (gap closure)

1. **Skill file format.** The original spec's flat `.opencode/skills/vm-*.md`
   "YAML-like structure" is replaced by the real OpenCode skill format
   (`.opencode/skills/<name>/SKILL.md` with `name`/`description`
   frontmatter) so the skills are discoverable by the agent runtime.
2. **VM types.** cx22/cx32 → **cx23/cx33** — Hetzner retired the 22/32
   line; `vm-provision.sh` (2026-08-15) is the authoritative mapping.
3. **`vm-deploy` implementation.** The hand-rolled build+scp recipe is
   replaced by `setup-harness.sh` (which also seeds the harness→SUT SSH
   identity, writes the SUT systemd unit with `Restart=no`, ensures
   observability, and verifies health).
4. **`~/.ssh/config` maintenance moved to the skill.** vm-provision.sh does
   not write ssh config; `vm-up`/`vm-down` maintain the aliases.
5. **`--preserve-data` lives in the skill.** The destroy script has no such
   flag; vm-down fetches reports + Prometheus TSDB before invoking
   `--destroy`, then cleans the record and aliases.
