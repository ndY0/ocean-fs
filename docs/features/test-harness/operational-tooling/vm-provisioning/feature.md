---
feature: "VM Provisioning — Cloud VM Lifecycle for Load Test Phases"
epic: "operational-tooling"
status: proposed
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# VM Provisioning — Cloud VM Lifecycle for Load Test Phases

## Summary

Create `scripts/vm-provision.sh` to provision cloud VMs sized appropriately for
each load test phase. Uses a cloud provider CLI (`hcloud` for Hetzner as the
primary target) to create a VM with the right specs (vCPU, RAM, disk), install
Rust via `rustup`, clone the OceanFS repo at a specified branch/commit, and
build the binaries. Returns the VM IP address. Accepts `--phase N`, `--provider`,
`--branch`, and `--ssh-key` arguments. Designed for both human and agent
invocation (the `vm-up` skill wraps this script). Start with Hetzner support;
other providers are extensible.

## Scope

### In Scope

- `scripts/vm-provision.sh` — single script
- CLI arguments:
  - `--phase N` — selects VM size: phase 1-2 (CX22: 2 vCPU, 4 GB, 40 GB), phase 3-4 (CX32: 4 vCPU, 8 GB, 80 GB), phase 5+ (separate provisioning model, not covered by this script)
  - `--provider PROVIDER` — `hetzner` (default), `gcp`, `aws` (extensible)
  - `--branch BRANCH` — git branch to clone (default `main`)
  - `--commit SHA` — specific commit to check out (optional)
  - `--ssh-key PATH` — path to SSH public key for VM access (default `~/.ssh/id_rsa.pub`)
  - `--name NAME` — VM name prefix (default `oceanfs-loadtest-{phase}`)
  - `--image IMAGE` — OS image (default `ubuntu-24.04`)
  - `--dry-run` — print what would be done without provisioning
- Provisioning steps (per provider):
  1. Create VM with specified image, type, ssh-key
  2. Wait for VM to be ready (poll `hcloud server describe`)
  3. Install dependencies: `apt-get update && apt-get install -y build-essential pkg-config libssl-dev curl`
  4. Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y`
  5. Clone repo: `git clone https://github.com/.../ocean-fs.git --branch $BRANCH`
  6. If `--commit`, `git checkout $SHA`
  7. Build: `cargo build --release -p oceanfs -p e2e`
  8. Print VM IP address to stdout
- Output: JSON to stdout with `{"ip": "...", "name": "...", "phase": N, "provider": "..."}`
- Error handling: on failure at any step, print error to stderr, attempt cleanup, exit non-zero
- `--destroy` flag: tear down a previously provisioned VM by name
- `--status` flag: check if VM exists and is running, print IP

### Out of Scope

- Persistent volume for Prometheus data across VM teardown/re-provision (the Prometheus data is ephemeral within the 7-day retention window; for historical storage, rsync the JSON reports and the Prometheus TSDB snapshot before teardown)
- Terraform/OpenTofu orchestration (a simple shell script is sufficient for single-VM provisioning)
- Multi-VM cluster provisioning (Phase 5 uses a separate deployment model — see `load-test-campaign.md` §6)
- Cost tracking or budget enforcement
- Kubernetes cluster provisioning

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Shell script only. |

## Interface (Public API)

```
Usage: ./scripts/vm-provision.sh [OPTIONS]

OPTIONS:
  --phase N            Load test phase (1-6). Determines VM size. [required]
  --provider NAME      Cloud provider: hetzner (default), gcp, aws
  --branch BRANCH      Git branch to clone (default: main)
  --commit SHA         Specific commit to checkout
  --ssh-key PATH       SSH public key path (default: ~/.ssh/id_rsa.pub)
  --name NAME          VM name prefix (default: oceanfs-loadtest-{phase})
  --image IMAGE        OS image (default: ubuntu-24.04)
  --dry-run            Print actions without executing
  --destroy NAME       Tear down VM with given name
  --status NAME        Check VM status
  -h, --help           Show this help

Output (stdout): JSON { "ip": "...", "name": "...", "phase": N, "provider": "..." }
```

## Data Flow

```
$ ./scripts/vm-provision.sh --phase 2 --branch main

  → hcloud server create --name oceanfs-loadtest-2 --type cx22 --image ubuntu-24.04 --ssh-key ~/.ssh/id_rsa.pub
  → Poll hcloud server describe until status=running
  → ssh root@{ip} "apt-get update && apt-get install -y build-essential curl ..."
  → ssh root@{ip} "curl ... | sh -s -- -y"  # rustup
  → ssh root@{ip} "git clone ... --branch main"
  → ssh root@{ip} "cd ocean-fs && cargo build --release -p oceanfs -p e2e"
  → echo '{"ip": "1.2.3.4", "name": "oceanfs-loadtest-2", "phase": 2, "provider": "hetzner"}'
```

## Definition of Done

- [ ] **Script:** `scripts/vm-provision.sh` is executable and has `--help` output
- [ ] **Script:** `--dry-run` prints all provisioning steps without executing
- [ ] **Script:** `--phase 1` selects CX22 (2 vCPU, 4 GB, 40 GB); `--phase 3` selects CX32 (4 vCPU, 8 GB, 80 GB)
- [ ] **Script:** Hetzner provider completes full provisioning cycle: create → wait → install → build → print IP
- [ ] **Script:** On failure at any step, prints clear error and exits non-zero
- [ ] **Script:** `--destroy oceanfs-loadtest-2` removes the VM
- [ ] **Script:** `--status oceanfs-loadtest-2` prints JSON with IP and running status
- [ ] **Docs:** Script header documents all arguments, VM size mapping, and provider requirements (HCLOUD_TOKEN env var)
- [ ] **Docs:** README section explains how to set up `hcloud` CLI and authenticate
- [ ] **Integration:** Agent workflow: `vm-up --phase 2` → IP returned → `vm-deploy` → `vm-test-phase 2` → `vm-down`
