---
name: vm-up
description: "Provision the OceanFS load-test VMs (SUT + Harness) on Hetzner Cloud for the two-VM test topology (ADR-0019). Use when the user asks to spin up the test VMs, create the cloud test infrastructure, or prepare a phase 2+ run on cloud VMs. Triggers: \"vm-up\", \"provision the VMs\", \"spin up the test VMs\", \"create the two VMs\", \"set up cloud VMs for phase N\"."
---

# vm-up

Provision the two-VM load-test topology on Hetzner Cloud per ADR-0019:
a **SUT VM** (OceanFS + Prometheus, no toolchain) and a **Harness VM**
(Rust toolchain + e2e harness), connected over the internal network.
All cost guardrails (hard size cap, confirmation gate, auto-shutdown TTL,
budget gate) are enforced **inside** `scripts/vm-provision.sh` — the skill
never bypasses them.

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `phase` | Load-test phase: 2 (CX23+CX23), 3-4 (CX33+CX23) | required |
| `branch` | Git branch cloned on the Harness | `main` |
| `commit` | Optional exact commit to check out | none |
| `name` | VM name prefix | `oceanfs-loadtest-{phase}` |
| `single-vm` | Budget mode: co-locate harness+SUT on one VM (Phase 2 only) | off |
| `ttl` | Auto-shutdown TTL in hours | 4 (`LOAD_TEST_TTL_HOURS`) |
| `confirm` | Confirmation for CX33+ | auto-pass `yes` for phases 3-4 |
| `ssh-key` | SSH public key path for the VMs | `.hetzner/.ssh/hetzner-ssh.pub` (loaded into the agent by `scripts/lib/env-hetzner.sh`); fallback `~/.ssh/id_rsa.pub` |

Phase 1 runs in CI — no VMs: `vm-provision.sh --phase 1` prints
"Phase 1 runs in CI, no cloud VMs needed". Phases 5+ use a separate
provisioning model (the script prints guidance).

## Prerequisites

- `hcloud` CLI authenticated (`HCLOUD_TOKEN` is auto-loaded from
  `.hetzner/.env` by `scripts/lib/env-hetzner.sh`, which also ensures
  ssh-agent and adds `.hetzner/.ssh/hetzner-ssh`)
- `jq`, `ssh`, and the SSH public key from `.hetzner/.ssh/`
  (default provisioning key; override with `--ssh-key PATH`)

## Procedure

1. **Idempotency check** — if a record exists and both VMs are running,
   do NOT re-provision (vm-provision.sh fails fast on name collisions):

   ```bash
   ./scripts/vm-provision.sh --status "${PREFIX:-oceanfs-loadtest-${PHASE}}"
   ```

   If both VMs are `running`, report the existing record
   (`.hetzner/provision-<prefix>.json`) and stop.

2. **Provision:**

   ```bash
   # Phases 3-4 require the confirmation gate: set CONFIRM=yes so the
   # script receives --confirm yes (the agent intends to provision).
   CONFIRM=""
   if [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
       CONFIRM="yes"
   fi

   ./scripts/vm-provision.sh --phase "${PHASE}" \
     --branch "${BRANCH:-main}" \
     ${COMMIT:+--commit "$COMMIT"} \
     ${NAME:+--name "$NAME"} \
     ${SSH_KEY:+--ssh-key "$SSH_KEY"} \
     ${SINGLE_VM:+--single-vm} \
     ${TTL:+--ttl "$TTL"} \
     ${CONFIRM:+--confirm yes}
   ```

   - Always pass `--confirm yes` for phases 3-4 (the agent intends to
     provision; the script prints the cost estimate with the confirmation).
   - The script installs the TTL timer on both VMs, applies managed
     firewalls (SUT: SSH + internal-net :9000/:9001; Harness: SSH only),
     configures the Harness (Rust toolchain, repo clone at `branch`/
     `commit`, `cargo build --release -p oceanfs -p e2e`), installs the
     observability stack on the SUT, and **writes the provisioning
     record** to `.hetzner/provision-<prefix>.json`.
   - It does NOT deploy the oceanfs binary/systemd unit to the SUT —
     that is **vm-deploy** (setup-harness.sh also re-syncs the repo so
     the deployed code is the pinned branch/commit).
   - Provisioning takes several minutes (VMs boot, apt, rustup, release
     build on the Harness). Report progress, not a hang.

3. **Ensure `~/.ssh/config` aliases** (convenience for later skills and
   `observe.sh`); idempotent — replace the HostName of existing aliases:

   ```
   Host oceanfs-sut      → HostName <sut-public-ip>
   Host oceanfs-harness  → HostName <harness-public-ip>
   ```

4. **Return the record** (`jq` the JSON the script wrote).

## Returns

```json
{
  "phase": 2,
  "prefix": "oceanfs-loadtest-2",
  "record": ".hetzner/provision-oceanfs-loadtest-2.json",
  "sut":     { "name": "oceanfs-loadtest-2-sut",     "public_ip": "1.2.3.4", "internal_ip": "10.0.0.5", "type": "cx23" },
  "harness": { "name": "oceanfs-loadtest-2-harness", "public_ip": "1.2.3.5", "internal_ip": "10.0.0.6", "type": "cx23" },
  "ttl_hours": 4,
  "ssh_config": { "oceanfs-sut": "1.2.3.4", "oceanfs-harness": "1.2.3.5" }
}
```

## Errors & next steps

- Script failure → surface the script's stderr (it cleans up created VMs
  unless `--keep-on-failure` was passed).
- The VMs auto-poweroff after the TTL (default 4h). Re-run vm-up (or
  `hcloud server poweron`) to extend a session; vm-status shows the timer.
- Next: run **vm-deploy** (build + deploy + observability), then
  **vm-test-phase**. To watch live metrics: `./scripts/observe.sh` +
  Grafana (`docker compose -f mcps/docker-compose.yml up -d prometheus grafana`).
