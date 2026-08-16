---
name: vm-status
description: "Check the status of the OceanFS load-test VMs (SUT + Harness) in the two-VM Hetzner topology. Use when the user asks whether the test VMs are running, whether oceanfs/prometheus/TTL services are active, or wants the current topology state before running vm-test-phase. Triggers: \"vm-status\", \"status of the VMs\", \"is the SUT up\", \"are the test VMs running\"."
---

# vm-status

Report the two-VM load-test topology state (SUT VM + Harness VM, per ADR-0019).
The source of truth is the provisioning record written by `vm-provision.sh`:
`.hetzner/provision-<prefix>.json` (gitignored, local to the laptop).

## Prerequisites

- `jq` installed on the laptop
- SSH key from the provisioning record installed locally (the VMs were
  provisioned with it — `root@<public_ip>` must work)

## Procedure

1. **Locate the provisioning record** — newest by default, or by prefix:

   ```bash
   PROVISION_FILE=$(ls -t .hetzner/provision-*.json 2>/dev/null | head -1)
   # or with an explicit prefix:
   PROVISION_FILE=".hetzner/provision-${PREFIX}.json"
   ```

   If no record exists, report `{ "status": "no_record" }` and tell the user
   to run **vm-up** first — there is nothing to check.

2. **Read the topology from the record:**

   ```bash
   SUT_PUB=$(jq -r '.sut.public_ip // empty' "$PROVISION_FILE")
   SUT_INT=$(jq -r '.sut.internal_ip // empty' "$PROVISION_FILE")
   SUT_TYPE=$(jq -r '.sut.type // empty' "$PROVISION_FILE")
   HARNESS_PUB=$(jq -r '.harness.public_ip // empty' "$PROVISION_FILE")
   HARNESS_TYPE=$(jq -r '.harness.type // empty' "$PROVISION_FILE")
   # The record carries no top-level name — the prefix is the filename:
   PREFIX=$(basename "$PROVISION_FILE" | sed 's/^provision-//; s/\.json$//')
   ```

3. **Live checks over SSH** (BatchMode so a dead VM fails fast instead of
   hanging on a password prompt). SUT:

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${SUT_PUB}" \
     'systemctl is-active oceanfs; systemctl is-active prometheus; \
      systemctl is-active oceanfs-ttl.timer; uptime -s; date -u +%Y-%m-%dT%H:%M:%SZ'
   ```

   Harness (no oceanfs/prometheus — those run on the SUT only):

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${HARNESS_PUB}" \
     'systemctl is-active oceanfs-ttl.timer; uptime -s; date -u +%Y-%m-%dT%H:%M:%SZ'
   ```

   A failed SSH (exit != 0) means the VM is unreachable — mark it
   `"unreachable"` with the ssh error, do not guess.

4. **Tunnel status** (feeds the persistent laptop Prometheus): check whether
   the observe.sh tunnel to the SUT Prometheus is up:

   ```bash
   curl -sf --max-time 2 http://localhost:9090/-/healthy && echo up || echo down
   ```

## Returns

Return a structured JSON object:

```json
{
  "prefix": "oceanfs-loadtest-2",
  "record": ".hetzner/provision-oceanfs-loadtest-2.json",
  "sut": {
    "name": "oceanfs-loadtest-2-sut",
    "status": "running",
    "public_ip": "1.2.3.4",
    "internal_ip": "10.0.0.5",
    "type": "cx23",
    "oceanfs": "active",
    "prometheus": "active",
    "ttl_timer": "active",
    "booted": "2026-08-16T08:00:00Z"
  },
  "harness": {
    "name": "oceanfs-loadtest-2-harness",
    "status": "running",
    "public_ip": "1.2.3.5",
    "internal_ip": "10.0.0.6",
    "type": "cx23",
    "ttl_timer": "active",
    "booted": "2026-08-16T08:01:00Z"
  },
  "tunnel": { "up": true, "url": "http://localhost:9090" }
}
```

`status` is `running`, `stopped` (powered off), `unreachable` (ssh failed),
or `not_found` (VM deleted but record stale — suggest vm-down cleanup).

## Notes

- The SUT firewall keeps `:9000`/`:9001` internal-only and `:9090` closed to
  the internet; the tunnel (`scripts/observe.sh`) is the only way to reach
  Prometheus from the laptop. If `tunnel.up` is false, run
  `./scripts/observe.sh` (or `--sut <public_ip>`).
- After `vm-down`, records are deleted — vm-status then reports
  `no_record`, which is the expected clean state.
