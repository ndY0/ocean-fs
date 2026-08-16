---
name: vm-deploy
description: "Deploy the latest OceanFS code to the load-test SUT VM. Use when the user asks to build and deploy the current code, sync the repo to the harness, install the oceanfs binary on the SUT, or prepare the VMs after provisioning or after a code change. Triggers: \"vm-deploy\", \"deploy to the SUT\", \"build and deploy\", \"update the test VMs\", \"sync the latest code\"."
---

# vm-deploy

Build on the **Harness VM** (it has the Rust toolchain) and deploy the
binary + systemd unit + observability stack to the **SUT VM**, then verify
SUT health. This is exactly what `scripts/setup-harness.sh` does — call it,
do not hand-roll the SSH steps.

Per ADR-0019: the SUT VM has NO Rust toolchain. Builds happen on the
Harness; the binary crosses to the SUT over the internal network.

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `branch` | Branch to ensure on the Harness before building | from provisioning record (or `main`) |
| `commit` | Exact commit to check out | from provisioning record |
| `repo` | Git repo URL | from provisioning record |
| `provision-file` | Provisioning record path | newest `.hetzner/provision-*.json` |

## Prerequisites

- VMs provisioned (**vm-up**) and reachable over SSH
- Private SSH key for the VMs available on the laptop (default: the
  record's `ssh_key` path with `.pub` stripped)

## Procedure

1. **Locate the record** (newest, or `--provision-file`):

   ```bash
   PROVISION_FILE=$(ls -t .hetzner/provision-*.json 2>/dev/null | head -1)
   ```

2. **Run the deployment pipeline** (from the repo root):

   ```bash
   ./scripts/setup-harness.sh \
     ${PROVISION_FILE:+--provision-file "$PROVISION_FILE"} \
     ${BRANCH:+--branch "$BRANCH"} \
     ${COMMIT:+--commit "$COMMIT"} \
     ${REPO:+--repo "$REPO"}
   ```

   `setup-harness.sh` performs, in order:
   1. Seeds the harness's SSH identity so the harness can reach the SUT
      over the internal network (required for crash control).
   2. Syncs the repo on the harness (fetch/checkout `branch`, optional
      `commit`) and runs `cargo build --release -p oceanfs -p e2e`.
   3. Deploys to the SUT over the internal network via
      `scripts/sut-deploy.sh`: installs `/usr/local/bin/oceanfs`, writes
      `/etc/oceanfs/oceanfs.toml` (shortened GC/AE/scrub intervals for
      load tests) and the systemd unit `oceanfs.service`
      (**`Restart=no`** — required so the harness's SIGKILL crash-control
      keeps the process down until it issues the restart).
   4. Ensures the observability stack on the SUT
      (`setup-observability.sh`: Prometheus :9090 + Node Exporter textfile
      collector). Non-fatal if it fails — the harness scrape still covers
      the run.
   5. Verifies SUT health over the internal network
      (`http://<sut-internal>:9000/admin/health`).

   Use `--dry-run` first if unsure about the record's targets.

3. **Report the outcome** as JSON (see below). If any step failed, the
   script exits non-zero with the failing step in stderr — relay it.

## Returns

```json
{
  "commit": "abc1234",
  "branch": "main",
  "sut": { "internal_ip": "10.0.0.5", "service": "oceanfs", "port": 9000 },
  "build": { "machine": "harness", "packages": ["oceanfs", "e2e"], "status": "ok" },
  "deploy": { "binary": "/usr/local/bin/oceanfs", "systemd": "oceanfs.service (Restart=no)", "status": "ok" },
  "observability": { "prometheus": "http://10.0.0.5:9090", "status": "ok" },
  "sut_health": "http://10.0.0.5:9000/admin/health -> 200"
}
```

## Notes

- Deployment always uses the code from the **harness's** clone — pass
  `branch`/`commit` to pin the exact code.
- The harness→SUT key seeding means later SSH crash control
  (`TARGET_HOST_SSH`) works from the harness without the laptop's key.
- Next: **vm-test-phase** to run the load test, **observe.sh** + Grafana
  to watch it live.
