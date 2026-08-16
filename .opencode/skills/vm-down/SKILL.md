---
name: vm-down
description: "Tear down the OceanFS load-test VMs (SUT + Harness) on Hetzner Cloud. Use when the user asks to destroy the test VMs, shut down the cloud test infrastructure, or clean up after a load test run. Triggers: \"vm-down\", \"tear down the VMs\", \"destroy the VMs\", \"shut down the test VMs\", \"clean up the test infrastructure\"."
---

# vm-down

Tear down the two-VM load-test topology (SUT + Harness) via
`scripts/vm-provision.sh --destroy <prefix>`. Optionally preserves
reports and Prometheus data **before** destruction.

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `name` | VM name prefix to destroy | from the newest `.hetzner/provision-*.json` |
| `preserve-data` | Fetch reports + Prometheus TSDB snapshot before destroy | false |

## Procedure

1. **Resolve the prefix** — explicit, else the newest record:

   ```bash
   # The record carries no top-level name — derive the prefix from the
   # filename (.hetzner/provision-<prefix>.json).
   PREFIX="${NAME:-$(ls -t .hetzner/provision-*.json | head -1 | xargs -I{} basename {} | sed 's/^provision-//; s/\.json$//' 2>/dev/null || true)}"
   ```

2. **Preserve data (only with `--preserve-data`)** — do this BEFORE the
   destroy, while the VMs are still up. Best-effort per item; a failed
   fetch is reported, not fatal:

   ```bash
   # 2a. Load reports from the Harness VM (tmpfs /tmp/oceanfs-reports).
   mkdir -p local-results
   rsync -avz "root@${HARNESS_PUB}:/tmp/oceanfs-reports/" local-results/ \
     || echo "no reports fetched from harness"

   # 2b. Prometheus TSDB snapshot from the SUT VM (optional, larger).
   ssh "root@${SUT_PUB}" \
     "tar czf - /var/lib/prometheus/data 2>/dev/null" \
     > "local-results/prometheus-${PREFIX}-$(date +%Y%m%dT%H%M%S).tar.gz" \
     || echo "prometheus snapshot failed"
   ```

   `HARNESS_PUB`/`SUT_PUB` come from the record
   (`.sut.public_ip`, `.harness.public_ip`).

3. **Destroy both VMs** (idempotent — missing VMs are reported as such,
   not errors):

   ```bash
   ./scripts/vm-provision.sh --destroy "$PREFIX"
   ```

4. **Clean local state:**
   - Delete `~/.ssh/config` aliases `oceanfs-sut` / `oceanfs-harness`
     (they now point at dead/recyclable IPs).
   - Delete the provisioning record (`.hetzner/provision-<prefix>.json`)
     so vm-status reports the clean `no_record` state instead of a stale
     topology.
   - Leave `local-results/` untouched — it is the user's archive.

## Returns

```json
{
  "destroyed": true,
  "prefix": "oceanfs-loadtest-2",
  "sut":     { "name": "oceanfs-loadtest-2-sut",     "destroyed": true },
  "harness": { "name": "oceanfs-loadtest-2-harness", "destroyed": true },
  "preserved_data": true,
  "preserved_paths": ["local-results/2_load_sustained_20260816T100000.json", "local-results/prometheus-oceanfs-loadtest-2-20260816T100500.tar.gz"],
  "record_deleted": ".hetzner/provision-oceanfs-loadtest-2.json"
}
```

## Notes

- The persistent laptop Prometheus (localhost:9091) already holds the
  federated metrics of every tunnel-up run, and `scripts/backup-observability.sh`
  archives that store (run-phase2.sh does it automatically after each remote
  run). `preserve-data` is the full-fidelity backstop for tunnel-less runs
  (SUT TSDB snapshot) plus the structured LoadReport JSONs.
- The TTL timer normally poweroffs the VMs first; vm-down is the full
  cleanup (delete). Destroyed VMs cannot be recovered — hence the
  `preserve-data` step for anything that matters.
- If `hcloud` reports the VMs already gone, still clean the local state
  and return `destroyed: true`.
