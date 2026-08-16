---
name: vm-logs
description: "Fetch OceanFS service logs from the load-test SUT VM (journald). Use when the user asks for the SUT's oceanfs logs, wants to debug a failed load test run, or check for errors/panics during or after a phase. Triggers: \"vm-logs\", \"get the logs\", \"SUT logs\", \"journald\", \"why did the test fail\", \"show me the errors\"."
---

# vm-logs

Fetch journald logs for the `oceanfs` systemd unit on the **SUT VM**.
The SUT runs exactly one oceanfs process under systemd (unit `oceanfs`,
deployed by `sut-deploy.sh`/`setup-harness.sh`), so logs are a single
unit — no multi-node correlation needed (that is Phase 3+).

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `since` | journalctl time window (`"10 min ago"`, `"1 hour ago"`, ISO timestamp) | `"10 min ago"` |
| `level` | `error` filters to ERROR/FATAL/PANIC lines only | all levels |
| `lines` | Maximum number of lines to return | 200 |
| `service` | systemd unit name | `oceanfs` |

## Procedure

1. **Resolve the SUT** — from the provisioning record (or the
   `oceanfs-sut` ssh alias):

   ```bash
   SUT_PUB=$(jq -r '.sut.public_ip' "$(ls -t .hetzner/provision-*.json | head -1)")
   ```

2. **Fetch the logs**:

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${SUT_PUB}" \
     "journalctl -u ${SERVICE:-oceanfs} --since '${SINCE:-10 min ago}' --no-pager -n ${LINES:-200}"
   ```

   With `--level error`:

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${SUT_PUB}" \
     "journalctl -u ${SERVICE:-oceanfs} --since '${SINCE:-10 min ago}' --no-pager -n ${LINES:-200} | grep -iE 'error|fatal|panic'"
   ```

3. **Return** the lines as `[{timestamp, message}, ...]` (parse the
   journald `Mmm dd HH:MM:SS host unit[pid]: msg` prefix), or return the
   raw tail when parsing is ambiguous. Always include the count of
   matching lines.

## Returns

```json
{
  "service": "oceanfs",
  "sut": "10.0.0.5 (public 1.2.3.4)",
  "since": "10 min ago",
  "lines": 3,
  "logs": [
    { "timestamp": "Aug 16 09:58:12", "message": "segment sealed id=42 shards=8/2" },
    { "timestamp": "Aug 16 09:59:01", "message": "gc cycle completed: 12 tombstones swept" },
    { "timestamp": "Aug 16 10:00:00", "message": "wal replay: 0 segments recovered" }
  ]
}
```

With `--level error` and nothing found: `{ "lines": 0, "logs": [] }` —
a clean signal worth stating explicitly (no errors in the window).

## Notes

- The unit is `Restart=no` by design: after the harness's crash-recovery
  SIGKILL the process stays down until `systemctl restart`. If logs show
  the service down after a run, that is the crash phase — not a failure.
- For service-state questions use **vm-status** (`oceanfs: active`).
- For process-level metric evidence use **vm-metrics**.
