# f1 Live Smoke Evidence — 2026-09-11

Cloud lifecycle exercised end-to-end on Hetzner (fsn1), then fully destroyed
(**zero servers and zero volumes left**; `hcloud volume list` empty). Public
IPs are redacted; the `--volume-pools` record, gate artifact, and destroy
logs are the primary evidence.

## 1. Volume-backed provisioning (`--phase 2 --single-vm --volume-pools`)

`--single-vm` is the 1-node topology used for the smoke (one CX33 SUT VM).
Quota preflight: `1 node(s) x 230 GB = 230 GB (cap 1024 GB)`.

Record (`.hetzner/provision-oceanfs-f1v.json`), 5 volumes:

| role | id | device | mount | size_gb |
|---|---|---|---|---|
| data | 106850154 | `/dev/sdb` | `/mnt/oceanfs-data` | 120 |
| wal | 106850156 | `/dev/sdc` | `/mnt/oceanfs-wal` | 20 |
| meta | 106850167 | `/dev/sdd` | `/mnt/oceanfs-meta` | 20 |
| hints | 106850174 | `/dev/sde` | `/mnt/oceanfs-hints` | 10 |
| spare | 106850182 | `/dev/sdf` | `/mnt/oceanfs-spare` | 60 |

Each volume was discovered by stable id (`/dev/disk/by-id/scsi-0HC_Volume_<id>`),
formatted ext4, mounted with `noatime`, and given a UUID-based `nofail` fstab
entry (verified: `mount /mnt/oceanfs-hints` remounted after `umount`).

## 2. `--status` volume inventory + drift

All five recorded volumes: `present: true`, `attached: true`,
`drift: false`. After deleting the spare volume out-of-band
(`hcloud volume detach` + `delete`), `--status` reported
`drift: true` with the spare at `present: false` while data stayed
`present: true` — drift detection in both directions.

## 3. Deploy with `--pools-on-mounts`

`./scripts/sut-deploy.sh --sut root@<sut> --pools-on-mounts --binary oceanfs`
→ node healthy. Config roots (on the node):

```
[[storage.pools]] name = "data-0"  role = "data"      root = "/mnt/oceanfs-data"
[[storage.pools]] name = "wal-0"   role = "wal"       root = "/mnt/oceanfs-wal"
[[storage.pools]] name = "meta-0"  role = "metadata"  root = "/mnt/oceanfs-meta"
[[storage.pools]] name = "hints-0" role = "hints"     root = "/mnt/oceanfs-hints"
```

`GET /admin/pools`: all four pools `status: healthy`.

Smoke write/read on the volume-backed pools:
`bucket PUT: 200`, `object PUT: 200`, `object GET: 200` + body returned.

## 4. Pre-flight negative case

`umount /mnt/oceanfs-hints` (hints not a mountpoint) → deploy aborts with:

```
assert_volume_mount: /mnt/oceanfs-hints is not a mountpoint (volume not attached/mounted?)
[ERROR] Volume mount pre-flight failed: /mnt/oceanfs-hints ... is not a writable block-device mount (>= 4G) disjoint from /var/lib/oceanfs.
```

exit 1, before any binary copy or service restart. After
`mount /mnt/oceanfs-hints`, the redeploy succeeded and all four pools stayed
healthy.

## 5. Destroy

`./scripts/vm-provision.sh --destroy oceanfs-f1v` deleted all five recorded
volumes (including the already-absent spare, reported idempotently) and the
server. A second destroy of the non-volume `oceanfs-f1` prefix exercised the
**prefix-scan fallback**: with no recorded volumes, the gate volume
`oceanfs-f1-sut-vol-data` (id 106850104, attached to the VM being destroyed)
was found by name-prefix and deleted after detach.

Final account state: no servers, no volumes.

## 6. Sysfs gate (separate artifact)

`artifacts/sysfs-gate-2026-09-11.json` — **PASS 11/11** on a real attached,
mounted volume. Note the rescan re-registered the device under a **different
letter** (`/dev/sdb` → `/dev/sdc`) with the same serial; stable identity via
`/dev/disk/by-id/scsi-0HC_Volume_<id>` is therefore mandatory, and f2's
injectors must resolve devices by id, never by letter.
