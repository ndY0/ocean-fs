# Cluster-churn debug handoff — get `load_cluster_churn` green first

**Date:** 2026-09-09
**Owner of record:** implementer session (handing to next session)
**Priority:** this test must be clean before proceeding on anything else
(per stakeholder).

## Goal

Make `e2e/tests/load_cluster_churn.rs` pass cleanly in **both** run modes:
- **Local quick** (`./scripts/run-phase3.sh --quick` — 3-node spawn on one
  machine, child-process kill/restart)
- **Fleet quick** (`./scripts/run-phase3.sh --harness … --quick --nodes
  10.0.0.2,10.0.0.3,10.0.0.4 --ssh root@10.0.0.2,…`)

## What is already fixed (do NOT revert)

`hlc_monotonic` was failing deterministically with
`node oceanfs-node-0: incarnation 4 -> 3`. Root cause was a **test
artifact**, not a product bug: the poller records one `/admin/cluster` view
per node per round and the old assertion compared all views on ONE timeline.
Right after a restart, the restarted node's self view already shows the new
incarnation while a lagging peer still reports the old one (gossip skew,
resolved within a gossip round). Fixed by checking monotonicity **per
observer node** (`by_observer` on `view.node_index`).

- Commit: `94d31c7` `test(e2e): load_cluster_churn incarnation monotonicity per observer`
- Product behavior was correct: node-0 incarnation climbed 4→5→6→7 per
  observer; membership file / ADR-0022 write-through were fine.

## Remaining failures (evidence from this session)

Post-fix fleet run (`3_load_cluster_churn_20260909T211615.json`):
```
manifest integrity: 0 keys absent (of 108)
read quorum:        18 of 108 sampled keys failed quorum   ← FAILING
hlc monotonic:      None (fixed)
cache invalidation: 0/20 stale (passed this run)
churn: 14 events, 0 failed | handoff stored=14 delivered=14 | convergence/ring/no-split-brain all pass
errors_total 594 / ops 18499
```
Post-fix local run (`3_load_cluster_churn_20260909T212643.json`):
```
read quorum: 2 failures
cache invalidation: 20/20 stale          ← FAILING
handoff stored=14193 delivered=13058 (≈1k undelivered)
errors_total 18800 / ops 46158 (~40% errors — heavy single-VM contention)
```

### Failure classes observed (quorum diag from report `assertions[].actual`)

Per-node tuples are `(key, status, hash)` in node order 0,1,2:

1. **Genuine single-copy keys** — only ONE node serves the object, others
   404, e.g.:
   - `hot-26`: (404, 200, 404)
   - `hot-47`: (200, 404, 404)
   - `hot-57`, `hot-31`: same shape
   This is real under-replication after churn (writes should leave ≥2
   copies with W=2). Investigate write-quorum + hint-delivery when a ring
   owner is down, and whether anything ever backfills a missed replica.
2. **Version-mismatch keys** — ALL nodes serve the same body but it is not
   a *recorded* manifest version, so the verifier counts 0 correct nodes,
   e.g. `cold-4510`: (200, 200, 200) with identical hash yet failed. The
   cache-invalidation/hot overwrite path does direct `target.put(...)`
   WITHOUT `Manifest::record`, so the new body hash is unrecorded.
   Question: should `verify_read_quorum` compare against *any served
   version* instead of a recorded one, or should those writes be recorded?

## Where to dig (hints)

- `e2e/tests/load_cluster_churn.rs`
  - read-quorum verification ~line 739-760, final aggregate assert ~1124
  - cache invalidation section ~895-935 (writes v1 node0, v2 node1, GET
    node2; L1 TTL 0 in profile; "0 bytes" = empty/missing, not stale v1)
  - churn scheduler kill/restart semantics (remote: systemctl SIGKILL +
    restart; local: child processes)
  - `POLL_INTERVAL` ~10s
- `e2e/src/load/manifest.rs`
  - `verify_read_quorum` (~348) and `verify_one_from_node` — note the
    counting semantics: a node counts as correct when the served body
    matches a RECORDED version (`is_none()`); 404 and unrecorded-version
    bodies both count as incorrect.
  - `record`/`record_delete`/version-accumulation semantics (see tests at
    bottom of the file).
- `e2e/src/load/generator.rs`
  - hot/cold key-space behavior; direct PUT paths that bypass
    `Manifest::record` (cache-invalidation keys, hot overwrites).
- Product side (untouched by the pools work — treat as pre-existing):
  - metadata-row replication quorum + hinted-handoff under node-down
    (`crates/oceanfs-server/src/write/coordinator.rs`, hinted handoff in
    `crates/oceanfs-durability/src/hinted_handoff/`)
  - cache-invalidation across nodes (L2 invalidation gossip; `cache` crates)
  - why local mode sees ~40% errors (contention on a single 8-vCPU VM vs
    real timeouts; consider tuning load/concurrency or running CI-like
    settings).

## Reproduce

```bash
# Local (3-node spawn on one machine):
ssh oceanfs-harness-3 'cd /root/ocean-fs && ./scripts/run-phase3.sh --quick'

# Fleet:
./scripts/run-phase3.sh --harness root@62.238.59.231 --quick \
  --nodes 10.0.0.2,10.0.0.3,10.0.0.4 \
  --ssh root@10.0.0.2,root@10.0.0.3,root@10.0.0.4 --seed 42
```

- Reports: on the harness at `/tmp/oceanfs-reports/3_load_cluster_churn_*.json`;
  `scp` to `/tmp/oceanfs-reports/` for parsing. Key fields:
  `assertions[].actual` (quorum per-node tuples), `cluster_views` (proves
  the incarnation skew), `churn_events`, `worker_stats`.
- Settle window env: `LOAD_TEST_SETTLE_GRACE_MS` (default 30000).
- After any e2e change: commit + push, then on the harness
  `git fetch && git reset --hard origin/main` before re-running.

## Environment state (as of handoff)

- Fleet (phase 3) VMs are UP, cost ~€0.28/h, TTL ≈ 02:51 local:
  - sut-0 `65.109.172.57` (10.0.0.2), sut-1 `62.238.10.74` (10.0.0.3),
    sut-2 `204.168.157.243` (10.0.0.4); harness `62.238.59.231` (10.0.0.5)
  - ssh aliases: `oceanfs-sut-0/1/2`, `oceanfs-harness-3`
  - harness clone pinned at `94d31c7`
- Phase-2 VMs were destroyed by the user.
- `main` = `94d31c7`. All session work pushed (drain metrics, deploy pools,
  histogram renderer fix, churn monotonic fix).

## Unrelated small action item (do later, not blocking)

Dashboard panel "WAL Files / Truncations" conflates `wal_truncations_total`
(crash-recovery `truncate()` only) with WAL file reclamation, which happens
via file deletion at rotation (`cleanup_old_wal_files`, 1,292 cleanups
observed; file count bounded at `WAL_RETENTION_FILES=4`). Relabel the panel
or add a real cleanup counter.
