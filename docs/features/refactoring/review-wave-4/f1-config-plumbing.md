---
feature: "f1: Config Plumbing — Thread User Config Instead of Defaults"
epic: "refactoring/review-wave-4"
status: proposed
priority: medium
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: Config Plumbing — Thread User Config Instead of Defaults

## Summary

The composition root constructs many subsystems from
`XxxConfig::default()` even though `NodeConfig` already carries the
corresponding user values (reviews `node.rs:615,625,715,732,737,777,784,
865,887,989,1073,1211,1260,1373,1382,3141`, `pool/health.rs:647`). This
feature threads the real values through. Pure plumbing — no semantics
change beyond "what the operator set now takes effect."

## Scope

### In Scope
- **Metadata store config** (`node.rs:619`): use a `metadata` config
  section instead of `..Default::default()`.
- **Acceleration config** (`node.rs:629`): thread `config.accel` /
  `AccelConfig` from user config.
- **RPC config** (`node.rs:719`): `RpcConfig::default()` → user RPC
  config (quickack/busy-poll/TLS).
- **Segment size tiers** (`node.rs:736`): `SegmentSizeConfig` from
  `NodeConfig`.
- **WAL config** (`node.rs:740`): keep `data_dir` override, thread the
  rest from config.
- **Pool config** (`node.rs:783`): `PoolConfig` from `NodeConfig`.
- **Seal config** (`node.rs:869`): thread `seal_timeout_ms`, per-pool
  write/read mode where the FS nature differs (review #49), no magic
  `5000`.
- **Segment replicator** (`node.rs:1012`): `ReplicationConfig` from
  `NodeConfig`.
- **Reconciliation** (`node.rs:1089`): `ReconcileConfig::default()` →
  user config.
- **Scrub** (`node.rs:1263`): complete config from `NodeConfig`
  (interval, parallel nodes, + remaining fields).
- **Heal / Merkle / GC / reaper / anti-entropy**: replace any remaining
  `::default()` with the config values that exist.
- **Health monitor tick** (`pool/health.rs:647`): per-pool
  `health_config` already exists; expose monitor `tick_interval` in
  `NodeConfig` (review #79).
- **Shutdown grace** (`node.rs:3146,3185,3190,3195`): replace hard-coded
  `10s`/`5s` with a configurable `shutdown_grace` (review #71).
- Add/verify `NodeConfig` fields for anything currently only available as
  a constant.

### Out of Scope
- New config schema beyond what `NodeConfig`/subsystem configs already
  define (add fields only where a knob is genuinely absent).
- Default-value tuning (only make defaults *overridable*).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | Add missing `NodeConfig` fields where required |
| `oceanfs-node` | Replace `::default()` constructions with config values |

## Definition of Done

- [ ] Every site listed above reads from user config.
- [ ] A config smoke test: set a non-default value for each knob and
      assert the subsystem observes it.
- [ ] Existing default behavior unchanged (defaults still default).
- [ ] Node tests + e2e green.
