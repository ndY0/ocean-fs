---
epic: "disk-resilience"
status: done
priority: high
created: 2026-08-22
updated: 2026-08-22
---

# Disk Resilience — Epic Plan

Epic: `disk-resilience`
ADR: [ADR-0029](../adr/0029-storage-pools-disk-resilience.md)
Brainstorm: [disk-resilience-pools](../../brainstorm/disk-resilience-pools.md)

## Goal

Give OceanFS a disk abstraction — **storage pools** — so a node can span
multiple disks, isolate WAL/metadata from segment I/O, survive a disk failure
surgically (re-replicate 1/(N×disks) instead of evicting a node), and route
reads/writes around degraded storage. Membership (SWIM, ADR-0028) stays
node-granular; ownership, placement, and loss accounting move to pool
granularity in the data plane, with a versioned per-node manifest gossiped as
a compact attribute.

This epic is **Phase A only** (foundation): topology config, pool runtime,
placement, role isolation, multi-data-pool segments, manifest gossip, cached
routing state, and runtime pool attach. Failure detection (Phase B) and scale
ops (Phase C) are separate epics that build on this foundation.

## Feature DAG

```
f1 pool-config
 └── f2 pool-runtime
      ├── f3 placement-policy
      ├── f4 role-isolation
      └── f6 manifest-gossip
 f3 ──→ f5 data-pool-placement
 f5 + f6 ──→ f7 routing-cache
 f2 + f6 ──→ f8 runtime-attach
```

Implementation order: **f1 → f2 → f3 → f4 → f5 → f6 → f7 → f8**. After f2,
f3/f4/f6 are independent and can land in any order; f5 needs f3; f7 needs
f5+f6; f8 needs f2+f6.

| # | Feature | Touches | Depends on |
|---|---|---|---|
| f1 | `pool-config` — pool types, validation, zero-config fallback | core | — |
| f2 | `pool-runtime` — StoragePool, PoolRegistry, startup probing, metrics | storage, core | f1 |
| f3 | `placement-policy` — role/weight/least-free pool selection | storage | f2 |
| f4 | `role-isolation` — metadata/WAL/event-wal/hints pinned to role pools | node, storage | f2 |
| f5 | `data-pool-placement` — multi-root segment store, segment→pool mapping | storage, durability, node | f3 |
| f6 | `manifest-gossip` — NodeManifest attribute on the membership plane | membership, node, proto | f2 |
| f7 | `routing-cache` — per-peer manifest cache, read/write preference | node | f5, f6 |
| f8 | `runtime-attach` — admin API adds a pool without restart | node, storage | f2, f6 |

## Phase-A scope decisions (from the brainstorm)

- One pool = one root = one failure domain (multi-root pools rejected at
  validation, ADR-0029 §D8).
- Roles: `data | wal | metadata | hints`; at most one `wal`, `metadata`,
  `hints` pool each; any number of `data` pools.
- Zero-config fallback: no `[storage.pools]` = today's single `data_dir`
  behavior, byte-for-byte (legacy mode). Migration is explicit, never
  automatic.
- The segment→pool mapping is persisted in the existing segments CF
  (`pool_id` column, default 0 = legacy root) so restart rebuilds it and
  legacy rows keep working.
- `PoolTech::Auto` (default) resolves to a placeholder in Phase A; real
  auto-detection lands in Phase B with the health monitor (where tech
  actually matters).
- Phase A does NOT implement: health monitoring/status transitions (all
  pools are Healthy), loss announcements, reconciliation, re-replication,
  `write_degraded` semantics (the field exists on the manifest, always
  false), drain/rebalance. The structures are built so Phase B slots in
  without redesign.

## Acceptance bar (epic DoD)

- [x] ADR-0029 D1, D2, D8 implemented for Phase A: pool-granular ownership
      under node-granular membership; versioned `NodeManifest`/
      `PoolManifest` gossip attribute; topology config with one-root-per-pool
      rule and runtime attach (f1/f2/f6/f8).
- [x] A node boots with a 4-pool topology (data×2, wal, metadata, hints):
      metadata store opens at the metadata pool root, WAL at the wal pool
      root, event-wal at the wal pool root, hint WAL at the hints pool root,
      segments spread across the data pools (f4 `role_isolation` e2e +
      f5 `data_pool_placement` e2e).
- [x] Zero-config fallback: a node with no `[storage.pools]` behaves exactly
      as before (all e2e suites green, legacy mode — `legacy_node_roundtrip`
      + the full node suite).
- [x] Placement spreads sealed segments across data pools (weight-aware,
      least-free-capacity); the segment→pool mapping survives restart
      (checkpoint v3 + event-WAL fold tests); GC unlinks from the owning
      root (f5 e2e + shard-store tests).
- [x] The NodeManifest propagates through gossip; peers can read it from the
      cached routing state; the manifest re-declares on restart (incarnation
      tie-in) (f6 3-node convergence + f7 flip-propagation integration
      tests).
- [x] `POST /admin/pools` attaches a new data pool at runtime without
      restarting the node; placement starts filling it (f8
      `runtime_attach` e2e: 4→5 manifest, sealed segments on both roots,
      GET round-trip, no restart).
- [x] All existing unit + integration suites stay green (regression gate);
      clippy/fmt/rustdoc clean across affected crates. Final sweep:
      storage 12/12 binaries, node 22/22 binaries, server 226 lib
      (one pre-existing unrelated failure in grpc_services), routing/core/
      network/membership green — all `--test-threads=1`; clippy
      `-D warnings` ×4 clean; rustdoc clean on the f8 crates; fmt clean.
