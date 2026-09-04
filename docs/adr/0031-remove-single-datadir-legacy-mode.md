# ADR-0031: Mandatory Storage Pools — Remove the Single-Data-Dir Legacy Mode

**Status:** Accepted
**Date:** 2026-09-04
**Deciders:** Stakeholder (review author), OceanFS architecture

---

## Context

ADR-0029 §D8 shipped a **zero-config fallback**: when `[storage.pools]` is
empty, the node boots an implicit single data pool at `data_dir` and every
role (metadata, WAL, event-WAL, hints, segments) resolves byte-for-byte to
today's single `{data_dir}/{role}` layout. This was justified as a
migration convenience for an unpublished system ("no pools = today's
behavior").

The 2026-08-25/09-03 whole-project review rejects that premise. The
reviewer's position, repeated across the codebase:

- `segment_store_impl.rs:16` — "no legacy mode"
- `pool_paths.rs:44` — "we need to get rid of the legacy mode"
- `node.rs:830-832` — a node without a data pool should not be permitted to
  start; a silent fallback to an arcane legacy mode is a big-bark pattern
- `gc/garbage_collector.rs:613-619` — legacy support must go; the duplicated
  `DiskSegmentShardStore`/`DiskSegmentStore` pair is a symptom
- `pool/mod.rs:783-806` — the implicit-pool fallback contradicts "no legacy"

The legacy branches are not free. They double every store's construction
surface (`legacy_dir` + `pool_id == 0` sentinel handling), force the
`pool_id`-less on-disk record shape to stay byte-compatible, and make every
path resolution conditional (`pinned_root(...).unwrap_or(data_dir.join(...))`).
They also keep `membership_state.toml` and the leave handler pinned to the
single `data_dir` model.

**Constraints:**

- OceanFS is **not in production**; there are no customers on the single
  `data_dir` layout and no on-disk data that must keep loading. The
  reviewer's rule is explicit: *we do not version, we refactor.*
- `pool_id: u32` starts at 0, so **pool 0 is a real, first-configured
  pool**. "Legacy" is *not* `pool_id == 0`; legacy is *the absence of
  pools*. This ADR must not corrupt that distinction.
- Event-WAL / checkpoint records may already exist on disk with the
  no-pool-id (48-byte) shape. Those files encode `pool_id = 0` on decode.
  Since there is no production data, the correct move is to make that shape
  *unreachable* (refuse old dirs), not to keep decoding it forever.

## Decision

**Storage pools become mandatory. The single-`data_dir` legacy mode is
removed.**

### D1. Pools required at boot

- `PoolRegistry::from_config` with an empty `[storage.pools]` **fails
  startup with an explicit error** listing the required roles (at minimum
  one `data`, one `wal`, one `metadata`, one `hints` pool; the exact
  requirement follows the role-pinning rules of ADR-0029 §D8).
- The implicit-pool fallback in `PoolRegistry::from_config`
  (`pool/mod.rs:783-806`) and the `StorageConfig::validate` empty-list
  acceptance are deleted.
- `Node::start` no longer branches on `config.storage.pools.is_empty()`
  (`node.rs:847-848`, `881-885`); `data_pools` is always the registry's
  pool list.

### D2. Legacy branches deleted from the data-access layer

- `DiskSegmentStore` and `DiskSegmentShardStore` lose the `legacy_dir`
  field and the "empty `data_pools` = legacy mode" resolution branch
  (`segment_store_impl.rs:28-34, 55-62`; `garbage_collector.rs:627-633`).
  Both keep `pool_id_for` resolution — `pool_id` always maps to a real,
  registered pool.
- `pool_paths.rs` loses the legacy fallback arms (`resolve_pinned(...)
  .unwrap_or(data_dir.join(...))`, the `hint_wal_dir` override, the
  Degraded-pool → legacy bridge). Role-pinned dirs resolve from pools only.
- `NodeLeaveHandler`'s single-`segment_dir` listing is superseded by the
  pool-aware replica model (see the roadmap item for review #34); the
  handler's `read_segment_data` fixed-76-byte slice (review #35) dies with
  it.

### D3. On-disk format stance

- The `pool_id`-less (no-flag) Seal record shape in the event WAL
  (`event_wal.rs:316-343`) and the v2 legacy checkpoint decode
  (`event_checkpoint.rs:500-516`) are **removed**.
- A node that boots onto a directory containing pre-pool event-WAL /
  checkpoint files fails startup with an explicit "unsupported pre-pool
  data directory" error. No silent migration, no continued decode.
- `pool_id = 0` remains a **valid pool id** (the first configured pool);
  nothing in the format changes for records written by a pools-enabled
  node.

### D4. Test rework

- `pool_paths` legacy-contract tests (`legacy_mode_resolves_exactly_as_before`,
  `degraded_pinned_pool_falls_back_to_legacy_path`) are replaced by
  no-pools-boot-refusal tests and pool-only resolution tests.
- `event_wal`'s legacy-record decode test is deleted; a new test asserts
  boot refusal on a pre-pool directory.
- Integration/e2e configs that omit `[storage.pools]` gain a minimal pool
  block (the deploy scripts already write pools; only dev/test configs need
  the addition).

### Out of scope

- **Pool health persistence across restart** (review #80) — separate ADR.
- **Membership-state relocation** off `data_dir` (review #42) — separate
  item; this ADR only stops *adding* single-dir assumptions.
- Any change to the `pool_id` numbering scheme.

## Consequences

### Positive

- Deletes an entire class of conditional code: every store, path resolver,
  and reader drops its "empty = legacy" branch. This is the prerequisite for
  the single-store unification (Theme 1 of the review triage).
- Kills the duplicated `DiskSegmentStore`/`DiskSegmentShardStore` legacy
  half — the first concrete step toward one data-access abstraction.
- Forces every node to declare its disk topology, which is the pre-condition
  for pool-aware placement, routing, and healing to be exercised in every
  test/e2e run instead of only in pool-mode configs.
- Removes the silent-degradation boot path the reviewer called a
  "big-bark pattern."

### Negative

- Dev/test/e2e configs must now declare pools (a small one-time fixture
  change; the load-test and fleet deploy scripts already write pools).
- Boot on an old pre-pool data dir refuses instead of working — acceptable
  by the "not in production, we refactor" rule, but it must be a clear error
  message, not a confusing panic.
- Touches `oceanfs-core` (config validation), `oceanfs-storage`
  (`pool/mod.rs`, stores), `oceanfs-durability` (stores, reaper),
  `oceanfs-node` (pool_paths, node.rs), and test fixtures across crates.

### Neutral

- Wire format for pools-enabled nodes is unchanged (pool 0 records still
  decode exactly as before).
- `[storage.pools]` schema itself is unchanged; only the "no pools allowed"
  validation is added.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Keep zero-config fallback** (status quo) | No config churn; dev ergonomics | Every store/path keeps conditional legacy branches; the fallback path is never exercised by pool tests; directly contradicts the review's "no legacy" mandate | Rejected: legacy mode is the complexity the review is removing |
| **Keep fallback but quarantine it** (feature-gated, deprecated with WARN) | Preserves dev convenience | Still compiles and maintains the full legacy path; feature flags multiply the conditional surface; a deprecated path nobody uses is dead weight | Rejected: quarantine is still maintenance; the reviewer's rule is removal |
| **Keep `pool_id = 0` as a "legacy sentinel"** (pool 0 = implicit data_dir) | Minimal format change | Conflates "first real pool" with "legacy root"; corrupts the pool model the whole disk-resilience epic is built on | Rejected: ADR-0029 pools start at 0; the sentinel would poison every pool-aware decision |

## References

- ADR-0029 §D8 (storage pools — the zero-config fallback this ADR amends)
- Review comments: `node.rs:830`, `pool/mod.rs:783`, `segment_store_impl.rs:16`,
  `pool_paths.rs:44`, `gc/garbage_collector.rs:613`
- Review triage roadmap: `docs/features/refactoring/review-2026-09-roadmap.md`
  (Theme 1, wave 2)
