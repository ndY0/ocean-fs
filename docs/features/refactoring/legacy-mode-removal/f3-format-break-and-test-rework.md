---
feature: "f3: On-Disk Format Break & Test/Fixture Rework"
epic: "refactoring/legacy-mode-removal"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: legacy-mode-removal/f1-boot-enforcement
    reason: Pre-pool boot refusal and fixture rework describe a pools-only world; the fixture-prep commit must merge with f1 (README landing order)
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0029-storage-pools-disk-resilience
perf:
  - "6.3: on-disk record layout stays byte-explicit after the format break"
created: 2026-09-04
updated: 2026-09-04
---

# f3: On-Disk Format Break & Test/Fixture Rework

## Summary

Apply ADR-0031 D3/D4. Remove the `pool_id`-less (no-flag) Seal record shape
from the segment event WAL and the legacy v2 checkpoint decode, so a node
booting onto a directory with **pre-pool** event-WAL/checkpoint files fails
startup with an explicit "unsupported pre-pool data directory" error — no
silent migration, no continued decode. Critically, pools-enabled nodes keep
working unchanged: the Seal encoder now always writes the pool id (value 0
included), so `pool_id = 0` — the first configured pool — still round-trips
byte-exact. Delete/replace the tests that pin legacy behavior and update every
dev/test/e2e node config to declare a minimal pool topology.

## Scope

### In Scope

**A. Event-WAL format (ADR-0031 D3)** — `oceanfs-storage`
`segment/event_wal.rs`:

- Encoder `SegmentEvent::to_record_bytes` Seal arm (`:315-343`): **always**
  set `SEAL_FLAG_POOL_ID` and always append the 4 pool-id bytes — drop the
  `if evt.pool_id != 0` conditionals (`:321`, `:330-332`, `:340-342`). A
  `pool_id = 0` Seal now encodes with the flag set (52-byte payload) instead
  of the pre-pool 48-byte shape.
- Decoder `decode_payload` (`:452-...`): delete the two no-pool decode arms —
  the 48-byte no-flag arm (`:463-479`) and the repacked-from-without-pool arm
  (`:499-517`). Keep the arms that require `SEAL_FLAG_POOL_ID` (`:480-498`,
  `:518-...`); they already decode `pool_id = 0` correctly.
- Update the framing doc (`:24-53`), the `SEAL_FLAG_POOL_ID`/`SEAL_POOL_ID_SIZE`
  docs (`:94-102`), and the `SealEvent.pool_id` doc (`:219-224`): "0 = legacy
  root" becomes "0 = the first configured pool".
- `SealEvent` sizes change for pool-0 records: fix the test-size constants
  (`record_sizes_match_the_framing_doc` `:1603`, helper `seal_event` /
  `seal_event_with_repacked` `:1505-1516`, `record_roundtrip_is_byte_exact`
  `:1538`, `seal_event_pool_id_roundtrips` `:1549`, and the
  repacked variants) to the always-pool-id layout.

**B. Boot refusal on pre-pool directories** (ADR-0031 D3):

- Add a dedicated `Error` variant in `oceanfs-storage` `error.rs` (e.g.
  `UnsupportedPrePoolDataDir { detail }`, message starting "unsupported
  pre-pool data directory …"); update any exhaustive matches on `Error`.
- Classify, rather than generic-corrupt, the two legacy shapes that can now
  appear on disk from a pre-pool node:
  - Event-WAL: a CRC-valid record with `kind == KIND_SEAL`, payload length 48
    or `48 + SEAL_REPACKED_FROM_SIZE`, and no pool flag is *legacy*, not
    corruption. `EventWalReader` (`event_wal.rs:1350-1358`) maps that shape to
    the new error (recommended seam: a private two-tier decode in
    `decode_payload` so the public `from_record_bytes` still returns `None`
    while the boot reader stays explicit).
  - Checkpoint: `decode_snapshot` (`event_checkpoint.rs:469-471, 500-516`)
    currently accepts `LEGACY_CHECKPOINT_VERSION` (v2, `:89`). Delete the v2
    acceptance and the `LegacySegmentMetadata` struct (`:442-451`). A
    checkpoint whose version byte is 2 must surface the new error from
    `EventCheckpoint::load_checkpoint` (`:246-277`) **instead of** the
    fall-to-older-snapshot path — a legacy directory must refuse boot, not
    silently start from an older (equally legacy) snapshot or from scratch.
- The error text reaches the operator via node.rs's existing mappings:
  `failed to load event WAL checkpoint: {e}` (`node.rs:1686-1687`) and
  `event-WAL recovery failed: {e}` (`node.rs:1707`).

**C. Test rework (ADR-0031 D4)**:

- Delete `legacy_seal_record_decodes_pool_id_zero` (`event_wal.rs:1585-1601`);
  rework `unmarked_seal_records_still_decode_with_the_marker_absent`
  (`:1761-1790`) into "a no-flag 48-byte Seal record is refused as an
  unsupported pre-pool record" (it crafts exactly the legacy shape).
- Delete `legacy_v2_checkpoint_decodes_with_pool_id_zero`
  (`event_checkpoint.rs:649-690`); add "a v2 checkpoint fails load_checkpoint
  with the pre-pool error".
- Add: a `SealEvent { pool_id: 0 }` round-trips byte-exact through the new
  always-flagged shape; a CRC-valid no-flag Seal record and a v2 checkpoint
  each fail boot with the explicit message.
- Node-level integration test: seed a wal-pool `event-wal` directory with a
  crafted legacy record/checkpoint and assert `Node::start` refuses with the
  pre-pool error (put in `crates/oceanfs-node/tests/`, run
  `--test-threads=1`).

**D. Config fixtures declare minimal pools (ADR-0031 D4)** — the fixture-prep
commit that merges with f1:

- A canonical minimal topology for tests/e2e: one `data`, one `wal`, one
  `metadata`, one `hints` pool on sibling tempdir roots, `Fatal`
  missing-root policy (the amended ADR-0029 §D8 shape used by f1).
- Update:
  - `crates/oceanfs-node/src/node.rs` unit-test `test_config` (`:3600`) and
    the `Node::start` doc examples (`:2800-3070`);
  - node integration tests that boot with `..NodeConfig::default()` and no
    pools (representative: `node_lifecycle.rs`, `e2e_single_node.rs`,
    `anti_entropy.rs`, `scrub_cycle.rs`, `read_write_roundtrip.rs`,
    `gc_compaction.rs`, `merkle_startup_rebuild.rs`, `orphan_reaper.rs`,
    `wal_recovery.rs` — grep `tests/` for `NodeConfig {` builders);
  - `e2e/src/harness.rs` `spawn_inner` (`:441-467`): append the minimal
    `[storage.pools]` block to the composed config when the caller's TOML has
    no `[storage]` section — this covers the ~30 `e2e/tests/*.rs` inline
    configs in one place;
  - remaining doc examples across crates that build
    `StorageConfig::default()` or data-only configs (`oceanfs-node`
    `pool_manifest.rs:37,170`, `oceanfs-storage` `io/segment_reader.rs:252`,
    `pool/placement.rs`, `pool/health.rs`, `oceanfs-server` `admin.rs`);
  - the ADR-0029 §D8 example inside `oceanfs-core` config tests to include a
    `hints` pool (see f1).
  - Deploy scripts (`.hetzner/`, e2e fleet/load harness configs) already write
    pools — **do not touch**.

### Out of Scope

- Any decode compatibility for pre-pool data — ADR-0031 explicitly refuses it.
- `Reserve`/`Delete`/`MetadataRefresh` record shapes — unchanged (they never
  carried a pool id).
- The pool-id carrying decode arms — these are the live format and must be
  preserved (roadmap wave-0 note on `event_wal.rs:1579`).
- Reader/sealer legacy fields (theme-1) and `NodeLeaveHandler` (c1).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/event_wal.rs` (encoder always writes pool id; no-flag decode arms removed; docs/constants updated), `segment/event_checkpoint.rs` (v2 decode removed; `LegacySegmentMetadata` deleted), `error.rs` (new pre-pool error variant), tests reworked |
| `oceanfs-node` | Boot-refusal integration test; unit-test config builder + doc examples declare pools |
| `e2e` | `harness.rs` default pool block; no per-file test edits required |
| `oceanfs-core` / `oceanfs-server` | Doc-example and test fixture config updates |

## Interface (Public API)

- `SegmentEvent::to_record_bytes` / `from_record_bytes` — **wire-format
  change**: pool-0 Seal records gain the pool-id flag + 4 bytes (52-byte
  payload). Public signatures unchanged; on-disk layout for pools-enabled
  nodes with `pool_id != 0` is unchanged, and `pool_id == 0` now writes the
  id explicitly (no production data to preserve — ADR-0031 "we refactor").
- `oceanfs_storage::Error` — new variant for unsupported pre-pool data dirs.
- `EventCheckpoint::load_checkpoint` — a v2 checkpoint now errors instead of
  decoding.
- No change to `StorageConfig`, `PoolConfig`, or `PoolRole`.

## Data Flow

```
Boot, wal pool root contains pre-pool files
  → EventWal::open / EventCheckpoint::load_checkpoint
  → no-flag Seal record OR version-2 checkpoint detected
  → Error::UnsupportedPrePoolDataDir("… pre-pool event-WAL/checkpoint …")
  → Node::start aborts: "failed to load event WAL checkpoint: unsupported
    pre-pool data directory …"      [explicit refusal, no migration]

Pools-enabled node, pool_id = 0 (first data pool)
  → Seal encoded with SEAL_FLAG_POOL_ID + pool_id(0)   [52-byte payload]
  → decodes back to pool_id = 0                        [byte-exact]
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes; `grep -rn
      "LEGACY_CHECKPOINT_VERSION" crates --include=*.rs` returns nothing;
      `grep -rn "SEAL_FLAG_POOL_ID" crates/oceanfs-storage/src/segment/event_wal.rs`
      shows the flag is always set in the Seal encoder.
- [ ] **Tests:** pool-0 Seal round-trips byte-exact; a crafted no-flag Seal
      record and a crafted v2 checkpoint each fail with the pre-pool error;
      `Node::start` refuses a pre-seeded legacy `event-wal` dir. Deleted:
      `legacy_seal_record_decodes_pool_id_zero`,
      `legacy_v2_checkpoint_decodes_with_pool_id_zero`, and the legacy arms of
      `unmarked_seal_records_still_decode_with_the_marker_absent`. Run
      `cargo test -p oceanfs-storage --lib -- --test-threads=1`, `cargo test
      -p oceanfs-node --lib -- --test-threads=1` (RocksDB caveat, PIPELINE.md
      §4.6), `cargo test -p oceanfs-node --test e2e_single_node --
      --test-threads=1`, `cargo test -p oceanfs-core`.
- [ ] **Docs:** `#![deny(missing_docs)]` passes; the event-WAL framing doc and
      `SealEvent.pool_id` doc describe the always-flagged pool-id layout.
- [ ] **ADR:** ADR-0031 D3/D4 satisfied — no no-flag Seal shape, no v2
      checkpoint decode, explicit pre-pool boot refusal, `pool_id = 0`
      preserved for pools-enabled nodes, legacy tests gone.
- [ ] **Perf:** layout remains byte-explicit and fixed-size per variant
      (perf 6.3); the added pre-pool classification is boot-time only, not on
      the append path.
- [ ] **Integration:** a fresh pools-enabled node writes, seals, restarts, and
      reads back (pool 0 included); boot over a pre-pool directory refuses
      with the explicit message.
