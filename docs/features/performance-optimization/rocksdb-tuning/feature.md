---
feature: "RocksDB Tuning for Blob Storage Workload"
epic: "performance-optimization"
status: proposed
priority: high
owner: ""
dependencies: []
adr:
  - 0001-segment-packing
  - 0009-storage-crate-split
perf:
  - "1.5 Zero-copy protobuf deserialization"
  - "11.1 Atomic counters on hot paths"
created: 2026-08-05
updated: 2026-08-05
---

# RocksDB Tuning for Blob Storage Workload

## Summary

Tune the RocksDB metadata store for OceanFS's specific workload: high
write volume on the `objects` CF (one PUT = one metadata write), large
sequential writes on the `segments` CF (one per segment seal), and
append-mostly on the `deletions` CF (tombstones). Current configuration
uses `Options::default()` with identical settings for all three column
families — missing bloom filters, per-CF write buffer sizing, block cache
configuration, compaction tuning, and metrics exposure. This feature adds
those settings, configured from `NodeConfig` where appropriate, with
commented rationale for each choice. The code changes live in
`oceanfs-storage/src/metadata/store.rs`.

## Scope

### In Scope

- **Bloom filter policy for the `objects` column family.** Configure 10
  bits per key (`set_bloom_filter(10.0, false)`) on the `objects` CF.
  A bloom filter reduces disk reads for point lookups (GET, HEAD) — without
  one, every key-not-found query must probe all SST files. With 10 bits/key,
  the false-positive rate is ~1%, eliminating 99% of unnecessary SST probes.
  This is the single highest-impact RocksDB configuration change for a
  metadata-heavy workload. Source: storage-IO H4(a).

- **Per-CF write buffer (memtable) sizes.** Set different write buffer sizes
  for each CF based on write volume:
  - `objects` CF: 64 MB (up from default 64 MB — keep default, but document
    that it should be configurable)
  - `segments` CF: 256 MB (large because segment metadata writes are big
    batches — one seal writes segment header, index, blob references)
  - `deletions` CF: 16 MB (low write volume, mostly tombstone records)
  Larger write buffers reduce write stalls on the write-heavy `segments` CF
  and decrease compaction frequency. Source: storage-IO H4(b), M6.

- **`max_open_files = -1` (unlimited).** RocksDB manages its own file cache;
  setting unlimited max open files lets it keep all SST file descriptors
  open, avoiding repeated open/close overhead. This is safe for OceanFS
  because the total number of SST files is bounded by the data size and
  compaction. Document the tradeoff: nodes with very large metadata stores
  may want to cap this at 4096 or derive from `ulimit`. Source: storage-IO
  H4(c).

- **Block cache size from `NodeConfig`.** Currently hardcoded (128 MB or
  RocksDB default). Add a `metadata_block_cache_mb: u64` field to `NodeConfig`
  (default: 512 MB) and use it in `rocksdb::BlockBasedOptions::set_block_cache()`.
  This is critical for metadata-heavy workloads where the block cache should
  be large enough to hold the hot subset of the objects index. Make this
  configurable per-node rather than assuming a one-size-fits-all value.
  Source: storage-IO RocksDB configuration audit table.

- **`compression_per_level` — zstd for bottom levels, Snappy for L0-L1.** Use
  `CompressionType::Zstd` for levels L2+ (cold data where compression ratio
  matters more) and `CompressionType::Snappy` for L0-L1 (hot data where
  decompression speed matters more). The current configuration uses `Zstd`
  for all levels, which adds unnecessary decompression latency for hot
  memtable flushes. Snappy is ~2-5× faster at decompression than Zstd for
  the same data. Source: storage-IO RocksDB configuration audit table.

- **RocksDB metrics exposure.** Wire RocksDB's internal properties to the
  `MetricsRegistry` (already being built by gap-closure Epic 2). Expose:
  - `rocksdb.block_cache.hit` / `rocksdb.block_cache.miss` (AtomicU64 gauges)
  - `rocksdb.compaction.pending_bytes` (gauge)
  - `rocksdb.memtable.size` per CF (gauge)
  - `rocksdb.num.running.compactions` / `flushes` (gauge)
  - `rocksdb.estimate.num.keys` per CF (gauge)
  Use `DB::property_value()` or `DB::property_int_value()` to query RocksDB
  properties periodically (every 30s via a background task or on `/admin/metrics`
  scrape). Source: storage-IO RocksDB configuration audit (metrics).

- **`optimize_level_style_compaction` call.** Call RocksDB's `optimize_level_style_compaction()`
  on the `objects` and `segments` CFs to tune the default level-style
  compaction for OceanFS's write pattern. Optionally, evaluate universal
  compaction for the `deletions` CF (append-mostly, tombstone-heavy) —
  document the tradeoff: universal compaction reduces write amplification
  but increases space amplification.

- **Documentation.** Every RocksDB option setting must have a comment
  explaining the rationale — workload characteristic, tradeoff, and
  configuration guidance for operators. This is particularly important for
  the block cache size and write buffer sizes, which operators may need to
  tune per workload.

### Out of Scope (for this feature)

- **Metric infrastructure (MetricsRegistry, Gauge, label support)** — handled
  by gap-closure Epic 2 (metrics-infrastructure). This feature only wires
  RocksDB properties into the (already-built) metrics system.
- **Protobuf metadata serialization** (storage-IO H5: `serde_json` → protobuf)
  — handled by gap-closure Epic 6 (codebase-hygiene). This feature configures
  RocksDB, not the serialization format.
- **Prefix extractor for LIST operations** (storage-IO M4) — separate
  optimization tracked in the storage I/O audit but not critical for the
  initial tuning pass. The manual `starts_with` check is correct, just slower
  than RocksDB-native prefix extraction.
- **Universal compaction evaluation** — documented but not implemented in
  this feature. If benchmarks show write amplification is a problem for the
  `deletions` CF, universal compaction can be added in a follow-up.
- **RocksDB write buffer manager** — only needed if total memtable memory
  exceeds available RAM. Not critical at current scale.
- **`Serde_json` → protobuf migration** — gap-closure Epic 6.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Modify `src/metadata/store.rs`: `open_db()` and `open_cf()` functions updated with bloom filter, per-CF write buffer sizes, block cache, compression_per_level, max_open_files, optimize_level_style_compaction. New module `src/metadata/rocksdb_metrics.rs` for periodic RocksDB property export. |
| `oceanfs-core` | New field `metadata_block_cache_mb: u64` in `NodeConfig` (default: 512 MB). |

## Interface (Public API)

- `pub struct MetadataStore` — existing struct, internal configuration change only.
  Constructor changes: `MetadataStore::open(path, config)` now takes
  `&NodeConfig` (or `&MetadataConfig`) to read block cache size and
  CF-specific tuning parameters.
- `pub struct MetadataConfig` (new or existing) — groups RocksDB-tuning
  configuration: `block_cache_mb`, `objects_write_buffer_mb`,
  `segments_write_buffer_mb`, `deletions_write_buffer_mb`.
- `pub struct RocksDbMetrics` (new in `src/metadata/rocksdb_metrics.rs`) —
  holds `AtomicU64` gauges for block cache hit/miss, compaction stats,
  memtable size. Registers with `MetricsRegistry`.
- `pub fn start_rocksdb_metrics_task(db: Arc<DB>, metrics: Arc<RocksDbMetrics>, interval: Duration)`
  — spawns a background task that queries RocksDB properties every `interval`
  and updates the gauges.

## Data Flow

```
MetadataStore::open(path, config)
  ├─ Build per-CF options:
  │   ├─ objects CF:
  │   │   ├─ set_bloom_filter(10.0, false)          // 10 bits/key, block-based
  │   │   ├─ set_write_buffer_size(64 MB)
  │   │   ├─ set_compression_per_level([Snappy, Snappy, Zstd, Zstd, Zstd, Zstd, Zstd])
  │   │   └─ optimize_level_style_compaction(memtable_size)
  │   ├─ segments CF:
  │   │   ├─ set_write_buffer_size(256 MB)
  │   │   └─ set_compression_per_level(...)         // same tiered compression
  │   └─ deletions CF:
  │       ├─ set_write_buffer_size(16 MB)
  │       └─ set_compression_per_level(...)
  ├─ Block cache: Arc<Cache>::new_lru_cache(config.block_cache_mb * 1024 * 1024)
  │   └─ shared across all CFs (single cache, avoids fragmentation)
  ├─ DB options:
  │   ├─ set_max_open_files(-1)                     // unlimited, RocksDB manages its own cache
  │   └─ ... existing options (create_if_missing, etc.)
  ├─ Open DB with CF descriptors
  └─ Return MetadataStore { db, rocksdb_metrics }

Background metrics task (every 30s):
  ├─ db.property_int_value("rocksdb.block.cache.hit")?     → metrics.block_cache_hit.store(...)
  ├─ db.property_int_value("rocksdb.block.cache.miss")?    → metrics.block_cache_miss.store(...)
  ├─ db.property_int_value("rocksdb.compaction.pending")?  → metrics.compaction_pending.store(...)
  ├─ per CF: db.property_int_value("rocksdb.cur-size-all-mem-tables")?
  │   → metrics.memtable_size[cf].store(...)
  └─ db.property_int_value("rocksdb.num-running-compactions")?
      → metrics.running_compactions.store(...)
```

## Definition of Done

- [ ] **Bloom filter:** `objects` CF configured with `set_bloom_filter(10.0, false)`.
  Comment documents the 10 bits/key choice and ~1% false-positive rate.
  `seal()` function in sealer.rs calls `set_bloom_filter` on the block-based
  table options before creating the CF descriptor.
- [ ] **Per-CF write buffer sizes:** `objects` = 64 MB, `segments` = 256 MB,
  `deletions` = 16 MB. All sizes documented with workload rationale. Write
  buffer sizes exposed via `NodeConfig` entries with defaults.
- [ ] **max_open_files:** Set to `-1` (unlimited). Comment documents the
  tradeoff: safe for bounded metadata size; cap at 4096 if SST file count
  grows unboundedly.
- [ ] **Block cache:** `metadata_block_cache_mb` read from `NodeConfig`,
  default 512 MB. Shared `Arc<Cache>` across all three CFs. Comment documents
  the shared-vs-separate cache tradeoff.
- [ ] **compression_per_level:** L0-L1 = Snappy, L2+ = Zstd. Comment documents
  the speed-vs-ratio tradeoff. Uses `set_compression_per_level` on the
  block-based table options.
- [ ] **optimize_level_style_compaction:** Called on `objects` and `segments`
  CFs with the per-CF memtable size. Comment documents why universal compaction
  was considered but not adopted for `deletions` CF (deferred to follow-up).
- [ ] **RocksDB metrics:** `RocksDbMetrics` struct created with `AtomicU64`
  gauges. Background task spawned in `MetadataStore::open()` that queries
  RocksDB properties every 30s. Gauges registered with `MetricsRegistry`
  (via `registry.register_gauge("rocksdb.block_cache.hit", ...)`). Metrics
  exposed on `/admin/metrics`.
- [ ] **Config:** `NodeConfig` gains `metadata_block_cache_mb` (default 512)
  and per-CF `objects_write_buffer_mb` (64), `segments_write_buffer_mb` (256),
  `deletions_write_buffer_mb` (16). All config fields have serde `#[serde(default = "...")]`.
- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage` and
  `oceanfs-core`. No breaking changes to `MetadataStore` constructor (adds
  config parameter — consumers in `oceanfs-node` must be updated).
- [ ] **Tests:** Existing `MetadataStore` tests pass with new configuration.
  New test: `rocksdb_tuning_roundtrip` — open DB, write 1000 objects, read
  back, verify bloom filter reduces SST probes. New test:
  `rocksdb_metrics_exports` — verify metrics gauges are populated within
  30s of DB open. New test: `per_cf_write_buffer_differentiation` — verify
  different CFs have different write buffer sizes.
- [ ] **Docs:** Module-level doc in `src/metadata/store.rs` section "## RocksDB
  Tuning" documents every option with rationale and operator guidance.
  `MetadataConfig` has `# Examples` showing typical configuration.
- [ ] **ADR:** ADR-0001 (segment packing) constraints satisfied — the objects
  CF bloom filter is optimized for point lookups (GET by key), which is the
  primary access pattern for segment-pack metadata. ADR-0009 (storage crate
  split) respected — RocksDB tuning stays within `oceanfs-storage`, no
  cross-crate coupling added.
- [ ] **Perf:** Manual benchmark: 10K sequential PUTs (object metadata writes),
  followed by 10K random GETs. Bloom filter reduces GET 404 latency by ~3×
  (from 3-5 SST probes to 1 with bloom filter). Write throughput unchanged
  (bloom filter only affects reads).
- [ ] **Integration:** Node startup creates `MetadataStore` with the new
  `MetadataConfig` from `NodeConfig`. `/admin/metrics` shows RocksDB gauges.
  End-to-end PUT/GET flow exercises the configured RocksDB.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
