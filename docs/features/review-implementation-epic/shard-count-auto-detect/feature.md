---
feature: "Shard Count Auto-Detect"
epic: "review-implementation-epic"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Item 5 (buffer pool config wiring) must be complete so that cache
      pool sizes can be scaled by the derived shard count; Item 8 provides
      the same auto-detect logic for the basic case — this feature extends it
      with the compute validation and cache pool scaling
adr: []
created: 2026-08-09
updated: 2026-08-09
---

# Shard Count Auto-Detect

## Summary

The segment shard count is hardcoded to 4 (review finding #5). The spec §4.3
defines `segment_shard_count = 4` as a config default, but the proper design
is: `segment_shard_count = 0` means auto-detect from CPU count, with a
configurable cap (`segment_shard_count_max`, default 16). Changing shard
count has transitive impacts on cache pool sizes — buffer pools must scale
accordingly. This feature implements the auto-derivation formula, a startup
validation that warns if `shard_count × pool_size_bytes × segment_size`
exceeds 25% of total system memory, and cache pool size scaling based on the
derived shard count.

The gap-closure addendum (Item 8) provides the initial `derive_shard_count()`
function and `segment_shard_count_max` config field. This feature extends
that work with the memory budget validation and buffer pool scaling logic.

## Scope

### In Scope
- `derive_shard_count()` function in `oceanfs-core::config::shard` (initial impl in gap-closure Item 8; enhanced here with validation)
- `validate_shard_memory_budget()` function: compute total shard memory, compare against system memory, WARN if >25%
- System memory detection via `/proc/meminfo` (Linux) or `sysinfo` crate
- Buffer pool sizing scaled by derived shard count in `oceanfs-node`
- Startup warning printed to stderr AND logged at WARN level
- Config fields: `segment_shard_count`, `segment_shard_count_max` in `NodeConfig`

### Out of Scope (for this feature)
- Dynamically adjusting shard count at runtime (requires restart)
- Auto-detecting segment size (uses configured `segment_default_target_size`)
- Auto-detecting pool size per shard (uses configured `buffer_pool_chunk_bytes` and `buffer_pool_max_chunks`)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New module `config/shard.rs`: `derive_shard_count()`, `validate_shard_memory_budget()`; config fields in `config/node.rs` |
| `oceanfs-node` | In `node.rs`, call `derive_shard_count()` and `validate_shard_memory_budget()`; scale `BufferPool::new()` parameters by shard count; print/ log warning |
| `oceanfs-storage` | No changes |

## Interface (Public API)

- `pub fn derive_shard_count(config_shard_count: usize, config_shard_max: usize) -> usize` in `oceanfs_core::config::shard`
  - If `config_shard_count > 0`: return `config_shard_count` directly
  - Else: return `min(available_parallelism, config_shard_max)`

- `pub struct ShardMemoryValidation` in `oceanfs_core::config::shard`
  - `pub total_shard_memory_bytes: u64`
  - `pub system_memory_bytes: u64`
  - `pub exceeds_threshold: bool`
  - `pub threshold_fraction: f64`

- `pub fn validate_shard_memory_budget(shard_count: usize, pool_size_bytes: usize, segment_size_bytes: u64) -> ShardMemoryValidation` in `oceanfs_core::config::shard`
  - Computes `total_shard_memory = shard_count * pool_size_bytes * segment_size_bytes`
  - Detects system memory via `get_total_system_memory_bytes()`
  - Returns `ShardMemoryValidation { ... exceeds_threshold: total > system * 0.25 }`

- `fn get_total_system_memory_bytes() -> u64`
  - Linux: read `/proc/meminfo`, parse `MemTotal`
  - Fallback: return `u64::MAX` (skip validation when detection fails)

## Data Flow

```
oceanfs.toml:
  segment_shard_count = 0       # auto-detect
  segment_shard_count_max = 16  # cap
  buffer_pool_chunk_bytes = 65536
  buffer_pool_max_chunks = 1024
  segment_default_target_size = 4194304

    ↓ config loaded by oceanfs-node

oceanfs-node::start():
  1. shard_count = derive_shard_count(config.segment_shard_count, config.segment_shard_count_max)
     → min(num_cpus, 16) = e.g., 8

  2. validation = validate_shard_memory_budget(
       shard_count=8,
       pool_size_bytes=65536,
       segment_size_bytes=4194304,
     )
     → total = 8 × 65536 × 4194304 = 2,199,023,255,552 bytes ≈ 2 TB
     → system_memory = 32 GB (from /proc/meminfo)
     → threshold = 32GB × 0.25 = 8 GB
     → 2 TB > 8 GB → exceeds_threshold = true

  3. If exceeds_threshold:
     WARN: "Shard memory budget (2 TB = 8 shards × 65536 pool bytes × 4194304 segment bytes)
            exceeds 25% of system memory (8 GB). Consider reducing segment_shard_count,
            buffer_pool_max_chunks, or segment_default_target_size."

  4. total_pool_chunks = config.buffer_pool_max_chunks * shard_count
     buffer_pool = BufferPool::new(config.buffer_pool_chunk_bytes, total_pool_chunks)
```

## Definition of Done

- [ ] **D5.1** In `crates/oceanfs-core/src/config/shard.rs`, implement:
  ```rust
  /// Derive the effective segment shard count from configuration.
  ///
  /// If `config_shard_count > 0`, use it directly.
  /// Otherwise, auto-detect: `min(num_cpus, config_shard_max)`.
  /// Falls back to 4 if CPU count cannot be determined.
  pub fn derive_shard_count(config_shard_count: usize, config_shard_max: usize) -> usize {
      if config_shard_count > 0 {
          config_shard_count
      } else {
          let num_cpus = std::thread::available_parallelism()
              .map(|n| n.get())
              .unwrap_or(4);
          num_cpus.min(config_shard_max)
      }
  }

  /// Result of validating the shard memory budget against system memory.
  #[derive(Debug, Clone)]
  pub struct ShardMemoryValidation {
      pub total_shard_memory_bytes: u64,
      pub system_memory_bytes: u64,
      pub exceeds_threshold: bool,
      pub threshold_fraction: f64,
  }

  /// Validate that the total shard memory budget does not exceed a fraction of system memory.
  ///
  /// `pool_size_bytes` is the size of one buffer pool chunk.
  /// `segment_size_bytes` is the default segment target size.
  /// The total is: `shard_count * pool_size_bytes * segment_size_bytes`.
  /// This is a rough upper bound; actual memory usage may be lower.
  pub fn validate_shard_memory_budget(
      shard_count: usize,
      pool_size_bytes: usize,
      segment_size_bytes: u64,
  ) -> ShardMemoryValidation {
      let total_shard_memory = (shard_count as u64)
          .saturating_mul(pool_size_bytes as u64)
          .saturating_mul(segment_size_bytes);
      let system_memory = get_total_system_memory_bytes();
      let threshold_fraction = 0.25;
      let threshold = (system_memory as f64 * threshold_fraction) as u64;
      ShardMemoryValidation {
          total_shard_memory_bytes: total_shard_memory,
          system_memory_bytes: system_memory,
          exceeds_threshold: total_shard_memory > threshold && system_memory != u64::MAX,
          threshold_fraction,
      }
  }

  /// Detect total system memory from the OS.
  /// Returns `u64::MAX` if detection fails (validation is skipped).
  fn get_total_system_memory_bytes() -> u64 {
      #[cfg(target_os = "linux")]
      {
          if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
              for line in content.lines() {
                  if line.starts_with("MemTotal:") {
                      // Format: "MemTotal:       32847632 kB"
                      let parts: Vec<&str> = line.split_whitespace().collect();
                      if parts.len() >= 2 {
                          if let Ok(kb) = parts[1].parse::<u64>() {
                              return kb.saturating_mul(1024);
                          }
                      }
                  }
              }
          }
      }
      // Fallback: return max to skip validation
      u64::MAX
  }
  ```

- [ ] **D5.2** In `crates/oceanfs-core/src/config/mod.rs`, add:
  ```rust
  pub mod shard;
  ```

- [ ] **D5.3** In `crates/oceanfs-core/src/config/node.rs`, add to `NodeConfig`:
  ```rust
  /// Number of segment shards. Set to 0 to auto-detect from CPU count.
  /// Default: 0 (auto).
  #[serde(default)]
  pub segment_shard_count: usize,
  /// Maximum shard count when auto-detecting (segment_shard_count = 0).
  /// Ignored when segment_shard_count > 0. Default: 16.
  #[serde(default = "default_segment_shard_count_max")]
  pub segment_shard_count_max: usize,
  ```
  Add `fn default_segment_shard_count_max() -> usize { 16 }` and entries in `NodeConfig::default()`.

- [ ] **D5.4** In `crates/oceanfs-node/src/node.rs`, function `Node::start()`, after config is loaded but before any hardware is initialized:
  ```rust
  use oceanfs_core::config::shard;

  let shard_count = shard::derive_shard_count(
      config.segment_shard_count,
      config.segment_shard_count_max,
  );
  tracing::info!(shard_count, config_shard_count = config.segment_shard_count, "Derived segment shard count");

  let validation = shard::validate_shard_memory_budget(
      shard_count,
      config.buffer_pool_chunk_bytes,
      config.segment_default_target_size,
  );
  if validation.exceeds_threshold {
      let msg = format!(
          "Shard memory budget ({} bytes = {} shards × {} pool bytes × {} segment bytes) \
           exceeds {:.0}% of system memory ({} bytes). \
           Consider reducing segment_shard_count, buffer_pool_chunk_bytes, or segment_default_target_size.",
          validation.total_shard_memory_bytes,
          shard_count,
          config.buffer_pool_chunk_bytes,
          config.segment_default_target_size,
          validation.threshold_fraction * 100.0,
          validation.system_memory_bytes,
      );
      tracing::warn!("{}", msg);
      eprintln!("WARNING: {}", msg);
  }
  ```

- [ ] **D5.5** In the same file, when constructing `BufferPool`, scale max_chunks by shard count:
  ```rust
  let total_pool_chunks = config.buffer_pool_max_chunks * shard_count;
  let buffer_pool = Arc::new(BufferPool::new(
      config.buffer_pool_chunk_bytes,
      total_pool_chunks,
  ));
  ```
  Ensure `shard_count` is used wherever segment shards are initialized (the per-core segment groups).

- [ ] **D5.6** In `crates/oceanfs-node/src/node.rs`, wherever the shard topology is initialized (currently using hardcoded `4`), replace with `shard_count`:
  ```rust
  // OLD: let shard_topology = SegmentShardTopology::new(4);
  // NEW:
  let shard_topology = SegmentShardTopology::new(shard_count);
  ```

- [ ] **D5.7** Add to `oceanfs.toml` example:
  ```toml
  [segment]
  # Shard count: 0 = auto-detect from CPU count
  shard_count = 0
  shard_count_max = 16
  ```

## Tests Required

- [ ] **T5.1** `test_derive_shard_count_auto_detects_from_cpu` — In `crates/oceanfs-core/src/config/shard.rs` test module:
  - Call `derive_shard_count(0, 64)`.
  - Assert result > 0.
  - Assert result == `min(available_parallelism, 64)`.
  - Also test with `max = 1`: `derive_shard_count(0, 1)` asserts result == 1 (capped).

- [ ] **T5.2** `test_derive_shard_count_explicit_overrides_auto` — In same module:
  - Call `derive_shard_count(8, 16)`.
  - Assert result == 8 (ignores CPU count when explicit > 0).

- [ ] **T5.3** `test_validate_shard_memory_budget_exceeds_threshold` — In same module:
  - Call `validate_shard_memory_budget(shard_count=1000, pool_size_bytes=65536, segment_size_bytes=4194304)`.
  - Assert `result.exceeds_threshold == true` (unless system has >8 TB RAM).

- [ ] **T5.4** `test_validate_shard_memory_budget_below_threshold` — In same module:
  - Call `validate_shard_memory_budget(shard_count=1, pool_size_bytes=1024, segment_size_bytes=65536)`.
  - Assert `result.exceeds_threshold == false`.

- [ ] **T5.5** `test_shard_count_flows_to_buffer_pool_sizing` — In `crates/oceanfs-node/tests/startup_config.rs`:
  - Create `NodeConfig` with `segment_shard_count = 8`, `buffer_pool_max_chunks = 100`.
  - Start a minimal node (without full gRPC — just the startup sequence up to pool construction).
  - Assert `BufferPool::max_chunks() == 800` (100 × 8).

- [ ] **T5.6** `test_warning_emitted_when_memory_budget_exceeded` — In `crates/oceanfs-node/tests/startup_config.rs`:
  - Set `segment_shard_count = 10000` (force unrealistic), `buffer_pool_chunk_bytes = 65536`, `segment_default_target_size = 4194304`.
  - Capture stderr and tracing output.
  - Assert a WARN log message and stderr message both contain "exceeds 25% of system memory".

- [ ] **T5.7** `test_shard_count_config_serde_roundtrip` — In `crates/oceanfs-core/src/config/node.rs` tests:
  - Serialize `NodeConfig` with `segment_shard_count = 0, segment_shard_count_max = 32` to TOML.
  - Deserialize.
  - Assert `segment_shard_count == 0` and `segment_shard_count_max == 32`.
  - Repeat with `segment_shard_count = 4` (explicit override), assert field preserved.

## ADR References

- No specific ADR for this feature. It follows the established config system patterns and the spec §4.3, §8.1, §11.2 configuration surface.
