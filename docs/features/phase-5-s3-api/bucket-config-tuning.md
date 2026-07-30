---
feature: "Bucket Configuration & Per-Bucket Policy"
epic: "phase-5-s3-api"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: s3-http-handlers
    reason: Bucket config is set via POST /{bucket}?policy
  - feature: tiered-segment-routing
    reason: Per-bucket tier thresholds override node defaults
adr:
  - 0001-segment-packing
perf:
  - "2.4: ArcSwap for read-mostly shared data (bucket policies)"
created: 2026-07-30
updated: 2026-07-30
---

# Bucket Configuration & Per-Bucket Policy

## Summary

Implement bucket-level configuration and per-bucket policy management in
`oceanfs-server`. Every performance, consistency, and sizing parameter is
overridable per bucket (see spec §8.1). Policies are stored in a bucket config
file, loaded at startup, and hot-reloaded via `POST /{bucket}?policy`.
`ArcSwap<BucketPolicy>` enables wait-free reads of bucket config on the hot
path.

## Scope

### In Scope
- `BucketPolicy` struct: all per-bucket tunables from spec §8.1
- `BucketConfigStore`: loads policies from `data_dir/buckets/`, validates, serves lookups
- `POST /{bucket}?policy`: accept JSON/TOML policy body, validate, store, propagate
- Config validation: reject invalid combinations (e.g., `m=0` with `k>0`, `R > N`)
- Default policy fallback: bucket not explicitly configured → use node-level defaults
- `ArcSwap<BucketPolicy>`: atomic policy updates without blocking readers
- LIST buckets: `GET /` returns all configured buckets
- DELETE bucket: remove policy + all objects (with confirmation if not empty)
- Unit tests for policy validation, hot-reload, bucket lifecycle

### Out of Scope
- IAM-style multi-tenancy policies (future work)
- Policy inheritance (buckets only; no account-level policies)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `BucketPolicy`, `BucketId`, `ConsistencyConfig`, `TuningConfig` |
| `oceanfs-server` | New modules: `bucket/config.rs`, `bucket/store.rs`, `bucket/policy.rs` |

## Interface (Public API)

- `pub struct BucketPolicy` — combines `ConsistencyConfig`, `SegmentConfig`, `EcConfig`, `CacheConfig`, `TuningConfig`, `HealConfig`, `GcConfig`
- `pub struct ConsistencyConfig` — `write_quorum: u8`, `read_quorum: u8`, `total_replicas: u8`
- `pub struct SegmentConfig` — `inline_threshold_bytes: u64`, `segment_small_threshold_bytes: u64`, `segment_small_target_size: u64`, `segment_default_target_size: u64`, `seal_timeout_ms: u64`, `active_pool_size: usize`, `shard_count: usize`
- `pub struct EcConfig` — `data_shards: u8`, `parity_shards: u8`, `strip_size_bytes: usize`, `codec: CodecType`
- `pub struct BucketConfigStore` — `pub fn new(data_dir: &Path) -> Self`, `pub async fn load_all(&self) -> Result<()>`, `pub fn get(&self, bucket: &BucketId) -> Option<Arc<BucketPolicy>>`, `pub async fn put(&self, bucket: BucketId, policy: BucketPolicy) -> Result<()>`, `pub async fn delete(&self, bucket: BucketId) -> Result<()>`, `pub fn list(&self) -> Vec<BucketId>`

## Data Flow

```
Default policy (from oceanfs.toml node config):
  ↓
Bucket overrides (from data_dir/buckets/{bucket_id}.toml):
  ├─ my-bucket: read_quorum=1, write_quorum=3, ec_k=8, ec_m=2
  ├─ archive: inline_threshold=0, ec_k=10, ec_m=4, read_quorum=3
  └─ ephemeral: inline_threshold=65536, negative_cache=true

Read path:
  BucketConfigStore::get(bucket_id) → Option<Arc<BucketPolicy>>
    ├─ Some(policy) → use bucket-specific policy
    └─ None → use node default policy

Hot-reload:
  POST /{bucket}?policy
    body: TOML with BucketPolicy
      → validate: R + W > N? k > 0? m > 0? thresholds consistent?
        → write to data_dir/buckets/{bucket}.toml
          → ArcSwap::store(Arc::new(policy))
            → all in-flight requests see new policy on next get() call
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-server`
- [ ] **Tests:** Unit tests: policy validation (rejects W > N, rejects k=0, rejects negative sizes), hot-reload (reader sees new policy after update), list buckets, delete bucket, default fallback when no override, concurrent reads during ArcSwap (no data race)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `BucketPolicy` fully documented with all fields
- [ ] **ADR:** ADR-0001 threshold configuration reflected in `SegmentConfig`
- [ ] **Perf:** Rule 2.4 (ArcSwap for wait-free policy reads)
- [ ] **Integration:** `tests/bucket_policy.rs`: create bucket with custom policy, PUT object, verify segment tier matches policy, hot-reload policy, verify new writes use updated policy
- [ ] **Manual:** TOML example in `BucketPolicy` docs matches spec §14.2
