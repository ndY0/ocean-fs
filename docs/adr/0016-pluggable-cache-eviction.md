# ADR-0016: Pluggable Cache Eviction Policy — `EvictionPolicy` Trait, GDSF, and Adaptive Learner Path

**Status:** Proposed
**Date:** 2026-08-09
**Deciders:** OceanFS design team

---

## Context

A manual code review on 2026-08-09 identified that L1 (object data) and L2
(metadata) cache eviction uses a linear scan (finding #13). The current
implementation assumes "the cache is always hot" — but eviction frequency is
load-dependent. Under heavy churn, a linear scan becomes a latency spike.

Beyond the immediate fix, there is a forward-looking requirement: the cache
should eventually support an adaptive learner that builds an eviction affinity
profile per blob from observed workload patterns, rather than relying on a
static TTL-based policy. This means the eviction mechanism must be pluggable.

The spec §5.2 defines three caching layers with per-bucket configuration. The
architecture guideline §2.1 (ADR-0005) mandates traits in the consuming crate.
Performance guideline §2.2 mandates `DashMap` for concurrent caches.

## Decision

### 1. `EvictionPolicy` Trait in `oceanfs-cache`

A new trait defines the eviction contract:

```rust
/// Pluggable eviction policy for the object and metadata caches.
///
/// Implementations range from simple TTL-LRU to adaptive learned policies.
/// The trait is called by the cache frontend on every access, insert, eviction,
/// and removal — the policy is strictly advisory (it selects victims; the
/// frontend performs the actual removal from the backing store).
pub trait EvictionPolicy: Send + Sync {
    /// Called on every cache hit. The policy may update internal state
    /// (e.g., promote in LRU order, increment frequency counter).
    fn on_access(&self, key: &CacheKey, meta: &AccessMetadata);

    /// Called when a new entry is inserted. `size` is the entry's memory
    /// footprint in bytes. The policy may track this for size-aware eviction.
    fn on_insert(&self, key: &CacheKey, size: usize, meta: &AccessMetadata);

    /// Called when the cache frontend needs to free memory. Returns the key
    /// of the entry that should be evicted, or `None` if the policy has no
    /// preference (frontend falls back to arbitrary eviction).
    ///
    /// Called in a loop until the cache is below its memory threshold.
    fn select_victim(&self) -> Option<CacheKey>;

    /// Called after an entry has been successfully evicted from the backing
    /// store. The policy may clean up any per-entry state.
    fn on_evict(&self, key: &CacheKey);

    /// Called when an entry is explicitly removed (invalidation, delete).
    /// Distinct from eviction because the policy shouldn't count forced
    /// removals as evidence for its eviction heuristics.
    fn on_remove(&self, key: &CacheKey);
}
```

**`AccessMetadata` — extensible per-access signals:**

```rust
pub struct AccessMetadata {
    pub timestamp: Instant,
    pub blob_size: u64,
    pub bucket_id: BucketId,
    pub content_type: Option<String>,
    /// Reserved for future learner features. Unused by TTL-LRU and GDSF.
    pub extensions: HashMap<String, String>,
}
```

The `extensions` field allows the adaptive learner (future) to attach
additional signals without breaking the trait contract. Current policies
ignore it.

**Concurrency:** All methods take `&self` (not `&mut self`) because the policy
is called from multiple concurrent cache operations. Implementations must use
interior mutability (atomic counters, `DashMap`, `parking_lot::Mutex` on
internal state).

**Placement:** The trait lives in `oceanfs-cache` (the crate that consumes it).
This follows ADR-0005 (trait-in-consuming-crate). `oceanfs-server` constructs
the concrete policy and passes it to the cache at startup.

### 2. L1 Object Cache: GDSF Policy

The L1 cache uses **Greedy-Dual Size Frequency (GDSF)**, a size-aware
eviction policy well-suited to mixed-size workloads:

- **Priority score:** `clock + (frequency / size)`
- **On access:** increment frequency, update priority
- **On eviction:** select entry with lowest priority
- **On insert:** assign priority = `global_clock + (1 / size)`, increment
  `global_clock` by the evicted entry's priority (or by 1 if no eviction)

GDSF naturally balances:
- **Size:** large blobs have lower priority (evicted faster) — avoids
  evicting 100 small hot blobs to make room for one large lukewarm blob
- **Frequency:** frequently accessed blobs accumulate priority — resists
  eviction
- **Recency:** the `clock` value provides an aging mechanism — entries
  accessed long ago see their relative priority decline

The metric to optimise is **byte hit ratio** (bytes served from cache / total
bytes requested), not raw hit count.

### 3. L2 Metadata Cache: LRU + TTL Policy

The L2 cache uses **LRU with TTL**, which is appropriate for metadata:

- Entries are uniformly small (~200 bytes) — size-awareness provides no benefit
- The miss penalty is low (one RocksDB GET) — aggressiveness of eviction matters
  less than correctness of staleness
- TTL is the **hard coherence deadline** — metadata gossip invalidation is lazy,
  so stale entries must be evicted by TTL regardless of access pattern

The `TtlLruPolicy` implements `EvictionPolicy` with:
- Internal: `DashMap<CacheKey, (Instant, u64)>` (insertion time + access count)
- `select_victim()` returns the entry with the oldest `(ttl_expiry, last_access)`
- `on_access()` updates `last_access`

### 4. Future: Adaptive Learner Path

The `EvictionPolicy` trait is designed so that a future `AdaptiveLearnerPolicy`
can be dropped in without changing the cache frontend:

- The learner observes `AccessMetadata` on every `on_access` and `on_insert`
- Builds a per-blob (or per-blob-class) model: `P(access in next T seconds | features)`
- Features: `{bucket_id, content_type, size_quantile, hour_of_day,
  access_count_last_5m, time_since_last_access}`
- `select_victim()` returns the entry with the lowest predicted access
  probability divided by size
- The learner uses an online update rule (e.g., stochastic gradient descent
  on a logistic regression model) — no batch training required

For L2, the learner's role is different: it tunes the **per-bucket TTL** based
on observed write rates rather than predicting per-entry access probability.
High-write buckets get shorter TTLs; read-only archives get effectively
infinite TTL.

This learner is **out of scope** for this ADR. The ADR only guarantees that
the trait boundary supports it without a breaking change.

### 5. Linear Scan Removal

The immediate trigger for this ADR — the linear scan eviction — is resolved
by both policies:

- GDSF uses a priority queue (binary heap or BTreeMap) — O(log n) victim
  selection, not O(n)
- LRU uses a time-ordered structure — O(log n) or O(1) depending on
  implementation (a linked hash map gives O(1) LRU eviction)

### Scope

**In scope:**
- `EvictionPolicy` trait definition in `oceanfs-cache`
- GDSF implementation for L1 object cache
- LRU+TTL implementation for L2 metadata cache
- Replacement of linear scan with O(log n) / O(1) eviction
- `AccessMetadata` struct with extension field

**Out of scope:**
- Adaptive learner implementation (future epic)
- L3 negative cache changes (Bloom filter, no eviction)
- External crate usage (`moka`, `quick_cache`) — we need the trait boundary

## Consequences

### Positive

- **Efficient eviction under load.** GDSF (L1) and LRU+TTL (L2) replace O(n)
  linear scan with O(log n) or O(1) eviction. Cache churn no longer produces
  latency spikes.
- **Pluggable future policies.** The `EvictionPolicy` trait supports swapping
  the eviction algorithm without changing the cache frontend. The adaptive
  learner can be implemented as a new `impl EvictionPolicy` with zero code
  changes to the cache or server.
- **Size-aware cache for L1.** GDSF prevents large lukewarm blobs from
  displacing small hot blobs — the dominant failure mode of plain LRU in
  mixed-size workloads.
- **Staleness safety for L2.** TTL guarantees metadata entries don't outlive
  their coherence window, even under high access frequency.
- **Extensible metadata.** `AccessMetadata::extensions` allows the learner to
  attach arbitrary signals without a breaking change to the trait.

### Negative

- **GDSF complexity.** GDSF requires maintaining a priority queue (global
  clock + per-entry priority). This is more code and more state than a
  simple linked list LRU.
- **Two policy implementations to maintain.** L1 and L2 use different
  policies — GDSF and LRU+TTL each need tests, benchmarks, and tuning.
- **Interior mutability in policies.** `&self` methods force all policy
  state behind atomics or locks. This is a minor ergonomic cost for
  implementors.
- **No off-the-shelf crate.** Using `moka` or `quick_cache` would be fewer
  lines of code, but neither supports a pluggable eviction trait — they
  bake in their own policies. The trait boundary is worth the implementation
  cost given the learner roadmap.

### Neutral

- **L3 negative cache is unaffected.** Bloom filter has no eviction — it
  is periodically rebuilt.
- **Prefetch engine is unaffected.** Prefetch populates caches but doesn't
  participate in eviction decisions.
- **Per-bucket cache configuration** (size, TTL, enabled) continues to work
  as before — the policy consumes these values from `CacheConfig` at
  construction time.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. Use `moka` or `quick_cache` crate** | Battle-tested, lock-free, well-benchmarked; zero implementation effort | Bakes in its own eviction policy; no pluggable trait; can't support future adaptive learner without forking; adds an external dependency with its own versioning lifecycle | Rejected: the learner roadmap requires a trait boundary we control |
| **B. Plain LRU for both L1 and L2** | Simplest implementation; one policy for both caches | LRU ignores blob size — large lukewarm blobs displace small hot ones; this is a known pathological failure mode for blob stores with mixed object sizes | Rejected: L1 needs size-awareness; GDSF is the minimum viable policy for this workload |
| **C. LFU (Least Frequently Used) for L1** | Resists one-hit-wonder eviction better than LRU | LFU accumulates frequency indefinitely — an entry accessed 1000 times last month beats one accessed 10 times in the last minute; no aging mechanism; stale metadata risk in L2 | Rejected: LFU without aging is worse than LRU for caching; GDSF's clock mechanism provides proper aging |
| **D. BTreeMap by TTL for both caches** | Simple ordered structure; O(log n) for all operations | TTL-only ignores access frequency entirely; a blob accessed 10,000 times per second is evicted the instant its TTL expires, even if it's the hottest key in the system | Rejected: TTL is a deadline, not a ranking; it must be combined with access frequency |

## References

- [Spec §5.2: Caching Layers](../../docs/spec.md#52-caching-layers)
- [Review 2026-08-09, finding #13](../../review/08-09-2026.md)
- [ADR-0005: Trait-in-Consuming-Crate Pattern](./0005-trait-in-consuming-crate.md)
- [Performance Guideline §2.2: DashMap for concurrent caches](../../guidelines/performance.md#22-dashmap-for-concurrent-caches)
- [Performance Guideline §6.5: BTreeMap for ordered access](../../guidelines/performance.md#65-btreemap-over-hashmap-for-ordered-access)
