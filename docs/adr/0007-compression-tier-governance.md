# ADR-0007: Node-Governed Compression Tier with Per-Bucket Opt-Down

**Status:** Proposed
**Date:** 2026-08-02
**Deciders:** OceanFS design team

---

## Context

The current specification (§9.9.2, §14.2) and ADR-0006 (§5) define
compression tier selection as **per-bucket only**, with no node-level default:

```toml
# Spec §9.9.2 — current design
[bucket.my-bucket.acceleration]
compress_tier = "auto"   # per-bucket only — no node-level default
```

The rationale (Spec §9.6.2) was: "compression is workload-dependent and only
meaningful to enable for specific buckets with compressible data." The bucket
owner knows their data, so they choose the compression tier.

This creates three structural problems:

### 1. Noisy Neighbor — Tenant Configuration Impacts Shared Node Resources

Compression backends are shared node resources. A GPU (nvCOMP) has finite
VRAM, a single PCIe bus, and compute cores that are serialized through the
`tokio::sync::Semaphore` (ADR-0006 §4, default 1 permit). When bucket A
configures `compress_tier = "gpu"`, all GPU compression operations from that
bucket consume the semaphore.

Since buckets are placed on nodes via consistent hashing (Spec §2.2),
bucket co-location is not under operator control. A single bucket can
saturate the GPU, starving all other co-located buckets — including those
that did not request GPU compression.

### 2. Unpredictable Per-Node Performance

The node operator cannot reason about the performance envelope of their
hardware because bucket configuration is dynamic and externally controlled.
A node operating with predictable CPU-only compression at X IOPS may
suddenly drop to 0.7X because a new bucket with GPU compression was created
on that node, consuming PCIe bandwidth and competing for GPU semaphore
waits — even if the bucket's data is incompressible and the GPU produces no
space savings.

This violates a fundamental operational principle: the operator provisions
hardware and must be able to guarantee a performance floor.

### 3. Asymmetric Model — Inconsistency with EC Acceleration

The EC acceleration tier model (Spec §9.9.1-9.9.2, ADR-0006 §7) uses a
two-level governance structure:

```
Node-level ec_tier: "auto"          ← operator controls available backends
Bucket-level accel_ec_tier: "gpu"   ← bucket can only downgrade from node
```

A bucket requesting `gpu_cuda` on a CPU-only node falls back to ISA-L
or CPU SIMD — the bucket **cannot upgrade** beyond what the node
provides. The node is the resource governor.

Compression has the opposite model: the bucket alone decides, with zero
node-level governance. There is no way for the operator to say "this
cluster runs CPU-only compression."

### Constraints

- The `Compressor` trait and `AccelDispatcher::resolve_compressor()` already
  exist in `oceanfs-accel` (source: `crates/oceanfs-accel/src/dispatcher.rs`
  L299-328). The fallback chain GpuNvcomp → CpuIgzip → CpuZstd is
  implemented.
- Compression is a **future epic** (not yet integrated into the write
  path). The design change has no runtime impact today — it is a
  specification amendment.
- The crate dependency DAG (`oceanfs-accel` between `oceanfs-ec` and
  `oceanfs-storage`) is unaffected.
- The `oceanfs-core` `CompressConfig` type already has a `tier` field
  (source: `crates/oceanfs-core/src/types.rs` L1165-1206). Adding a
  node-level config requires only a new config struct in `oceanfs-core`.

## Decision

**Add a node-level `compression` section to `oceanfs.toml` that governs
available compression backends. Per-bucket `compress_tier` can only select
from or downgrade from what the node provides — it cannot upgrade.**

### Node-Level Configuration

```toml
# oceanfs.toml — NEW section
[compression]
# Whether segment compression is enabled at all.
# When disabled, no compression is applied regardless of bucket settings.
enabled = true

# Compression acceleration tier available on this node.
#   "auto"     — probe: nvCOMP > ISA-L igzip > CPU zstd (default)
#   "cpu_zstd" — zstd crate only (CPU, always available)
#   "cpu_igzip" — ISA-L igzip (requires isa-l feature + AVX-512)
#   "gpu_nvcomp" — nvCOMP GPU batch (requires cuda feature + nvCOMP library)
#   "none"     — no compression, even if bucket requests it
tier = "auto"

# GPU-specific compression settings
compression_gpu_min_batch_bytes = 1048576   # 1 MB — minimum batch size for GPU offload
```

The node-level `compression.tier` controls the **ceiling** — the maximum
acceleration tier available to any bucket on this node. A value of
`"cpu_zstd"` means no bucket may use ISA-L igzip or nvCOMP, regardless
of its own `compress_tier` setting.

### Per-Bucket Configuration (Revised)

```toml
# bucket config — REVISED semantics
[bucket.my-bucket.compression]
# Whether to compress segment data for this bucket.
#   "auto"     — use node's tier, with node's algorithm selection
#   "cpu_zstd" — explicit CPU zstd (always a safe downgrade)
#   "cpu_igzip" — ISA-L igzip (falls back if node doesn't support it)
#   "gpu_nvcomp" — nvCOMP (falls back if node doesn't support it)
#   "none"     — no compression for this bucket
tier = "auto"

# Compression level (0-22 for zstd, 0-12 for igzip)
level = 3
```

### Resolution Semantics

When the write path processes a bucket with `compress_tier = T_bucket`:
1. If the node has `compression.enabled = false` → no compression for any
   bucket.
2. If the node has `compression.tier = T_node`:
   - Resolve `effective_tier = min(T_bucket, T_node)` in the fallback chain
     (GpuNvcomp > CpuIgzip > CpuZstd > None).
   - A bucket requesting `gpu_nvcomp` on a `cpu_zstd` node gets `cpu_zstd`.
   - A bucket requesting `none` always gets `none` regardless of node tier.
3. Bucket `level` passes through to the resolved compressor.

The `min` operation is defined on the fixed capability ordering:

```
GpuNvcomp > CpuIgzip > CpuZstd > None
```

A bucket can only select a tier ≤ the node's tier. This mirrors the EC
model exactly: the bucket can **downgrade** (e.g., request `cpu_zstd` even
when GPU is available, to avoid GPU semaphore contention for latency-
sensitive buckets) but can never **upgrade** beyond what the node provides.

### Migration Path

This is a specification amendment. Since compression is not yet integrated
into the write path, no runtime code changes are required immediately.
The concrete implementation work is:

1. Add `CompressionConfig` to `oceanfs-core` (node-level config struct).
2. Extend `AccelConfig` or add a `node_compression_tier` field to
   `AccelDispatcher::new()`.
3. Modify `resolve_compressor()` to cap the requested tier at the node's
   ceiling.
4. Update spec §9.6.2, §9.9.1, §9.9.2, §14.1, §14.2.
5. Update ADR-0006 §5.

## Consequences

### Positive
- **Operator control restored.** The node operator provisions hardware and
  declares what compression backends are available. Node performance is
  predictable regardless of bucket configuration.
- **Noisy neighbor eliminated.** A bucket cannot commandeer GPU resources
  on a CPU-only node or a node where GPU compression has been disabled.
- **Consistent governance model.** Compression follows the same two-level
  structure as EC acceleration: node = ceiling, bucket = opt-down.
- **Buckets still control their algorithm.** A latency-sensitive bucket can
  choose `cpu_zstd` (no GPU semaphore wait) even on a GPU-enabled node.
  A bucket of pre-compressed data can choose `none`.
- **No breaking change.** Compression is not yet in the write path.

### Negative
- **Slightly more configuration.** Operators now have a `[compression]`
  section in `oceanfs.toml`. The default (`enabled = true`, `tier = "auto"`)
  means most deployments need zero configuration.
- **Bucket "auto" is now bounded.** A bucket with `compress_tier = "auto"`
  previously meant "probe nvCOMP > igzip > zstd." Now it means "use the
  best tier the node provides." On a `cpu_zstd`-only node, `auto` resolves
  to `cpu_zstd`. This is a restriction, but a desirable one — the bucket's
  `auto` should not override the operator's intent.

### Neutral
- **The `AccelDispatcher` gains one more field** (`node_compression_tier` or
  equivalent). This is a minor code change in a well-covered module.
- **Spec sections require updating.** §9.6.2, §9.9.1, §9.9.2, §14.1, §14.2
  must be revised to reflect the two-level model. This is documentation only
  (compression is not yet in any implementation phase).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Per-bucket only (current design)** | Simpler config; bucket owner has full control | Noisy neighbor problem; unpredictable node performance; operator has no resource governance; inconsistent with EC model | Rejected: violates the principle that the operator controls shared node resources. The EC model already proves the two-level design is correct. |
| **Node-level only (remove per-bucket compression config entirely)** | Simplest possible config; operator has full control | Bucket owner loses ability to opt out of compression for incompressible data (text logs vs JPEGs); cannot choose lower tier for latency-sensitive buckets | Rejected: overcorrects. Per-bucket `compress_tier = "none"` is valuable for buckets of pre-compressed data. Per-bucket `"cpu_zstd"` is valuable for latency-sensitive workloads that want to avoid GPU semaphore contention. |
| **Node-level `tier` but per-bucket can override to any tier (no ceiling)** | Operators have a default; buckets can still request GPU | Same noisy neighbor problem — a bucket can override the operator's intent. No performance predictability. | Rejected: the point of node-level governance is to set a hard ceiling. A soft default that any bucket can override provides no operational guarantee. |
| **Resource quotas (e.g., "max 2 concurrent GPU compression ops per bucket")** | Fine-grained fairness; no bucket can starve others | Complex to implement, configure, and debug. Adds per-bucket tracking, admission control, and fairness scheduling to the compression path. Compression is a future epic — this is preemptive complexity for a problem that the ceiling model solves with one line of config. | Rejected: YAGNI. The node ceiling prevents the worst-case (GPU saturation by untrusted tenants). Quotas can be added later if needed, but they don't replace the need for a ceiling. |

## References

- [Spec §9.6.2: zstd Compression](../spec.md#962-zstd-compression) — current
  per-bucket-only design language.
- [Spec §9.9.2: Bucket Configuration (per-bucket override)](../spec.md#992-bucket-configuration-per-bucket-override) — `compress_tier`
  field.
- [Spec §9.9.1: Node Configuration](../spec.md#991-node-configuration-oceanfstoml) — currently has no `[compression]` section.
- [Spec §14.1-14.2: Configuration Reference](../spec.md#14-configuration-reference) — node and bucket configuration.
- [ADR-0006: Hardware Acceleration Tier Model](0006-hardware-acceleration-tier-model.md) §5 — states
  "`compress_tier` is per-bucket only; there is no node-level compression tier
  configuration." This ADR amends that statement.
- [ADR-0006: Hardware Acceleration Tier Model](0006-hardware-acceleration-tier-model.md) §7 — defines
  the per-bucket override model for EC that this ADR mirrors.
- [Architecture Guidelines §1.2: Crate Responsibilities](../../guidelines/architecture.md#12-crate-responsibilities) — `oceanfs-core` owns config types; `oceanfs-accel` owns dispatcher.
- Code: `CompressionTier` enum in `crates/oceanfs-core/src/types.rs` L1133.
- Code: `CompressConfig` struct in `crates/oceanfs-core/src/types.rs` L1165.
- Code: `AccelDispatcher::resolve_compressor()` in `crates/oceanfs-accel/src/dispatcher.rs` L314-320.
