---
feature: "Hardware Offload"
epic: "performance-optimization"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: performance-optimization
    feature: platform-io-optimizations
    reason: "The I/O abstraction layer (DiskIo, DirectIoBuf, mmap) from Feature 3 provides the infrastructure that GDS, pmem, and RDMA paths integrate with or replace."
  - epic: gap-closure-epic-4
    reason: "WAL fsync must be correctly wired (QW-2) before pmem can replace it. The read path (parallel fetch, EC decode) must be wired before RDMA data-plane replacement is testable."
adr:
  - 0006-hardware-acceleration-tier-model
  - 0007-compression-tier-governance
perf:
  - "2.7 Tokio semaphore for concurrency limits"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
  - "12.1 SAFETY comments on every unsafe block"
created: 2026-08-05
updated: 2026-08-05
---

# Hardware Offload

## Summary

Four performance optimizations requiring specific, non-commodity hardware.
Each is feature-gated behind a Cargo feature (`gds`, `qat`, `pmem`, `rdma`)
so they compile out entirely on standard hardware — a build without these
features produces a fully functional binary with zero runtime overhead for
the offload paths. **GPU Direct Storage (GDS)** enables DMA from NVMe SSD
directly to GPU memory, eliminating the CPU-DRAM bounce buffer for CUDA EC
encode. **Intel QAT** offloads zstd compression/decompression to a hardware
accelerator, freeing CPU cores for other work. **Persistent memory (pmem)**
replaces fsync-bottlenecked WAL writes with cache-line-granularity durable
writes via DAX — no `fsync` calls at all. **RDMA (RoCE/InfiniBand)**
replaces the gRPC data plane for write replication and shard fetch with
one-sided RDMA writes and reads directly into remote memory. This is the
most architecturally significant feature in this epic — it introduces a
new transport alongside gRPC. Each offload path has a runtime probe at
startup and a portable fallback for when the hardware is absent. This is a
v2-class feature: it requires the gRPC data path, WAL, and compression to
be stable before the offload paths can be integrated and tested. Code lives
in `oceanfs-accel` (GDS, QAT), `oceanfs-storage` (pmem), and
`oceanfs-network` (RDMA).

## Scope

### In Scope

- **GPU Direct Storage (GDS).** On NVIDIA A100+ GPUs with supported NVMe
  SSDs, DMA segment data directly from SSD to GPU memory via the `cufile`
  API, bypassing CPU DRAM entirely. For the CUDA EC encode path with
  large multi-segment blobs (>100 MB, when GPU EC is used per spec §9.5),
  the standard path incurs a PCIe round-trip: SSD → CPU DRAM (DMA) →
  GPU VRAM (DMA via PCIe). GDS replaces that with: SSD → GPU VRAM in
  one DMA transfer — the NVMe controller writes directly into GPU BAR
  memory. This saves ~50% of PCIe bandwidth and eliminates the CPU-side
  buffer allocation, memcpy, and page table management.
  Implementation:
  - Feature-gated: `#[cfg(feature = "gds")]` in `Cargo.toml`.
  - Requires: NVIDIA GPU with GDS support (A100, H100, L40S),
    `libcufile.so` (CUDA toolkit 11.0+), NVMe SSD with NVMe-MI
    (management interface) support, and `nvidia-fs.ko` kernel module.
  - New type `GdsFileReader` in `oceanfs-accel/src/cuda/gds.rs`:
    ```rust
    pub struct GdsFileReader {
        file: cufile::CuFile,
        device_ptr: cudarc::driver::sys::CUdeviceptr,
    }
    impl GdsFileReader {
        pub async fn read_segment(&self, segment_id: SegmentId,
            offset: u64, len: usize) -> Result<GdsBuffer>;
    }
    ```
    where `GdsBuffer` wraps a CUDA device pointer and knows its size
    for later deallocation.
  - Integration: `CudaBackend::encode()` checks if the input segment
    data is on disk (not already in CPU memory) and if GDS is available.
    If so, it uses `GdsFileReader` instead of `cudaMemcpyAsync` for
    the H→D transfer. The GPU kernel reads directly from the GDS-
    populated VRAM region.
  - Fallback: when GDS is unavailable (no GPU, no GDS-capable drive,
    feature not compiled), fall back to standard pinned-memory
    `cudaMemcpyAsync` as implemented in the existing CUDA backend.
  - Semaphore-bounded: GDS operations are serialized through the
    existing GPU semaphore (per ADR-0006 §4) because they consume the
    same PCIe bandwidth and GPU memory.

- **Intel QAT for compression.** Offload zstd compression/decompression
  to Intel QuickAssist Technology (QAT) hardware accelerators, available
  on select Xeon Scalable processors (Skylake-SP and later with QAT
  accelerator, or discrete QAT add-in cards). QAT provides hardware-
  accelerated deflate and zstd at line rate (100+ Gbps) without consuming
  CPU cores. Implementation:
  - Feature-gated: `#[cfg(feature = "qat")]` in `Cargo.toml`.
  - Requires: QAT driver (`/dev/qat_*` devices), QATzip or QATengine
    library (`libqatzip.so` or `libqatengine.so`), Intel QAT hardware.
  - Implements the `Compressor` trait from `oceanfs-accel`:
    ```rust
    #[cfg(feature = "qat")]
    pub struct QatCompressor {
        session: QatSession,  // wraps QATzip session handle
    }
    impl Compressor for QatCompressor {
        fn compress(&self, data: &[u8], level: u32) -> Result<Vec<u8>>;
        fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    }
    ```
  - Integration: `AccelDispatcher::resolve_compressor()` probes QAT
    as part of the compression tier chain (per ADR-0007 resolution):
    ```
    GpuNvcomp > Qat > CpuIgzip > CpuZstd > None
    ```
    QAT slots in between GPU nvCOMP and CPU igzip — it is hardware-
    accelerated but doesn't consume GPU resources. The compression
    governance model (ADR-0007) applies: the node's `compression.tier`
    sets the ceiling; a bucket can only downgrade.
  - Semaphore-bounded: QAT devices have finite queue depth (16-64
    concurrent requests depending on device). Use a `Semaphore` with
    permits equal to the configured QAT queue depth.
  - Fallback: when QAT is unavailable, the compression tier chain
    skips it and falls to the next backend (CpuIgzip or CpuZstd).

- **Persistent memory (Optane/pmem) for WAL.** Map the WAL file from
  persistent memory (`/dev/pmem0` or a DAX-enabled filesystem) via
  `mmap` with direct access (DAX). Writes to the mmap'd region are
  cache-line-sized and made durable via CPU cache flush instructions
  (`clwb` + `sfence`) — no `fsync` syscall needed. This eliminates
  the fsync bottleneck that dominates write latency (1-10ms per fsync
  on NVMe). Implementation:
  - Feature-gated: `#[cfg(feature = "pmem")]` in `Cargo.toml`.
  - Requires: `/dev/pmem0` device or filesystem mounted with `-o dax`,
    `libpmem` library (`libpmem.so` from PMDK), and persistent memory
    hardware (Intel Optane DC Persistent Memory, or emulated pmem for
    development/testing).
  - New type `PmemWalWriter` in `oceanfs-storage/src/wal/pmem.rs`:
    ```rust
    pub struct PmemWalWriter {
        region: *mut u8,          // mmap'd DAX region
        region_size: usize,
        write_offset: AtomicU64,  // current append position
        committed_offset: AtomicU64, // persisted up to this point
    }
    impl PmemWalWriter {
        pub fn append(&self, entry: &WalEntry) -> Result<u64>;
        pub fn persist(&self, up_to_offset: u64); // clwb + sfence
    }
    ```
    `persist()` replaces the current `sync_all()` call in the WAL
    group commit flusher. Instead of `sync_file_range + fdatasync`,
    the flusher calls `libpmem::pmem_persist(ptr, len)` which issues
    `clwb` for each cache line in the range followed by `sfence`.
    This is a CPU instruction barrier — nanosecond-scale, not
    millisecond-scale like fsync.
  - Integration: the WAL initialization in `oceanfs-storage` probes
    for pmem availability at startup. If available and `wal_pmem_enabled
    = true`, construct a `PmemWalWriter` instead of the standard
    `File`-based `WalWriter`. The WAL group commit infrastructure
    (`WalSyncGroup`) is unchanged — only the persistence mechanism
    differs.
  - Fallback: when pmem is unavailable, fall back to the standard
    `WalWriter` (which uses the optimized `sync_file_range +
    fdatasync` path from Feature 6).
  - `unsafe`: the pmem mapping and direct pointer access are `unsafe`.
    All `unsafe` blocks must have `// SAFETY:` comments per §12.1.
    The invariants: (a) pmem region is valid DAX memory, (b) writes
    are cache-line-aligned, (c) `clwb` + `sfence` is called after
    every logical write before acknowledging to the client.

- **RDMA (RoCE/InfiniBand) for inter-node replication.** Write blob
  data directly into a remote node's memory via RDMA, bypassing the
  kernel network stack entirely. No TCP, no gRPC serialization, no
  userspace buffer copies — just a DMA transfer from NIC to NIC.
  This replaces gRPC for the data plane only: `AppendSegment` (write
  replication) and `FetchShard` (read path). The control plane (gossip,
  membership, coordination) stays on gRPC/TCP because it is low-
  bandwidth and benefits from HTTP/2 features (multiplexing, TLS).
  Implementation:
  - Feature-gated: `#[cfg(feature = "rdma")]` in `Cargo.toml`.
  - Requires: RDMA-capable NIC (NVIDIA ConnectX, AWS EFA, Intel True
    Scale), `libibverbs` (`libibverbs.so`), and an RDMA fabric (RoCE
    v2 or InfiniBand). The RDMA connection manager (`librdmacm`) handles
    connection setup.
  - New types in `oceanfs-network/src/rdma/`:
    ```rust
    pub struct RdmaTransport {
        device: RdmaDevice,           // RDMA device context
        protection_domain: Pd,        // memory registration domain
        completion_queue: Cq,         // shared CQ for all QPs
        peers: DashMap<NodeId, RdmaPeer>,  // per-peer queue pairs
    }
    pub struct RdmaPeer {
        qp: QueuePair,                // connected QP to remote node
        remote_memory: Vec<MemoryRegion>,  // registered remote MRs
    }
    impl RdmaTransport {
        pub async fn write_to_remote(&self, peer: NodeId,
            data: &[u8], remote_addr: u64, remote_rkey: u32)
            -> Result<()>;
        pub async fn read_from_remote(&self, peer: NodeId,
            local_buf: &mut [u8], remote_addr: u64, remote_rkey: u32)
            -> Result<()>;
    }
    ```
    The `RdmaTransport` replaces gRPC streaming for two specific RPCs:
    - `AppendSegment`: instead of streaming chunked protobuf messages
      over gRPC, the coordinator performs an RDMA write of the segment
      data directly into a pre-registered memory region on each
      replica node.
    - `FetchShard`: instead of a gRPC server-streaming response, the
      fetcher performs an RDMA read from the remote node's registered
      shard memory region into a local buffer.
  - Memory registration: each node pre-registers a pool of memory
    regions for RDMA — an "RDMA buffer pool" analogous to the segment
    buffer pool. Incoming writes land in these regions; outgoing reads
    source from them. The pool is sized by `rdma_buffer_pool_bytes`.
  - Integration: the `WriteCoordinator` and `ReadCoordinator` in
    `oceanfs-server` detect whether RDMA is available for the target
    peer. If yes, they use `RdmaTransport` for the data movement;
    if no (or for the first message to negotiate the RDMA connection),
    fall back to gRPC streaming. This is a hybrid transport model:
    gRPC is always available as the control channel; RDMA is an
    optional accelerator for data.
  - Architectural significance: this is a **new transport**, not a
    socket option. It introduces a parallel data path alongside gRPC.
    The DoD (see below) explicitly notes this is a v2 feature that
    requires the gRPC data path to be stable first.
  - `unsafe`: RDMA verbs (`ibv_post_send`, `ibv_post_recv`, memory
    registration) are `unsafe` FFI calls. All `unsafe` blocks must
    have `// SAFETY:` comments per §12.1.

### Out of Scope (for this feature)

- **GPU EC kernel (CUDA).** Already implemented as part of Phase 8.
  GDS only changes how data gets to the GPU, not the kernel.
- **nvCOMP GPU compression.** Already part of the compression epic;
  GDS is for EC data, not compression data.
- **FPGA-based EC acceleration.** FPGAs are a separate hardware
  category with a different programming model (bitstreams, not CUDA
  kernels). Out of scope for this feature.
- **DPDK / AF_XDP kernel bypass.** These are alternatives to RDMA
  that still use Ethernet framing. RDMA provides a cleaner abstraction
  (memory-to-memory) for this use case. DPDK evaluation is deferred.
- **TLS over RDMA.** RDMA connections are assumed to be on a trusted
  isolated fabric (management network). Encryption of the RDMA data
  path is a future consideration — RoCE v2 supports IPsec; InfiniBand
  uses fabric-level partitioning.
- **RDMA for gossip/membership.** The control plane stays on gRPC/TCP.
  RDMA is only for the high-bandwidth data plane (segment data).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-accel` | New modules: `src/cuda/gds.rs` (`#[cfg(feature = "gds")]` GdsFileReader), `src/qat.rs` (`#[cfg(feature = "qat")]` QatCompressor). Modify `src/cuda/backend.rs` to use GDS for H→D transfer when available. Modify `src/dispatcher.rs` to include QAT in compression tier chain. |
| `oceanfs-storage` | New module `src/wal/pmem.rs` (`#[cfg(feature = "pmem")]` PmemWalWriter). Modify `src/wal/mod.rs` to probe pmem and construct the appropriate writer. |
| `oceanfs-network` | New module `src/rdma/` with `transport.rs` (RdmaTransport), `peer.rs` (RdmaPeer), `memory.rs` (RdmaBufferPool), `mod.rs` (feature-gated facade). |
| `oceanfs-server` | Modify `src/write_coordinator.rs` to use RDMA for `AppendSegment` when available. Modify `src/read_coordinator.rs` to use RDMA for `FetchShard` when available. Graceful fallback to gRPC. |
| `oceanfs-core` | New config types: `GdsConfig` (in `AccelConfig`), `PmemConfig` (in `StorageConfig`), `RdmaConfig` (in `NetworkConfig`). |
| `Cargo.toml` | New features: `gds`, `qat`, `pmem`, `rdma`. All default-disabled. |

## Interface (Public API)

- `pub struct GdsFileReader` (`#[cfg(feature = "gds")]`) in
  `oceanfs-accel::cuda::gds` — reads segment data from NVMe directly
  into GPU VRAM. Implements an async read interface.
- `pub struct QatCompressor` (`#[cfg(feature = "qat")]`) in
  `oceanfs-accel::qat` — implements `Compressor` trait for QAT-
  accelerated zstd.
- `pub struct PmemWalWriter` (`#[cfg(feature = "pmem")]`) in
  `oceanfs-storage::wal::pmem` — implements the WAL append+persist
  interface using persistent memory DAX. Implements the same trait
  as standard `WalWriter`.
- `pub struct RdmaTransport` (`#[cfg(feature = "rdma")]`) in
  `oceanfs-network::rdma` — implements `write_to_remote()` and
  `read_from_remote()` for inter-node data movement. Not gRPC;
  a standalone transport.
- `pub struct RdmaConfig` in `oceanfs-core` — configuration for
  RDMA buffer pool size, device name, port, and connection parameters.
- No breaking changes to existing `Encoder`, `Decoder`, `Compressor`,
  or RPC service traits. Each offload path is an additional backend
  or an optional transport, not a replacement.

## Data Flow

**GDS data flow:**
```
Segment sealed (100 MB+) → CudaBackend::encode()
  ├─ is GDS available? && segment data is on NVMe SSD
  │     └─ YES: GdsFileReader::read_segment(segment_id)
  │            ├─ cufile::CuFile::open(nvme_device_path)
  │            ├─ allocate GPU VRAM buffer (cudaMalloc)
  │            ├─ cuFileRead(file, gpu_ptr, size, offset, 0)
  │            │     └─ NVMe SSD → GPU VRAM (direct DMA, no CPU DRAM)
  │            └─ return GdsBuffer { gpu_ptr, size }
  │     └─ NO:  standard path (cudaMemcpyAsync from pinned CPU buffer)
  ├─ launch GPU EC kernel (reads from GDS-populated VRAM)
  └─ copy parity shards GPU→CPU (cudaMemcpyAsync D→H)
```

**QAT compression data flow:**
```
Segment sealed → SegmentSealer (compression enabled)
  ├─ resolve compressor: AccelDispatcher::resolve_compressor(bucket)
  │     └─ node compression.tier = "auto"
  │          probe: nvCOMP? → QAT? → igzip? → zstd
  │          └─ QAT available: return Arc<QatCompressor>
  ├─ QatCompressor::compress(segment_data, level=3)
  │     ├─ submit to QAT device queue
  │     ├─ QAT hardware compresses (zstd/deflate at line rate)
  │     └─ return compressed data
  └─ write compressed data to disk
```

**Pmem WAL data flow:**
```
PUT /bucket/key → WriteCoordinator → WalWriter::append(entry)
  ├─ pmem available?
  │     └─ YES: PmemWalWriter
  │            ├─ memcpy(entry, mmap_region + write_offset)  // CPU write to pmem
  │            ├─ write_offset += entry_size
  │            └─ return offset
  │     └─ NO:  standard WalWriter (file-based)
  │
WalSyncGroup flusher wakes:
  ├─ pmem? → pmem_persist(mmap_region + committed, new_data_len)
  │           └─ clwb for each cache line → sfence → DONE
  │           └─ latency: ~100ns (cache flush) vs ~1-10ms (fsync)
  ├─ non-pmem? → sync_file_range + fdatasync (Feature 6)
  └─ wake all N waiters
```

**RDMA data flow (write replication):**
```
PUT /bucket/key → WriteCoordinator::replicate_to_remotes()
  ├─ for each replica node (W successors):
  │     ├─ RDMA available to peer?
  │     │     └─ YES: RdmaTransport::write_to_remote(peer, data,
  │     │                 remote_mr.addr, remote_mr.rkey)
  │     │            └─ NIC DMA: local memory → remote memory
  │     │               zero CPU involvement on remote side
  │     │     └─ NO:  gRPC AppendSegment streaming (fallback)
  │     └─ wait for W completions (RDMA completion queue or gRPC acks)
  └─ quorum reached → 200 OK to client
```

## Definition of Done

- [ ] **GDS:** `GdsFileReader` implemented (`#[cfg(feature = "gds")]`).
  `CudaBackend::encode()` uses GDS when: feature enabled, GPU GDS-capable,
  NVMe drive supports cuFile, segment size ≥ `ec_gpu_min_segment_size`.
  Unit tests with mocked `cufile` (or `#[cfg(not(feature = "gds"))]`
  fallback path). `// SAFETY:` on all `unsafe` CUDA/cuFile calls. Criterion
  benchmark: GDS H→D transfer latency vs pinned-memory `cudaMemcpyAsync`
  for 100 MB segment (expected: ~50% reduction in PCIe bandwidth consumed).
- [ ] **QAT:** `QatCompressor` implemented (`#[cfg(feature = "qat")]`).
  Implements `Compressor` trait. Probed at startup; added to compression
  tier chain (GpuNvcomp > Qat > CpuIgzip > CpuZstd). Semaphore-bounded
  with configurable QAT queue depth. Tests: compress/decompress round-trip
  matches CPU zstd output for same level; QAT unavailable → next backend
  used (no crash). Benchmark: QAT throughput vs CPU zstd for 4 MB segment
  data (expected: 2-5× throughput at equivalent compression ratio).
- [ ] **Pmem:** `PmemWalWriter` implemented (`#[cfg(feature = "pmem")]`).
  WAL initialization probes `/dev/pmem0` and DAX filesystem. `pmem_persist`
  replaces `fsync` in group commit. All `unsafe` blocks have `// SAFETY:`.
  Tests: pmem append+persist round-trip (write, kill -9, restart, verify
  recovery); pmem latency vs fsync latency benchmark (expected: ~1000×
  reduction in WAL sync latency — nanoseconds vs milliseconds). Config
  fields: `wal_pmem_enabled`, `wal_pmem_path`.
- [ ] **RDMA:** `RdmaTransport`, `RdmaPeer`, `RdmaBufferPool` implemented
  (`#[cfg(feature = "rdma")]`). `WriteCoordinator` uses RDMA write for
  `AppendSegment` when available; `ReadCoordinator` uses RDMA read for
  `FetchShard` when available. Graceful fallback to gRPC per peer. All
  `unsafe` RDMA verbs calls have `// SAFETY:`. Tests: RDMA buffer pool
  registration/de-registration; RDMA write + local read verification;
  RDMA read from pre-populated buffer; fallback to gRPC when RDMA
  unavailable. Integration test: 2-node RDMA loopback (soft-RoCE or
  SoftiWARP for CI). **This is a v2 feature** — must not block the gRPC
  data path from shipping.
- [ ] **Feature gates:** All four features compile to zero code when
  disabled (`--no-default-features`). CI builds and tests with:
  `--no-default-features`, `--features gds`, `--features qat`,
  `--features pmem`, `--features rdma`, `--all-features`.
- [ ] **Config:** New config types in `oceanfs-core`: `GdsConfig`
  (`enabled`, `min_segment_size`), `QatConfig` (`enabled`,
  `max_concurrent_ops`, `device_id`), `PmemConfig` (`enabled`, `path`),
  `RdmaConfig` (`enabled`, `device_name`, `port`, `buffer_pool_bytes`).
  All default-disabled.
- [ ] **Code:** `cargo build --all-targets` succeeds with and without
  each feature. No dead code warnings. Cross-compilation to macOS/ARM
  succeeds (all offload features compile out).
- [ ] **Tests:** All existing tests pass (no regressions from feature
  gates or new code paths). New tests for each offload path cover:
  availability probe (hardware present → path active; absent → fallback),
  functionality (correct output), error handling (hardware failure →
  graceful degradation), and benchmark.
- [ ] **Docs:** Each offload module has module-level docs explaining
  the hardware requirement, kernel/driver prerequisites, and deployment
  considerations. `// SAFETY:` comments on every `unsafe` block.
- [ ] **ADR:** ADR-0006 (acceleration tier model) satisfied — GDS is a
  CUDA backend optimization (doesn't change tier structure). QAT slots
  into the compression tier chain per ADR-0007 governance (node ceiling,
  bucket opt-down). ADR-0007 (compression tier governance) applied:
  QAT is available only when the node's `compression.tier` is ≥ QAT.
- [ ] **Perf:** Criterion benchmarks for each path: GDS vs pinned-memory
  DMA latency; QAT vs CPU zstd throughput; pmem WAL sync latency vs
  NVMe fsync; RDMA write/read latency vs gRPC streaming for 4 MB payload.
  Expected: pmem wins by 1000× on sync; RDMA wins by 3-5× on throughput;
  QAT wins by 2-5× on compression; GDS saves 50% PCIe bandwidth.
- [ ] **Integration:** End-to-end test for each offload path: PUT with
  pmem WAL → GET verifies durability; PUT with RDMA replication → GET
  from replicas; encode with GDS → decode verifies correctness;
  PUT with QAT compression → GET with decompression verifies round-trip.
  Hardware-dependent tests are `#[ignore]` by default; documented in
  the test file for manual execution on appropriately equipped hardware.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
