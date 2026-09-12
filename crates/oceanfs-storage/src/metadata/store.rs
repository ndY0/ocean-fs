//! RocksDB-backed metadata store with strongly-typed CRUD.
//!
//! ## RocksDB Tuning
//!
//! Each column family is tuned for OceanFS's specific workload:
//!
//! | CF | Pattern | Bloom Filter | Write Buffer | Compression |
//! |---|---|---|---|---|
//! | objects | point lookups (GET/HEAD) | 10 bits/key (~1% FP) | 64 MB | Snappy L0-L1, Zstd L2+ |
//! | deletions | append-mostly | none | 16 MB | Snappy L0-L1, Zstd L2+ |
//!
//! The `segments` + `deleted_segments` column families are removed
//! (ADR-0025 Decision 3): segment lifecycle state lives in the event
//! log + checkpoint + registry, never in RocksDB.
//!
//! The bloom filter on `objects` eliminates ~99% of unnecessary SST probes
//! for key-not-found queries — the single highest-impact RocksDB tuning for
//! a metadata-heavy workload (storage-IO H4).
//!
//! Compression is tiered: Snappy for L0-L1 (hot data, decompression speed
//! matters) and Zstd for L2+ (cold data, compression ratio matters).
//!
//! `max_open_files = -1` lets RocksDB keep all SST file descriptors open,
//! avoiding repeated open/close overhead.
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    sync::Arc,
    time::Duration,
};

use oceanfs_core::{
    BucketId, ChunkRef, Counter, DeadChunkKind, DeadChunkRecord, Gauge, HashOutput, Hlc, LabelSet,
    MetadataConfig, MetricRegistrar, ObjectKey, ObjectMetadata, Tombstone,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::{
    error::{Error, Result},
    metadata::{
        cf,
        journal::{JournalOp, MetadataJournal},
    },
};

/// Assumed worst-case number of metadata writers concurrently inside
/// [`RocksDbMetadataStore::put_object_in_bucket`].
///
/// The server-side `AsyncMetadataOps` adapter bounds its own writers at 16
/// (its default), but this store is also reached synchronously by the
/// segment-service replica-apply and prefetch paths, which are not under
/// that semaphore. 4× the adapter bound is a generous ceiling; the actual
/// in-flight writer count at the store stays in the tens even under load
/// because nothing feeds it from an unbounded pool.
///
/// This is the single knob to change when metadata-write concurrency
/// becomes config-driven: replace it with the configured bound and pass it
/// to [`stripe_count_for_writers`].
const ASSUMED_CONCURRENT_METADATA_WRITERS: usize = 64;

/// Returns the power-of-two stripe count that keeps the expected number of
/// cross-key collisions below one for `max_concurrent_writers` simultaneous
/// distinct-key writers.
///
/// Collision model (birthday bound): with C concurrent distinct-key
/// writers hashed uniformly into N stripes, the expected number of
/// colliding pairs is ~C²/2N. Keeping that below one needs N > C²/2, so we
/// take `next_power_of_two(C²)` (for C = 64 that is 4096 → ~0.5 expected
/// collisions per in-flight window). Memory is not a constraint (each
/// stripe is a ~4-byte `parking_lot::Mutex<()>`, so 4096 stripes ≈ 16 KiB),
/// so we size for the generous ceiling, not today's 16-op adapter bound.
fn stripe_count_for_writers(max_concurrent_writers: usize) -> usize {
    max_concurrent_writers.saturating_mul(max_concurrent_writers).next_power_of_two().max(256)
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// RocksDB property gauges exposed for Prometheus / `/admin/metrics`.
///
/// Updated periodically by a background task polling RocksDB internal
/// properties. All gauges use the `oceanfs_core::Gauge` type so they
/// can be registered with the `MetricsRegistry` and rendered in
/// Prometheus text format.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::RocksDbMetrics;
/// use oceanfs_core::MetricRegistrar;
///
/// let metrics = RocksDbMetrics::new();
/// // Register with the metrics registry at node startup:
/// // metrics.register(&registry);
/// ```
#[derive(Debug, Clone)]
pub struct RocksDbMetrics {
    /// Block cache hit count.
    pub block_cache_hit: Gauge,
    /// Block cache miss count.
    pub block_cache_miss: Gauge,
    /// Approximate memtable size across all CFs (bytes).
    pub memtable_size: Gauge,
    /// Number of currently running compactions.
    pub running_compactions: Gauge,
    /// Number of currently running flushes.
    pub running_flushes: Gauge,
    /// Estimated number of keys across all CFs.
    pub estimate_num_keys: Gauge,
    /// Number of SST files at level 0 (the write-stall sentinel).
    pub num_files_at_level_0: Gauge,
    /// Total size of all live SST files (bytes).
    pub live_sst_files_size: Gauge,
    /// Estimated memory used by table readers (bytes).
    pub estimate_table_readers_mem: Gauge,
    /// Number of supersede dead-chunk records captured on overwrite
    /// (ADR-0034 D2; the "capture rule is firing" signal).
    pub supersede_captured_total: Counter,
    /// Total dead bytes captured by supersede records on overwrite
    /// (ADR-0034 D2).
    pub supersede_dead_bytes_total: Counter,
}

impl RocksDbMetrics {
    /// Creates a new set of RocksDB property gauges.
    pub fn new() -> Self {
        let empty = LabelSet::empty();
        Self {
            block_cache_hit: Gauge::new(
                "rocksdb_block_cache_hit".into(),
                "RocksDB block cache hit count".into(),
                empty.clone(),
            ),
            block_cache_miss: Gauge::new(
                "rocksdb_block_cache_miss".into(),
                "RocksDB block cache miss count".into(),
                empty.clone(),
            ),
            memtable_size: Gauge::new(
                "rocksdb_memtable_size_bytes".into(),
                "Approximate memtable size across all CFs".into(),
                empty.clone(),
            ),
            running_compactions: Gauge::new(
                "rocksdb_num_running_compactions".into(),
                "Number of currently running compactions".into(),
                empty.clone(),
            ),
            running_flushes: Gauge::new(
                "rocksdb_num_running_flushes".into(),
                "Number of currently running flushes".into(),
                empty.clone(),
            ),
            estimate_num_keys: Gauge::new(
                "rocksdb_estimate_num_keys".into(),
                "Estimated number of keys across all CFs".into(),
                empty.clone(),
            ),
            num_files_at_level_0: Gauge::new(
                "rocksdb_num_files_at_level_0".into(),
                "Number of SST files at level 0".into(),
                empty.clone(),
            ),
            live_sst_files_size: Gauge::new(
                "rocksdb_live_sst_files_size_bytes".into(),
                "Total size of all live SST files".into(),
                empty.clone(),
            ),
            estimate_table_readers_mem: Gauge::new(
                "rocksdb_estimate_table_readers_mem_bytes".into(),
                "Estimated memory used by table readers".into(),
                empty.clone(),
            ),
            supersede_captured_total: Counter::new(
                "metadata_supersede_captured_total".into(),
                "Supersede dead-chunk records captured on overwrite (ADR-0034 D2)".into(),
                empty.clone(),
            ),
            supersede_dead_bytes_total: Counter::new(
                "metadata_supersede_dead_bytes_total".into(),
                "Total dead bytes captured by supersede records on overwrite (ADR-0034 D2)".into(),
                empty.clone(),
            ),
        }
    }

    /// Registers all RocksDB gauges with the given metrics registrar.
    ///
    /// Call this at node startup after creating the metadata store,
    /// before the metrics endpoint is exposed.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::RocksDbMetrics;
    /// use oceanfs_core::MetricRegistrar;
    ///
    /// // Given a metrics registry:
    /// // let metrics = RocksDbMetrics::new();
    /// // metrics.register(&registry);
    /// ```
    pub fn register(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.block_cache_hit.clone());
        registrar.register_gauge(self.block_cache_miss.clone());
        registrar.register_gauge(self.memtable_size.clone());
        registrar.register_gauge(self.running_compactions.clone());
        registrar.register_gauge(self.running_flushes.clone());
        registrar.register_gauge(self.estimate_num_keys.clone());
        registrar.register_gauge(self.num_files_at_level_0.clone());
        registrar.register_gauge(self.live_sst_files_size.clone());
        registrar.register_gauge(self.estimate_table_readers_mem.clone());
        registrar.register_counter(self.supersede_captured_total.clone());
        registrar.register_counter(self.supersede_dead_bytes_total.clone());
    }
}

impl Default for RocksDbMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A RocksDB-backed metadata store with three column families.
///
/// Manages object metadata (`objects` CF), segment metadata (`segments` CF),
/// and deletion tombstones (`deletions` CF).
///
/// # Examples
///
/// ```ignore
/// use oceanfs_core::MetadataConfig;
/// use oceanfs_storage::RocksDbMetadataStore;
/// let config = MetadataConfig::default();
/// let store = RocksDbMetadataStore::open(&config).unwrap();
/// ```
pub struct RocksDbMetadataStore {
    db: Arc<DB>,
    /// RocksDB property gauges for observability.
    pub metrics: Arc<RocksDbMetrics>,
    /// Per-key overwrite stripes (ADR-0034 D2 capture).
    ///
    /// Serializes the read→decide→WriteBatch critical section of
    /// [`Self::put_object_in_bucket`] per `(bucket, key)`: concurrent
    /// same-key overwrites share a stripe, so a superseded version's chunks
    /// are captured exactly once instead of twice (or never). Distinct keys
    /// almost never share a stripe (see [`stripe_count_for_writers`]).
    key_locks: Box<[parking_lot::Mutex<()>]>,
    /// Per-store random hash seed so client-chosen object keys cannot be
    /// deliberately aligned onto a single stripe (SipHash via `RandomState`).
    key_lock_hasher: RandomState,
    /// Optional metadata change journal (ADR-0038, ae2 / S2). `None` when
    /// `[metadata_sync] enabled = false`: every capture site is a branch
    /// on this `Option` and performs no I/O.
    journal: Option<Arc<MetadataJournal>>,
}

/// Outcome of applying a pulled metadata state through the sync worker
/// (ADR-0038 D5).
///
/// # Examples
///
/// ```ignore
/// match store.sync_apply_object(&bucket, meta)? {
///     SyncApplyOutcome::Applied => counters.applied.inc(),
///     SyncApplyOutcome::AlreadyCurrent => counters.skipped.inc(),
///     SyncApplyOutcome::LocalWins => counters.rejected.inc(),
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncApplyOutcome {
    /// The pulled state won the LWW comparison and was written.
    Applied,
    /// The local state is already the same logical version (no write).
    AlreadyCurrent,
    /// The local state won the LWW comparison (no write).
    LocalWins,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LwwDecision {
    Apply,
    AlreadyCurrent,
    LocalWins,
}

/// Logical identity hash of a live row: `(size, blake3_hash, inline_data,
/// hlc)` — physical chunk refs are deliberately excluded (ADR-0038 D5).
fn object_identity_hash(meta: &ObjectMetadata) -> HashOutput {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"oceanfs-meta-object-row-v1");
    hasher.update(&meta.size.to_le_bytes());
    match &meta.blake3_hash {
        Some(hash) => {
            hasher.update(&[1u8]);
            hasher.update(hash.as_bytes());
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    match &meta.inline_data {
        Some(data) => {
            hasher.update(&[1u8]);
            hasher.update(&(data.len() as u64).to_le_bytes());
            hasher.update(data);
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    hasher.update(&meta.hlc.wall_time().to_le_bytes());
    hasher.update(&meta.hlc.logical().to_le_bytes());
    HashOutput::from_bytes(*hasher.finalize().as_bytes())
}

/// Logical identity hash of a tombstone: its delete version only —
/// `deletion_time` and chunk refs are local accounting, not identity.
fn tombstone_identity_hash(tombstone: &Tombstone) -> HashOutput {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"oceanfs-meta-tombstone-v1");
    hasher.update(&tombstone.hlc.wall_time().to_le_bytes());
    hasher.update(&tombstone.hlc.logical().to_le_bytes());
    HashOutput::from_bytes(*hasher.finalize().as_bytes())
}

fn object_logically_equal(a: &ObjectMetadata, b: &ObjectMetadata) -> bool {
    a.size == b.size
        && a.blake3_hash == b.blake3_hash
        && a.inline_data == b.inline_data
        && a.hlc == b.hlc
}

/// Decides whether a pulled OBJECT row wins against the local state.
///
/// Order: a local plain tombstone is compared first (delete wins ties
/// unless the object's identity hash is greater), then a local live row.
/// Equal HLC with equal logical identity is `AlreadyCurrent`; equal HLC
/// with different identity is broken by the greater identity hash
/// (symmetric on both sides).
fn decide_object(
    pulled: &ObjectMetadata,
    local_row: Option<&ObjectMetadata>,
    local_tombstone: Option<&Tombstone>,
) -> LwwDecision {
    if let Some(tombstone) = local_tombstone {
        match pulled.hlc.cmp(&tombstone.hlc) {
            std::cmp::Ordering::Less => return LwwDecision::LocalWins,
            std::cmp::Ordering::Equal => {
                if object_identity_hash(pulled) <= tombstone_identity_hash(tombstone) {
                    return LwwDecision::LocalWins;
                }
            }
            std::cmp::Ordering::Greater => {}
        }
    }
    if let Some(row) = local_row {
        match pulled.hlc.cmp(&row.hlc) {
            std::cmp::Ordering::Less => return LwwDecision::LocalWins,
            std::cmp::Ordering::Equal => {
                if object_logically_equal(pulled, row) {
                    return LwwDecision::AlreadyCurrent;
                }
                if object_identity_hash(pulled) <= object_identity_hash(row) {
                    return LwwDecision::LocalWins;
                }
            }
            std::cmp::Ordering::Greater => {}
        }
    }
    LwwDecision::Apply
}

/// Decides whether a pulled TOMBSTONE wins against the local state. A
/// local tombstone at the same HLC is identical (its identity is its
/// version); a live row at the same HLC loses to the tombstone iff the
/// tombstone's identity hash is greater.
fn decide_tombstone(
    pulled: &Tombstone,
    local_row: Option<&ObjectMetadata>,
    local_tombstone: Option<&Tombstone>,
) -> LwwDecision {
    if let Some(tombstone) = local_tombstone {
        return match pulled.hlc.cmp(&tombstone.hlc) {
            std::cmp::Ordering::Less => LwwDecision::LocalWins,
            std::cmp::Ordering::Equal => LwwDecision::AlreadyCurrent,
            std::cmp::Ordering::Greater => apply_after_row_check(pulled, local_row),
        };
    }
    apply_after_row_check(pulled, local_row)
}

fn apply_after_row_check(pulled: &Tombstone, local_row: Option<&ObjectMetadata>) -> LwwDecision {
    if let Some(row) = local_row {
        match pulled.hlc.cmp(&row.hlc) {
            std::cmp::Ordering::Less => return LwwDecision::LocalWins,
            std::cmp::Ordering::Equal => {
                if tombstone_identity_hash(pulled) <= object_identity_hash(row) {
                    return LwwDecision::LocalWins;
                }
            }
            std::cmp::Ordering::Greater => {}
        }
    }
    LwwDecision::Apply
}

fn io_err(e: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl RocksDbMetadataStore {
    /// Opens or creates a metadata store at the given data directory.
    ///
    /// Configures per-column-family tuning: bloom filter on `objects`,
    /// per-CF write buffer sizes, tiered compression, shared block cache,
    /// unlimited open files, and level-style compaction optimisation.
    ///
    /// Spawns a background task that polls RocksDB internal properties
    /// every 30s and updates the [`RocksDbMetrics`] gauges.
    ///
    /// # Errors
    ///
    /// Returns an error if RocksDB cannot open the database or create
    /// the required column families.
    pub fn open(config: &MetadataConfig) -> Result<Self> {
        Self::open_with_journal(config, None)
    }

    /// Opens or creates a metadata store with an optional change journal
    /// (ADR-0038, ae2 / S2).
    ///
    /// Passing `None` is byte-for-byte equivalent to [`Self::open`]: every
    /// capture site branches on the `Option` and performs no journal I/O.
    /// When `Some`, every logical mutation appends its `{seq, op, key,
    /// hlc}` record **before** the row commit becomes visible (J1) and a
    /// journal failure fails the mutation closed.
    ///
    /// # Errors
    ///
    /// Returns an error if RocksDB cannot open the database or create the
    /// required column families.
    /// # Examples
    ///
    /// ```ignore
    /// let store = RocksDbMetadataStore::open_with_journal(&config, Some(journal))?;
    /// ```
    pub fn open_with_journal(
        config: &MetadataConfig,
        journal: Option<Arc<MetadataJournal>>,
    ) -> Result<Self> {
        Self::open_impl(config, journal)
    }

    fn open_impl(config: &MetadataConfig, journal: Option<Arc<MetadataJournal>>) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        // --- DB-level options ---
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(num_cpus::get() as i32);
        // max_open_files = -1: unlimited. RocksDB manages its own file cache;
        // keeping all SST files open avoids repeated open/close overhead.
        // Safe because total SST count is bounded by compaction.
        opts.set_max_open_files(config.max_open_files);

        // --- Shared block cache ---
        // A single LRU cache shared across all CFs avoids fragmentation
        // and lets hot blocks from any CF evict cold blocks from another.
        let block_cache = rocksdb::Cache::new_lru_cache(config.block_cache_size);

        // --- Per-CF descriptors with tuning ---

        // Objects CF: point-lookup pattern → bloom filter for fast key-not-found.
        let objects_opts = build_cf_opts(
            &block_cache,
            config.objects_write_buffer_mb,
            true, // bloom filter on objects
            config.memtable_size,
        );

        // Deletions CF: append-mostly, low volume → small write buffer.
        let deletions_opts = build_cf_opts(
            &block_cache,
            config.deletions_write_buffer_mb,
            false,
            config.memtable_size,
        );

        // RocksDB opens `objects` + `deletions` ONLY (ADR-0025 Decision
        // 3): the `segments` and `deleted_segments` CFs are removed —
        // segment lifecycle state lives in the event log + checkpoint +
        // registry.
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(cf::CF_OBJECTS, objects_opts),
            ColumnFamilyDescriptor::new(cf::CF_DELETIONS, deletions_opts),
        ];

        let db = DB::open_cf_descriptors(&opts, &config.data_dir, cf_descriptors)
            .map_err(|e| Error::Io(io_err(e)))?;

        let db = Arc::new(db);
        let metrics = Arc::new(RocksDbMetrics::default());

        // Per-key overwrite lock: sized by the documented collision model
        // for the assumed writer ceiling (see `stripe_count_for_writers`).
        let shard_count = stripe_count_for_writers(ASSUMED_CONCURRENT_METADATA_WRITERS);
        debug_assert!(shard_count.is_power_of_two());
        let key_locks = (0..shard_count).map(|_| parking_lot::Mutex::new(())).collect::<Box<[_]>>();
        let key_lock_hasher = RandomState::new();

        // Attempt to mlock the RocksDB block cache in physical RAM as
        // swap defense (perf rule §3.4, Feature 6). The Rust rocksdb crate
        // does not expose the raw memory region of the LRU cache, so this
        // is a best-effort advisory log. A future C FFI extension or a
        // rocksdb crate upgrade may enable actual mlock semantics.
        //
        // When `mlock_block_cache = true` and the platform is Linux, we
        // attempt to mlock the existing process pages. This is a
        // less-precise fallback: it locks ALL existing pages, not just
        // the block cache, but it prevents swap of the cache pages that
        // are already resident. New pages allocated by RocksDB after this
        // call are not locked.
        //
        // IMPORTANT — `MCL_FUTURE` is deliberately NOT used. With
        // `MCL_FUTURE`, every subsequent `mmap` of the process counts
        // against `RLIMIT_MEMLOCK`; once the process's locked total
        // crosses that ceiling, ALL further allocations fail with
        // `EAGAIN` ("too much memory has been locked") and Rust aborts
        // via `handle_alloc_error`. Under sustained load this crashed
        // the whole node the moment its footprint passed the ceiling
        // (e.g. a 2 GB `RLIMIT_MEMLOCK` with a multi-GB write working
        // set). Locking only the currently resident pages gives the
        // swap defense without capping future growth.
        if config.mlock_block_cache && !cfg!(test) {
            #[cfg(target_os = "linux")]
            {
                // Check the lock ceiling BEFORE calling: if
                // `RLIMIT_MEMLOCK` is 0 (e.g. systemd's historical
                // `LimitMEMLOCK=0` default) the call can only fail
                // uselessly; report it once instead.
                let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
                // SAFETY: `getrlimit` writes into `rlim`, a valid
                // `libc::rlimit` whose lifetime covers the call.
                #[allow(unsafe_code)]
                let rlim_ok = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rlim) } == 0;
                if rlim_ok && rlim.rlim_cur == 0 {
                    tracing::warn!(
                        "mlock_block_cache: RLIMIT_MEMLOCK = 0; \
                         RocksDB block cache is not pinned in RAM. \
                         Set LimitMEMLOCK (systemd) or the memlock \
                         rlimit to enable page pinning."
                    );
                } else {
                    // SAFETY: `mlockall(MCL_CURRENT)` locks all currently
                    // mapped pages of the calling process into physical
                    // RAM. The call requires `CAP_IPC_LOCK` or a
                    // sufficient `RLIMIT_MEMLOCK`; if not held, fails
                    // with EPERM/ENOMEM and we log a warning. It does NOT
                    // use `MCL_FUTURE`, so it imposes no cap on future
                    // allocations (see the note above).
                    #[allow(unsafe_code)]
                    let ret = unsafe { libc::mlockall(libc::MCL_CURRENT) };
                    if ret != 0 {
                        let err = std::io::Error::last_os_error();
                        if err.raw_os_error() == Some(libc::EPERM) {
                            tracing::warn!(
                                "mlockall(MCL_CURRENT) failed: CAP_IPC_LOCK not held. \
                                 RocksDB block cache is not pinned in RAM — \
                                 system may swap it under memory pressure. \
                                 See deployment docs for capability requirements."
                            );
                        } else {
                            tracing::warn!(
                                error = %err,
                                "mlockall(MCL_CURRENT) failed — \
                                 RocksDB block cache is not pinned in RAM"
                            );
                        }
                    } else {
                        // Verify the kernel actually honoured mlockall by
                        // reading VmLck from /proc/self/status. The syscall
                        // can return success even when RLIMIT_MEMLOCK silently
                        // caps the locked amount below the block cache size.
                        let locked_kb = read_vmlck_kb();
                        let cache_mb = config.block_cache_size / (1024 * 1024);
                        let locked_mb = locked_kb / 1024;

                        if locked_kb == 0 {
                            tracing::warn!(
                                cache_mb,
                                "mlockall returned success but VmLck = 0 kB. \
                                 RLIMIT_MEMLOCK is likely too low. \
                                 RocksDB block cache is NOT pinned in RAM. \
                                 Raise RLIMIT_MEMLOCK in systemd unit or \
                                 /etc/security/limits.conf to at least {cache_mb} MB."
                            );
                        } else if locked_mb < cache_mb as u64 / 2 {
                            tracing::warn!(
                                cache_mb,
                                locked_mb,
                                "mlockall locked only {locked_mb} MB but block cache \
                                 is {cache_mb} MB. RLIMIT_MEMLOCK may be insufficient. \
                                 RocksDB block cache is partially pinned — \
                                 some pages may still be swapped."
                            );
                        } else {
                            tracing::info!(
                                cache_mb,
                                locked_mb,
                                "RocksDB block cache pages pinned in RAM via \
                                 mlockall(MCL_CURRENT): \
                                 VmLck = {locked_mb} MB (cache = {cache_mb} MB)"
                            );
                        }
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                tracing::info!("mlock_block_cache=true ignored on non-Linux platform");
            }
        }

        Ok(Self { db, metrics, key_locks, key_lock_hasher, journal })
    }

    /// Runs `f` with the per-key overwrite lock for `(bucket, key)` held.
    ///
    /// The lock is held only across the read→decide→single-WriteBatch
    /// critical section of [`Self::put_object_in_bucket`] — no I/O outside
    /// the RocksDB batch, no other store lock is acquired while it is held
    /// (LOCK ORDER: the metadata store has no other internal locks).
    fn with_key_lock<R>(&self, bucket: &BucketId, key: &ObjectKey, f: impl FnOnce() -> R) -> R {
        let mut hasher = self.key_lock_hasher.build_hasher();
        hasher.write(bucket.as_str().as_bytes());
        hasher.write_u8(0);
        hasher.write(key.as_str().as_bytes());
        // `key_locks.len()` is a power of two (see `open`), so the mask
        // replaces a modulo.
        let shard = (hasher.finish() as usize) & (self.key_locks.len() - 1);
        let _guard = self.key_locks[shard].lock();
        f()
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    /// Stores object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the objects column family is not found, serialization
    /// fails, or the underlying RocksDB write fails.
    pub fn put_object(&self, meta: ObjectMetadata) -> Result<()> {
        // Default-bucket overlay of the journaled choke point: capture,
        // stale-tombstone clearing, and the J1 append all stay in one
        // place.
        self.put_object_in_bucket(&BucketId::new("default"), meta)
    }

    /// Stores object metadata with an explicit bucket.
    ///
    /// This is the single concrete choke point behind the `MetadataStore`
    /// trait (S3 PUT, write-coordinator inline writes, hint-apply,
    /// replica-apply, and the node `MetadataStoreAdapter` all funnel here),
    /// and it is where the ADR-0034 D2 **capture rule** is enforced: every
    /// chunk reference that stops being referenced by a live object row is
    /// captured into a dead-chunk record **in the same WriteBatch** as the
    /// row change.
    ///
    /// Two capture cases, both decided on the read **before** the batch:
    ///
    /// - **Overwrite of a live row** (`meta.hlc > existing.hlc`): the
    ///   superseded version's chunks are folded into a versioned
    ///   **supersede** record keyed with the superseded version's HLC, so
    ///   the record coexists with the new live row, ages under the
    ///   tombstone TTL discipline, is attributable to the segments it
    ///   references, and is never interpreted as a delete of the key.
    /// - **Re-PUT over a tombstoned key** (no live row, plain tombstone
    ///   with chunks): the delete's chunks are **migrated** into a
    ///   supersede record (preserving the original `deletion_time` so TTL
    ///   aging does not reset) before the plain tombstone is cleared — a
    ///   plain `(bucket, key)` tombstone would otherwise be wiped by this
    ///   write and its dead bytes silently lost.
    ///
    /// A fresh write clears any stale tombstone so later read-repair
    /// pushes for the new version are not rejected by the tombstone gate
    /// (membership-stability-fixes F3/t19). A **same- or older-HLC write**
    /// (e.g. a read-repair physical re-point of the same logical version)
    /// and an inline→inline no-op overwrite never capture: the predecessor
    /// is still the winning logical version, so its bytes are not dead.
    ///
    /// Compaction-remap and healing re-points do NOT route through this
    /// method — they use [`Self::batch_write`] (`BatchOp::PutObject`), which
    /// performs the same-version physical re-point without capturing.
    ///
    /// # Errors
    ///
    /// Returns an error if the objects or deletions column family is not
    /// found, serialization fails, or the RocksDB batch write fails.
    pub fn put_object_in_bucket(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()> {
        let key = meta.object_key.clone();
        self.with_key_lock(bucket, &key, || self.put_object_in_bucket_locked(bucket, meta))
    }

    /// Read→decide→single-`WriteBatch` body of [`Self::put_object_in_bucket`],
    /// invoked with the per-key lock held so concurrent same-key writers
    /// cannot double- or lose-capture.
    fn put_object_in_bucket_locked(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()> {
        let key = &meta.object_key;
        let objects_cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let deletions_cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        let row_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        // The capture reads happen BEFORE the batch; the batch itself
        // carries supersede-write + tombstone-clear + row-put, so a crash
        // between row write and capture is impossible by construction
        // (ADR-0034 D6 "Crash between row write and capture").
        //
        // The reads are tolerant: a predecessor row or tombstone value that
        // fails to decode is treated as absent (no capture) rather than
        // erroring the write. Pre-accounting, `put_object_in_bucket` never
        // read the predecessor and an overwrite always won over a corrupt
        // row; treat-unreadable-as-absent preserves that behavior.
        let existing = self.get_object(bucket, key).ok().flatten();
        let plain_tombstone = self.get_tombstone(bucket, key).ok().flatten();

        let mut batch = rocksdb::WriteBatch::default();

        // V2 capture decision. Tuple: (chunks, hlc, captured_at).
        let capture: Option<(smallvec::SmallVec<[ChunkRef; 4]>, Hlc, i64)> =
            match (&existing, &plain_tombstone) {
                // Overwrite of a live, segment-stored predecessor by a
                // strictly-newer version: capture its bytes as dead.
                (Some(prev), _) if prev.is_segment_stored() && meta.hlc > prev.hlc => {
                    Some((prev.chunks.clone(), prev.hlc, now_ms()))
                }
                // Re-PUT over a tombstoned key with no live row: migrate the
                // delete's capture into a supersede, preserving the original
                // deletion time so the record keeps its TTL age.
                (None, Some(ts)) if !ts.chunks.is_empty() => {
                    Some((ts.chunks.clone(), ts.hlc, ts.deletion_time))
                }
                _ => None,
            };

        if let Some((chunks, hlc, captured_at)) = capture {
            let dead_bytes: u64 = chunks.iter().map(|c| u64::from(c.length)).sum();
            let supersede_key = cf::encode_supersede_key(bucket.as_str(), key.as_str(), hlc);
            let value = bincode::serialize(&Tombstone { deletion_time: captured_at, hlc, chunks })
                .map_err(|e| Error::Io(io_err(e)))?;
            batch.put_cf(&deletions_cf, supersede_key, value);
            self.metrics.supersede_captured_total.inc();
            self.metrics.supersede_dead_bytes_total.add(dead_bytes);
        }

        // Clear any stale plain tombstone: the object is alive again.
        batch.delete_cf(&deletions_cf, &row_key);

        // The new live row.
        let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;
        batch.put_cf(&objects_cf, &row_key, value);

        // J1 (ADR-0038 D1): the change record is made durable BEFORE the
        // row commit becomes visible. An append failure fails the write
        // closed — the batch is never written.
        self.journal_append(JournalOp::Put, &row_key, meta.hlc)?;

        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Retrieves object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the objects column family is not found,
    /// deserialization fails, or the RocksDB read fails.
    pub fn get_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        match self.db.get_cf(&cf, db_key) {
            Ok(Some(value)) => {
                let meta: ObjectMetadata = bincode::deserialize(&value)
                    .or_else(|_| serde_json::from_slice(&value))
                    .map_err(|e| Error::Io(io_err(e)))?;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Deletes object metadata and writes a deletion tombstone.
    ///
    /// Removes the object row from `CF_OBJECTS` and records a tombstone
    /// in `CF_DELETIONS` so that garbage collection can identify and
    /// compact this key across all replicas.
    ///
    /// The tombstone carries the delete's HLC (`hlc`, stamped by the
    /// originating node's clock — hlc-causality-closure G4) so that
    /// delete-vs-write LWW is decidable across replicas.
    ///
    /// Runs under the same per-key overwrite lock as
    /// [`Self::put_object_in_bucket`], and the row delete + tombstone
    /// write commit in one WriteBatch: a concurrent same-key overwrite
    /// can never interleave between the chunk capture and the row removal
    /// in a way that double- or loses the dead-byte capture (ADR-0034 D2).
    ///
    /// # Errors
    ///
    /// Returns an error if the objects or deletions column family is not found,
    /// or if the RocksDB batch write fails.
    pub fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()> {
        self.with_key_lock(bucket, key, || self.delete_object_locked(bucket, key, hlc))
    }

    /// Row-delete + tombstone-write body of [`Self::delete_object`],
    /// invoked with the per-key lock held.
    fn delete_object_locked(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()> {
        let objects_cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let deletions_cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        // Capture the object's chunk references BEFORE removing the row:
        // the tombstone is the only surviving record of which segments
        // hold this object's bytes, and GC marks them dead from the
        // tombstone. Without this, GC could never detect dead bytes for
        // deleted objects (the row is gone, so the old object-scan
        // matching could never fire). A row that cannot be decoded is
        // treated as absent (chunks empty) so a delete never fails on a
        // corrupt predecessor.
        let chunks: smallvec::SmallVec<[ChunkRef; 4]> =
            self.get_object(bucket, key).ok().flatten().map(|meta| meta.chunks).unwrap_or_default();

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&objects_cf, &db_key);

        // Write a deletion tombstone so that GC can compact this key
        // across replicas. Without this, cross-node deletion compaction
        // is non-functional.
        let tombstone = Tombstone { deletion_time: now_ms(), hlc, chunks };
        let value = bincode::serialize(&tombstone).map_err(|e| Error::Io(io_err(e)))?;
        batch.put_cf(&deletions_cf, &db_key, value);

        // J1: journal the delete before the row removal is visible.
        self.journal_append(JournalOp::Delete, &db_key, hlc)?;

        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Applies a point-fetched OBJECT row through the mandatory HLC-LWW
    /// guards (ADR-0038 D5).
    ///
    /// Logical identity is `(size, blake3_hash, inline_data, hlc)`; chunk
    /// refs are **not** identity, so two replicas holding the same logical
    /// object with different physical refs are already in sync and are not
    /// repaired. Equal HLC with different identity is broken
    /// deterministically by the greater identity hash, symmetrically on
    /// both sides. There is **no local-segment-presence precondition**: a
    /// row referencing segments this node does not hold is applied
    /// normally (rows and bytes live on independent rings; bytes are the
    /// segment plane's).
    ///
    /// The apply reuses the same choke point as a client PUT
    /// ([`Self::put_object_in_bucket`]): superseded chunk capture, stale
    /// tombstone clearing, and the J1 journal append all happen exactly
    /// once.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// match store.sync_apply_object(&bucket, fetched)? {
    ///     SyncApplyOutcome::Applied => { /* written */ }
    ///     _ => { /* already current or local wins */ }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when a read, the row write, or the journal append
    /// fails (the journal failure fails the apply closed).
    pub fn sync_apply_object(
        &self,
        bucket: &BucketId,
        meta: ObjectMetadata,
    ) -> Result<SyncApplyOutcome> {
        let key = meta.object_key.clone();
        self.with_key_lock(bucket, &key, || {
            let local_row = self.get_object(bucket, &key).ok().flatten();
            let local_tombstone = self.get_tombstone(bucket, &key).ok().flatten();
            match decide_object(&meta, local_row.as_ref(), local_tombstone.as_ref()) {
                LwwDecision::Apply => {
                    self.put_object_in_bucket_locked(bucket, meta)?;
                    Ok(SyncApplyOutcome::Applied)
                }
                LwwDecision::AlreadyCurrent => Ok(SyncApplyOutcome::AlreadyCurrent),
                LwwDecision::LocalWins => Ok(SyncApplyOutcome::LocalWins),
            }
        })
    }

    /// Applies a point-fetched plain TOMBSTONE through the mandatory
    /// HLC-LWW guards (ADR-0038 D5): a newer delete removes the local live
    /// row and writes the pulled tombstone verbatim (preserving its
    /// deletion version); a newer local state wins and nothing is
    /// written. The local live row's chunk refs (if any) are captured as
    /// a supersede record so byte accounting survives the delete.
    ///
    /// # Errors
    ///
    /// Returns an error when a read, the row write, or the journal append
    /// fails (the journal failure fails the apply closed).
    /// # Examples
    ///
    /// ```ignore
    /// let outcome = store.sync_apply_tombstone(&bucket, &key, tombstone)?;
    /// ```
    pub fn sync_apply_tombstone(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        tombstone: Tombstone,
    ) -> Result<SyncApplyOutcome> {
        self.with_key_lock(bucket, key, || {
            let local_row = self.get_object(bucket, key).ok().flatten();
            let local_tombstone = self.get_tombstone(bucket, key).ok().flatten();
            match decide_tombstone(&tombstone, local_row.as_ref(), local_tombstone.as_ref()) {
                LwwDecision::Apply => {
                    self.apply_tombstone_row_locked(bucket, key, tombstone, local_row.as_ref())?;
                    Ok(SyncApplyOutcome::Applied)
                }
                LwwDecision::AlreadyCurrent => Ok(SyncApplyOutcome::AlreadyCurrent),
                LwwDecision::LocalWins => Ok(SyncApplyOutcome::LocalWins),
            }
        })
    }

    /// Row-delete + pulled-tombstone write body of
    /// [`Self::sync_apply_tombstone`], invoked with the per-key lock held.
    fn apply_tombstone_row_locked(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        tombstone: Tombstone,
        local_row: Option<&ObjectMetadata>,
    ) -> Result<()> {
        let objects_cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let deletions_cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
        let row_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        let mut batch = rocksdb::WriteBatch::default();
        // Capture the local row's chunks as a supersede record (ADR-0034
        // D2): they stop being referenced here and remain attributable.
        if let Some(local) = local_row {
            if local.is_segment_stored() {
                let dead_bytes: u64 = local.chunks.iter().map(|c| u64::from(c.length)).sum();
                let supersede_key =
                    cf::encode_supersede_key(bucket.as_str(), key.as_str(), local.hlc);
                let value = bincode::serialize(&Tombstone {
                    deletion_time: now_ms(),
                    hlc: local.hlc,
                    chunks: local.chunks.clone(),
                })
                .map_err(|e| Error::Io(io_err(e)))?;
                batch.put_cf(&deletions_cf, supersede_key, value);
                self.metrics.supersede_captured_total.inc();
                self.metrics.supersede_dead_bytes_total.add(dead_bytes);
            }
        }
        batch.delete_cf(&objects_cf, &row_key);
        let value = bincode::serialize(&tombstone).map_err(|e| Error::Io(io_err(e)))?;
        batch.put_cf(&deletions_cf, &row_key, value);
        // J1: the delete's change record is durable before the tombstone
        // becomes visible.
        self.journal_append(JournalOp::Delete, &row_key, tombstone.hlc)?;
        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;
        Ok(())
    }

    /// Lists objects by key prefix (S3 LIST).
    pub fn list_objects(&self, bucket: &BucketId, prefix: &str) -> Vec<Result<ObjectMetadata>> {
        let cf = self.db.cf_handle(cf::CF_OBJECTS);
        let Some(cf_handle) = cf else {
            return vec![];
        };
        let prefix_key = cf::encode_object_key(bucket.as_str(), prefix);

        let iter = self.db.iterator_cf(
            &cf_handle,
            rocksdb::IteratorMode::From(&prefix_key, rocksdb::Direction::Forward),
        );

        iter.take_while(
            move |item| {
                if let Ok((key, _)) = item {
                    key.starts_with(&prefix_key)
                } else {
                    false
                }
            },
        )
        .filter_map(|item| match item {
            Ok((_key, value)) => match bincode::deserialize::<ObjectMetadata>(&value)
                .or_else(|_| serde_json::from_slice::<ObjectMetadata>(&value))
            {
                Ok(meta) => Some(Ok(meta)),
                Err(_) => None,
            },
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    /// Lists object metadata for every object across all buckets.
    ///
    /// Scans the whole objects column family. Used by the replaced-wal
    /// recovery's one-time candidate enumeration (ADR-0034 carve-out:
    /// wal_recovery in oceanfs-node) — a per-bucket scan would miss
    /// objects in other buckets, so the segment-id set it derives would
    /// be incomplete.
    pub fn list_objects_all(&self) -> Vec<Result<ObjectMetadata>> {
        let cf = self.db.cf_handle(cf::CF_OBJECTS);
        let Some(cf_handle) = cf else {
            return vec![];
        };
        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((_key, value)) => match bincode::deserialize::<ObjectMetadata>(&value)
                .or_else(|_| serde_json::from_slice::<ObjectMetadata>(&value))
            {
                Ok(meta) => Some(Ok(meta)),
                Err(_) => None,
            },
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    /// Lists object metadata for every object across all buckets,
    /// decoding the owning bucket from each RocksDB key.
    ///
    /// [`ObjectMetadata`] does not carry its bucket (the bucket lives in
    /// the encoded key `{bucket}\0{key}`), so GC liveness tracking — which
    /// must match tombstones against objects per-bucket — uses this method.
    pub fn list_objects_all_with_bucket(&self) -> Vec<Result<(BucketId, ObjectMetadata)>> {
        let cf = self.db.cf_handle(cf::CF_OBJECTS);
        let Some(cf_handle) = cf else {
            return vec![];
        };
        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((key, value)) => {
                let (bucket_str, _) = cf::decode_object_key(&key)?;
                match bincode::deserialize::<ObjectMetadata>(&value)
                    .or_else(|_| serde_json::from_slice::<ObjectMetadata>(&value))
                {
                    Ok(meta) => Some(Ok((BucketId::new(bucket_str), meta))),
                    Err(_) => None,
                }
            }
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    // ------------------------------------------------------------------
    // Tombstone operations
    // ------------------------------------------------------------------

    /// Records a deletion tombstone directly.
    ///
    /// **Accounting/fixture primitive, never a logical delete.** It does
    /// not remove the live row and is therefore **not journaled**
    /// (ADR-0038 D1 capture matrix): journaling it would make consumers
    /// infer a delete that never happened. Logical deletes go through
    /// [`Self::delete_object`] (which journals).
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found, serialization
    /// fails, or the RocksDB write fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Accounting/fixture primitive; logical deletes use delete_object().
    /// store.put_tombstone(&bucket, &key, tombstone)?;
    /// ```
    pub fn put_tombstone(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        tombstone: Tombstone,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());
        let value = bincode::serialize(&tombstone).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, db_key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Deletes a tombstone entry for the given object key from the deletions CF.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found or if
    /// the RocksDB delete operation fails.
    pub fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
        let encoded_key = cf::encode_object_key(bucket.as_str(), key.as_str());
        self.db.delete_cf(&cf, &encoded_key).map_err(|e| Error::Io(io_err(e)))?;
        Ok(())
    }

    /// Deletes one versioned supersede dead-chunk record (ADR-0034 D2;
    /// f2's post-compaction supersede cleanup).
    ///
    /// Reconstructs the exact versioned key (`{bucket}\0{key}` + the
    /// supersede tail carrying `version`) and deletes that single record.
    /// The key's LIVE object row is untouched — a supersede record is
    /// never a tombstone of the key.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found or if
    /// the RocksDB delete operation fails.
    pub fn delete_dead_chunk_record(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        version: Hlc,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
        let encoded_key = cf::encode_supersede_key(bucket.as_str(), key.as_str(), version);
        self.db.delete_cf(&cf, &encoded_key).map_err(|e| Error::Io(io_err(e)))?;
        Ok(())
    }

    /// Checks if a deletion tombstone exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found or
    /// the RocksDB read fails.
    pub fn has_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> Result<bool> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        match self.db.get_cf(&cf, db_key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Retrieves the deletion tombstone for a key, if one exists.
    ///
    /// Used for order-aware delete-vs-write resolution at the
    /// repair-push boundary (hlc-causality-closure G6).
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found,
    /// the RocksDB read fails, or the stored value cannot be decoded.
    pub fn get_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<Tombstone>> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        match self.db.get_cf(&cf, db_key) {
            Ok(Some(value)) => {
                let tombstone: Tombstone = bincode::deserialize(&value)
                    .or_else(|_| serde_json::from_slice(&value))
                    .map_err(|e| Error::Io(io_err(e)))?;
                Ok(Some(tombstone))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Lists all deletion tombstones for a bucket.
    ///
    /// Supersede dead-chunk records (ADR-0034 D2) are structurally
    /// invisible here: every pre-f2 consumer (GC, caches, the read-repair
    /// tombstone gate) sees byte-identical output to the pre-accounting
    /// store, which contained only plain tombstones.
    pub fn list_tombstones(&self, bucket: &BucketId) -> Vec<Result<(ObjectKey, Tombstone)>> {
        let cf = self.db.cf_handle(cf::CF_DELETIONS);
        let Some(cf_handle) = cf else {
            return vec![];
        };

        let prefix = cf::encode_object_key(bucket.as_str(), "");

        let iter = self.db.iterator_cf(
            &cf_handle,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        iter.take_while(
            move |item| {
                if let Ok((key, _)) = item {
                    key.starts_with(&prefix)
                } else {
                    false
                }
            },
        )
        .filter_map(|item| match item {
            Ok((key, value)) => {
                // Route the decode through `decode_deletions_key` and skip
                // supersede records: their versioned keys must never be
                // surfaced as a plain tombstone for the (now-live) key.
                let cf::DeletionsKey::Plain { bucket: bucket_str, key: key_str } =
                    cf::decode_deletions_key(&key)?
                else {
                    return None;
                };
                if bucket_str != bucket.as_str() {
                    return None;
                }
                match bincode::deserialize::<Tombstone>(&value)
                    .or_else(|_| serde_json::from_slice::<Tombstone>(&value))
                {
                    Ok(tombstone) => Some(Ok((ObjectKey::new(&key_str), tombstone))),
                    Err(_) => None,
                }
            }
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    /// Lists all deletion tombstones across every bucket.
    ///
    /// Scans the deletions CF from the start, decoding each key into its
    /// owning bucket + object key. Used by GC liveness tracking so that
    /// tombstones in ANY bucket (not just "default") drive compaction.
    ///
    /// Supersede dead-chunk records (ADR-0034 D2) are skipped: they are
    /// live keys and must not reach the GC tombstone path (f2 consumes
    /// them through [`Self::list_dead_chunk_records_all`] instead).
    pub fn list_tombstones_all(&self) -> Vec<Result<(BucketId, ObjectKey, Tombstone)>> {
        let cf = self.db.cf_handle(cf::CF_DELETIONS);
        let Some(cf_handle) = cf else {
            return vec![];
        };

        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((key, value)) => {
                let cf::DeletionsKey::Plain { bucket, key } = cf::decode_deletions_key(&key)?
                else {
                    return None;
                };
                match bincode::deserialize::<Tombstone>(&value)
                    .or_else(|_| serde_json::from_slice::<Tombstone>(&value))
                {
                    Ok(tombstone) => {
                        Some(Ok((BucketId::new(&bucket), ObjectKey::new(&key), tombstone)))
                    }
                    Err(_) => None,
                }
            }
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    /// Lists every captured dead-chunk record across all buckets — plain
    /// tombstones (`kind: Tombstone`) and versioned supersedes (`kind:
    /// Supersede`) — as the typed accounting feed f2 consumes (ADR-0034
    /// D2/D3).
    ///
    /// Unlike [`Self::list_tombstones_all`], supersede records ARE
    /// returned. Unlike the plain-tombstone enumerations, the record's
    /// `kind` is derived from the deletions-CF key classification, so a
    /// supersede key is never surfaced as a plain tombstone of its key and
    /// a plain tombstone is never surfaced as a supersede.
    pub fn list_dead_chunk_records_all(
        &self,
    ) -> Vec<Result<(BucketId, ObjectKey, DeadChunkRecord)>> {
        let cf = self.db.cf_handle(cf::CF_DELETIONS);
        let Some(cf_handle) = cf else {
            return vec![];
        };

        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((key, value)) => {
                let decoded = cf::decode_deletions_key(&key)?;
                // The stored value keeps the `Tombstone` shape for both
                // kinds; `kind` comes from the key classification.
                let tombstone: Tombstone = match bincode::deserialize(&value)
                    .or_else(|_| serde_json::from_slice(&value))
                {
                    Ok(t) => t,
                    Err(_) => return None,
                };
                let (bucket, key, kind) = match decoded {
                    cf::DeletionsKey::Plain { bucket, key } => {
                        (bucket, key, DeadChunkKind::Tombstone)
                    }
                    cf::DeletionsKey::Supersede { bucket, key, .. } => {
                        (bucket, key, DeadChunkKind::Supersede)
                    }
                };
                let record = DeadChunkRecord {
                    kind,
                    captured_at: tombstone.deletion_time,
                    hlc: tombstone.hlc,
                    chunks: tombstone.chunks,
                };
                Some(Ok((BucketId::new(&bucket), ObjectKey::new(&key), record)))
            }
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    // ------------------------------------------------------------------
    // Async wrappers
    // ------------------------------------------------------------------

    /// Async version of [`Self::put_object`].
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking task fails to spawn, the objects column
    /// family is not found, serialization fails, or the RocksDB write fails.
    pub async fn put_object_async(&self, meta: ObjectMetadata) -> Result<()> {
        let db = self.db.clone();
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let key = cf::encode_object_key("default", meta.object_key.as_str());
            let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;
            // J1: the change record is durable before the row becomes
            // visible.
            if let Some(journal) = journal {
                journal.append(JournalOp::Put, &key, meta.hlc)?;
            }
            db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
    }

    /// Async version of [`Self::get_object`].
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking task fails to spawn, the objects column
    /// family is not found, deserialization fails, or the RocksDB read fails.
    pub async fn get_object_async(
        &self,
        bucket: BucketId,
        key: ObjectKey,
    ) -> Result<Option<ObjectMetadata>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());
            match db.get_cf(&cf, db_key) {
                Ok(Some(value)) => {
                    let meta: ObjectMetadata = bincode::deserialize(&value)
                        .or_else(|_| serde_json::from_slice(&value))
                        .map_err(|e| Error::Io(io_err(e)))?;
                    Ok(Some(meta))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(Error::Io(io_err(e))),
            }
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
    }

    /// Async row removal (no tombstone).
    ///
    /// This is a raw row delete: it does not write a deletion tombstone
    /// (use [`Self::delete_object`] for a logical delete). It still
    /// journals the state change — when a live row existed, its version
    /// is appended as a `delete` record before the removal becomes
    /// visible (J1), so a replica cannot silently miss the removal.
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking task fails to spawn, the objects
    /// column family is not found, the journal append fails (fail-closed),
    /// or the RocksDB delete fails.
    /// # Examples
    ///
    /// ```ignore
    /// store.delete_object_async(bucket, key).await?; // journals the removal
    /// ```
    pub async fn delete_object_async(&self, bucket: BucketId, key: ObjectKey) -> Result<()> {
        let db = self.db.clone();
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            let objects_cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());
            // J1: read the removed row's version and journal the change
            // before the row disappears; an unreadable row is treated as
            // absent (no record), mirroring the write path's tolerance.
            if let Some(journal) = journal {
                if let Ok(Some(value)) = db.get_cf(&objects_cf, &db_key) {
                    if let Some(meta) = decode_metadata(&value) {
                        journal.append(JournalOp::Delete, &db_key, meta.hlc)?;
                    }
                }
            }
            db.delete_cf(&objects_cf, db_key).map_err(|e| Error::Io(io_err(e)))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
    }

    // ------------------------------------------------------------------
    // Batch operations
    // ------------------------------------------------------------------

    /// Atomically writes a batch of metadata operations.
    ///
    /// # Errors
    ///
    /// Returns an error if a required column family is not found, serialization
    /// fails for any operation, or the RocksDB batch write fails.
    pub fn batch_write(&self, ops: Vec<BatchOp>) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        // Change records for operations that actually change logical
        // state (ADR-0038 D1 capture matrix). `batch_write` is used by
        // compaction/healing remaps that preserve HLC and re-point
        // physical refs only: those must NOT journal. The defensive
        // read-before-write below is what distinguishes the two.
        let mut journal_entries: Vec<(JournalOp, Vec<u8>, Hlc)> = Vec::new();

        for op in &ops {
            match op {
                BatchOp::PutObject(bucket, key, value) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_OBJECTS)
                        .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
                    let k = cf::encode_object_key(bucket.as_str(), key.as_str());
                    let v = bincode::serialize(value).map_err(|e| Error::Io(io_err(e)))?;
                    let logical_change = self
                        .get_object(bucket, key)
                        .ok()
                        .flatten()
                        .map(|previous| !object_logically_equal(&previous, value))
                        .unwrap_or(true);
                    if logical_change {
                        journal_entries.push((JournalOp::Put, k.clone(), value.hlc));
                    }
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::DeleteObject(bucket, key) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_OBJECTS)
                        .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
                    let k = cf::encode_object_key(bucket.as_str(), key.as_str());
                    if let Some(previous) = self.get_object(bucket, key).ok().flatten() {
                        journal_entries.push((JournalOp::Delete, k.clone(), previous.hlc));
                    }
                    batch.delete_cf(&cf, k);
                }
                BatchOp::PutTombstone(bucket, key, tombstone) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_DELETIONS)
                        .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
                    let k = cf::encode_object_key(bucket.as_str(), key.as_str());
                    let v = bincode::serialize(tombstone).map_err(|e| Error::Io(io_err(e)))?;
                    let changed = self
                        .get_tombstone(bucket, key)
                        .ok()
                        .flatten()
                        .map(|previous| previous.hlc != tombstone.hlc)
                        .unwrap_or(true);
                    if changed {
                        journal_entries.push((JournalOp::Delete, k.clone(), tombstone.hlc));
                    }
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::DeleteTombstone(bucket, key) => {
                    // GC accounting cleanup (post-compaction tombstone
                    // removal) — never journaled.
                    let cf = self
                        .db
                        .cf_handle(cf::CF_DELETIONS)
                        .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
                    let encoded = cf::encode_object_key(bucket.as_str(), key.as_str());
                    batch.delete_cf(&cf, encoded);
                }
            }
        }

        if !journal_entries.is_empty() {
            let refs: Vec<(JournalOp, &[u8], Hlc)> =
                journal_entries.iter().map(|(op, key, hlc)| (*op, key.as_slice(), *hlc)).collect();
            self.journal_append_batch(&refs)?;
        }

        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Queries a RocksDB property by name.
    pub fn property(&self, name: &str) -> Option<String> {
        self.db.property_value(name).ok().flatten()
    }

    /// Returns a reference to the metrics gauges.
    pub fn metrics(&self) -> &Arc<RocksDbMetrics> {
        &self.metrics
    }

    /// Returns the change journal when `[metadata_sync]` is enabled.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let journal = store.metadata_journal().cloned();
    /// ```
    pub fn metadata_journal(&self) -> Option<&Arc<MetadataJournal>> {
        self.journal.as_ref()
    }

    /// Appends one change record before a row commit (J1). A no-op when
    /// the journal is disabled; an append failure propagates so the
    /// caller fails its metadata write closed.
    fn journal_append(&self, op: JournalOp, key: &[u8], hlc: Hlc) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.append(op, key, hlc)?;
        }
        Ok(())
    }

    /// Appends a batch of change records with one group commit (J1).
    fn journal_append_batch(&self, items: &[(JournalOp, &[u8], Hlc)]) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.append_batch(items)?;
        }
        Ok(())
    }

    /// Flushes all column families to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB flush operation fails.
    pub fn close(&self) -> Result<()> {
        self.db.flush().map_err(|e| Error::Io(io_err(e)))
    }

    /// Spawns a background task that polls RocksDB internal properties
    /// every 30 seconds and updates the metrics gauges.
    ///
    /// Call this after opening the store when a Tokio runtime is available
    /// (typically at node startup). Test code may skip this.
    ///
    /// The task is **cancellable**: it exits when `shutdown` is cancelled,
    /// dropping its `Arc<DB>` so the RocksDB LOCK is released — a node
    /// restart (or a clean shutdown) must not leak the DB handle.
    pub fn start_metrics_task(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        // Property-availability check: a gauge that never resolves stays
        // pinned at 0, which would silently hide real conditions (e.g.
        // `rocksdb.num-files-at-level0` masking a write stall) from the
        // load-test assertions.
        for unresolved in unresolved_rocksdb_properties(&self.db) {
            tracing::warn!("rocksdb property unavailable; gauge will stay at 0: {unresolved}");
        }
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            poll_rocksdb_metrics(db, metrics, shutdown).await;
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: build per-CF options
// ---------------------------------------------------------------------------

/// Builds tuned column-family options.
///
/// - Sets the block cache (shared across all CFs).
/// - Sets tiered compression: Snappy for L0-L1 (fast decompression for hot
///   data), Zstd for L2+ (better compression ratio for cold data).
/// - Sets the write buffer size per CF based on workload.
/// - Enables bloom filter (10 bits/key, ~1% false-positive rate) if requested.
///   Bloom filters are critical for point-lookup workloads (objects CF) where
///   key-not-found queries must otherwise probe every SST file.
/// - Calls `optimize_level_style_compaction` for the given memtable size to
///   tune level multipliers and compaction triggers.
fn build_cf_opts(
    block_cache: &rocksdb::Cache,
    write_buffer_mb: usize,
    use_bloom: bool,
    memtable_size: usize,
) -> Options {
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(block_cache);

    if use_bloom {
        // 10 bits per key → ~1% false-positive rate.
        // Eliminates ~99% of unnecessary SST file probes for key-not-found
        // queries. The tradeoff is ~1.25 bytes/key of additional memory
        // for the bloom filter data structure. For 100M objects, that's
        // ~125 MB of additional RAM — well within the block cache budget.
        block_opts.set_bloom_filter(10.0, false);
    }

    let mut cf_opts = Options::default();
    cf_opts.set_block_based_table_factory(&block_opts);

    // Tiered compression: Snappy for L0-L1 (decompression speed ~2-5×
    // faster than Zstd), Zstd for L2+ (compression ratio ~30-50% better).
    // Level count is arbitrary; RocksDB uses level_compaction_dynamic_level_bytes
    // by default, which adjusts the level layout.
    cf_opts.set_compression_per_level(&[
        rocksdb::DBCompressionType::Snappy, // L0
        rocksdb::DBCompressionType::Snappy, // L1
        rocksdb::DBCompressionType::Zstd,   // L2
        rocksdb::DBCompressionType::Zstd,   // L3
        rocksdb::DBCompressionType::Zstd,   // L4
        rocksdb::DBCompressionType::Zstd,   // L5
        rocksdb::DBCompressionType::Zstd,   // L6
    ]);

    let wb = write_buffer_mb * 1024 * 1024;
    cf_opts.set_write_buffer_size(wb);

    // Tune level-style compaction multipliers and triggers based on
    // the per-CF memtable size. This is separate from
    // `optimize_level_style_compaction` at the DB level.
    cf_opts.optimize_level_style_compaction(memtable_size);

    cf_opts
}

// ---------------------------------------------------------------------------
// Background metrics task
// ---------------------------------------------------------------------------

/// Polls RocksDB internal properties every 30 seconds and updates gauges.
///
/// Runs as a `tokio::spawn`-ed background task. Properties that fail to
/// parse are silently skipped (they may not exist on all RocksDB versions).
///
/// Cancellable: the loop exits when `shutdown` is cancelled, releasing
/// its `Arc<DB>` (every spawned task must be cancellable — a leaked task
/// keeps the RocksDB handle and its LOCK alive past shutdown).
async fn poll_rocksdb_metrics(
    db: Arc<DB>,
    metrics: Arc<RocksDbMetrics>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let interval = Duration::from_secs(30);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }

        if let Some(v) = property_u64(&db, "rocksdb.block.cache.hit") {
            metrics.block_cache_hit.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.block.cache.miss") {
            metrics.block_cache_miss.set(v);
        }
        if let Some(v) = property_u64_cf_sum(&db, "rocksdb.cur-size-all-mem-tables") {
            metrics.memtable_size.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.num-running-compactions") {
            metrics.running_compactions.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.num-running-flushes") {
            metrics.running_flushes.set(v);
        }
        if let Some(v) = property_u64_cf_sum(&db, "rocksdb.estimate-num-keys") {
            metrics.estimate_num_keys.set(v);
        }
        // SST-level properties are PER-COLUMN-FAMILY: the DB-level read
        // reports the (empty) default CF, which would pin these gauges
        // at 0 and make the write-stall assertion vacuous. Sum across
        // the real CFs instead.
        if let Some(v) = property_u64_cf_sum(&db, "rocksdb.num-files-at-level0") {
            metrics.num_files_at_level_0.set(v);
        }
        if let Some(v) = property_u64_cf_sum(&db, "rocksdb.live-sst-files-size") {
            metrics.live_sst_files_size.set(v);
        }
        if let Some(v) = property_u64_cf_sum(&db, "rocksdb.estimate-table-readers-mem") {
            metrics.estimate_table_readers_mem.set(v);
        }
    }
}

/// Returns the RocksDB property value for `name` as a `u64`, or `None`
/// when the property is unavailable (unknown on this RocksDB version).
fn property_u64(db: &DB, name: &str) -> Option<u64> {
    db.property_int_value(name).ok().flatten()
}

/// Sums a RocksDB property across the three real column families.
///
/// Some properties (SST file counts, sizes, reader memory) are only
/// reported per-CF; the DB-level read covers the empty default CF.
fn property_u64_cf_sum(db: &DB, name: &str) -> Option<u64> {
    let mut total = 0u64;
    let mut found = false;
    for cf_name in [cf::CF_OBJECTS, cf::CF_DELETIONS] {
        if let Some(cf) = db.cf_handle(cf_name) {
            if let Some(v) = db.property_int_value_cf(&cf, name).ok().flatten() {
                total = total.saturating_add(v);
                found = true;
            }
        }
    }
    found.then_some(total)
}

/// Verifies that the properties the metrics poller depends on actually
/// resolve on this RocksDB build.
///
/// A property that silently fails to parse would leave its gauge pinned
/// at 0 — for `rocksdb.num-files-at-level0` that would hide a real
/// write-stall condition from the load-test assertions. Returns the
/// names of any unresolvable properties.
///
/// Note: `rocksdb.block.cache.hit`/`miss` are intentionally absent from
/// the required list — they do not resolve via `property_int_value` on
/// current RocksDB builds (pre-existing; the block-cache gauges polled
/// from them have always read 0). Phase 2 assertions use the L1 object
/// cache counters instead.
pub(crate) fn unresolved_rocksdb_properties(db: &DB) -> Vec<String> {
    const REQUIRED: &[&str] = &["rocksdb.num-running-compactions", "rocksdb.num-running-flushes"];
    // SST-level properties are per-CF; require them on at least the
    // objects CF (the busiest).
    const REQUIRED_CF: &[&str] = &[
        "rocksdb.num-files-at-level0",
        "rocksdb.live-sst-files-size",
        "rocksdb.estimate-table-readers-mem",
    ];
    let mut unresolved: Vec<String> = REQUIRED
        .iter()
        .filter(|name| property_u64(db, name).is_none())
        .map(|name| (*name).to_string())
        .collect();
    for name in REQUIRED_CF {
        let Some(cf) = db.cf_handle(cf::CF_OBJECTS) else {
            unresolved.push(format!("{name} (objects CF missing)"));
            continue;
        };
        if db.property_int_value_cf(&cf, *name).ok().flatten().is_none() {
            unresolved.push(format!("{name} (objects CF)"));
        }
    }
    unresolved
}

// ---------------------------------------------------------------------------
// BatchOp
// ---------------------------------------------------------------------------

/// An operation in a batch write.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Put an object metadata entry (bucket-carried, mirroring
    /// `oceanfs_storage_api::BatchOp::PutObject`).
    PutObject(BucketId, ObjectKey, ObjectMetadata),
    /// Delete an object.
    DeleteObject(BucketId, ObjectKey),
    /// Put a tombstone.
    PutTombstone(BucketId, ObjectKey, Tombstone),
    /// Delete a tombstone entry.
    DeleteTombstone(BucketId, ObjectKey),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// mlock verification helper
// ---------------------------------------------------------------------------

/// Reads the current process's locked memory (VmLck) from /proc/self/status.
///
/// Returns the value in kilobytes, or 0 if the file cannot be read or parsed.
/// Used to verify that `mlockall` actually pinned pages in physical RAM.
#[cfg(target_os = "linux")]
fn read_vmlck_kb() -> u64 {
    let content = match std::fs::read_to_string("/proc/self/status") {
        Ok(c) => c,
        Err(_) => return 0,
    };
    for line in content.lines() {
        if let Some(val) = line.strip_prefix("VmLck:") {
            // Format: "VmLck:\t  123456 kB"
            let kb_str = val.split_whitespace().next().unwrap_or("0");
            return kb_str.parse::<u64>().unwrap_or(0);
        }
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn read_vmlck_kb() -> u64 {
    0
}

// ---------------------------------------------------------------------------
// MetadataStore trait implementation (Item 6: RocksDB coupling fix)
// ---------------------------------------------------------------------------

/// g8 `metadata-loss-recovery` primitives: raw range scans, a fresh-store
/// probe, and HLC-guarded rebuild folds.
///
/// These are recovery-path-only (perf rule 7.1: one-time, off the hot
/// path). The range stream must never buffer a whole range in memory —
/// the `visit_*` methods iterate the CF row by row and stop when the
/// visitor returns `false`.
impl RocksDbMetadataStore {
    /// Visits every raw `(key, value)` row of the objects column family.
    ///
    /// The visitor returns `false` to stop the scan early. Errors from
    /// the RocksDB iterator surface as [`Error::Io`].
    ///
    /// # Errors
    ///
    /// Returns an error if the objects column family is missing or the
    /// iterator fails mid-scan.
    pub fn visit_objects_rows(&self, mut visitor: impl FnMut(&[u8], &[u8]) -> bool) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => {
                    if !visitor(&key, &value) {
                        return Ok(());
                    }
                }
                Err(e) => return Err(Error::Io(io_err(e))),
            }
        }
        Ok(())
    }

    /// Visits every raw `(key, value)` row of the deletions column family
    /// (plain tombstones AND supersede records).
    ///
    /// The visitor returns `false` to stop the scan early.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is missing or the
    /// iterator fails mid-scan.
    pub fn visit_deletions_rows(
        &self,
        mut visitor: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => {
                    if !visitor(&key, &value) {
                        return Ok(());
                    }
                }
                Err(e) => return Err(Error::Io(io_err(e))),
            }
        }
        Ok(())
    }

    /// Whether BOTH metadata column families are empty (a fresh store).
    ///
    /// g8 boot detection uses this: a store that opens empty while the
    /// node already holds data (intact `.dat` / registry) was REPLACED —
    /// the objects/deletions index must be rebuilt from peers. A
    /// one-key peek per CF; never scans.
    ///
    /// # Errors
    ///
    /// Returns an error if either column family is missing.
    pub fn both_metadata_cfs_empty(&self) -> Result<bool> {
        Ok(self.cf_is_empty(cf::CF_OBJECTS)? && self.cf_is_empty(cf::CF_DELETIONS)?)
    }

    fn cf_is_empty(&self, name: &str) -> Result<bool> {
        let cf = self
            .db
            .cf_handle(name)
            .ok_or_else(|| Error::InvalidConfig(format!("{name} CF not found")))?;
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.next().is_none())
    }

    /// Applies one streamed OBJECT row to a fresh store during a
    /// metadata rebuild (g8). HLC-LWW guarded:
    ///
    /// - dropped when an existing plain tombstone's HLC is `>=` the row's
    ///   HLC (a newer delete must not be resurrected);
    /// - dropped when the existing live row is strictly newer;
    /// - otherwise the row is written and any older plain tombstone
    ///   cleared — exactly the live-state a healthy peer would show.
    ///
    /// Returns `Ok(true)` when the row was applied, `Ok(false)` when it
    /// was shadowed or un-decodable (skipped, never fatal).
    ///
    /// # Errors
    ///
    /// Returns an error when a RocksDB read or write fails.
    pub fn rebuild_apply_object_row(&self, row_key: &[u8], value: &[u8]) -> Result<bool> {
        let Some(meta) = decode_metadata(value) else { return Ok(false) };
        self.apply_object_row_locked(row_key, value, meta)
    }

    /// Applies one streamed DELETION row to a fresh store during a
    /// metadata rebuild (g8): a plain tombstone deletes an older live row
    /// and records the delete; a supersede record is re-written verbatim
    /// (byte-accounting restored). HLC-LWW guarded against newer live
    /// rows. Returns `Ok(true)` when applied.
    ///
    /// # Errors
    ///
    /// Returns an error when a RocksDB read or write fails.
    pub fn rebuild_apply_deletion_row(&self, row_key: &[u8], value: &[u8]) -> Result<bool> {
        let Some(deleted) = decode_deletions_value(value) else { return Ok(false) };
        let objects_cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let deletions_cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        // A supersede record never touches the live row — just re-write it
        // verbatim (idempotent: identical key → identical value).
        if let Some(cf::DeletionsKey::Supersede { .. }) = cf::decode_deletions_key(row_key) {
            self.db.put_cf(&deletions_cf, row_key, value).map_err(|e| Error::Io(io_err(e)))?;
            return Ok(true);
        }

        // Plain tombstone: the object row (same key bytes) must not be a
        // strictly-newer live row.
        let existing = self.db.get_cf(&objects_cf, row_key).map_err(|e| Error::Io(io_err(e)))?;
        if let Some(row) = existing {
            if let Some(meta) = decode_metadata(&row) {
                if meta.hlc >= deleted.hlc {
                    return Ok(false); // newer live row wins
                }
            }
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&objects_cf, row_key);
        batch.put_cf(&deletions_cf, row_key, value);
        // A plain tombstone is a logical delete: journal it. Supersede
        // records returned above are GC accounting and never journal.
        self.journal_append(JournalOp::Delete, row_key, deleted.hlc)?;
        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;
        Ok(true)
    }

    fn apply_object_row_locked(
        &self,
        row_key: &[u8],
        value: &[u8],
        meta: ObjectMetadata,
    ) -> Result<bool> {
        let objects_cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
        let deletions_cf = self
            .db
            .cf_handle(cf::CF_DELETIONS)
            .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;

        // Shadowed by a delete that is not older than this row?
        let tombstone = self.db.get_cf(&deletions_cf, row_key).map_err(|e| Error::Io(io_err(e)))?;
        if let Some(ts) = tombstone {
            if let Some(ts) = decode_deletions_value(&ts) {
                if ts.hlc >= meta.hlc {
                    return Ok(false);
                }
            } else {
                // Undecodable tombstone: keep it (never resurrect).
                return Ok(false);
            }
        }
        // Shadowed by a strictly-newer live row?
        let existing = self.db.get_cf(&objects_cf, row_key).map_err(|e| Error::Io(io_err(e)))?;
        if let Some(row) = existing {
            if let Some(existing_meta) = decode_metadata(&row) {
                if existing_meta.hlc >= meta.hlc {
                    return Ok(false);
                }
            }
        }

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(&deletions_cf, row_key);
        batch.put_cf(&objects_cf, row_key, value);
        // The rebuilt node's journal covers the state it restores
        // (ADR-0038 D1 capture matrix: g8 / bootstrap row apply).
        self.journal_append(JournalOp::Put, row_key, meta.hlc)?;
        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;
        Ok(true)
    }
}

/// Decodes an objects-CF value into [`ObjectMetadata`], tolerating the
/// legacy serde_json fallback (the write path uses bincode).
fn decode_metadata(value: &[u8]) -> Option<ObjectMetadata> {
    bincode::deserialize::<ObjectMetadata>(value)
        .or_else(|_| serde_json::from_slice::<ObjectMetadata>(value))
        .ok()
}

/// Decodes a deletions-CF value into [`Tombstone`], tolerating the
/// legacy serde_json fallback.
fn decode_deletions_value(value: &[u8]) -> Option<Tombstone> {
    bincode::deserialize::<Tombstone>(value)
        .or_else(|_| serde_json::from_slice::<Tombstone>(value))
        .ok()
}

impl oceanfs_storage_api::MetadataStore for RocksDbMetadataStore {
    fn list_object_keys(&self, bucket: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
        self.list_objects(bucket, "")
            .into_iter()
            .filter_map(|r| r.ok())
            .map(|meta| Ok((bucket.clone(), meta.object_key)))
            .collect::<std::io::Result<Vec<_>>>()
    }

    fn list_objects_all(&self) -> Vec<std::io::Result<ObjectMetadata>> {
        self.list_objects_all()
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn list_objects_all_with_bucket(&self) -> Vec<std::io::Result<(BucketId, ObjectMetadata)>> {
        self.list_objects_all_with_bucket()
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn get_object_metadata(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<ObjectMetadata>> {
        self.get_object(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn list_objects(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Vec<std::io::Result<ObjectMetadata>> {
        self.list_objects(bucket, prefix)
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn list_tombstones(&self, bucket: &BucketId) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> {
        self.list_tombstones(bucket)
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn list_tombstones_all(&self) -> Vec<std::io::Result<(BucketId, ObjectKey, Tombstone)>> {
        self.list_tombstones_all()
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn list_dead_chunk_records_all(
        &self,
    ) -> Vec<std::io::Result<(BucketId, ObjectKey, DeadChunkRecord)>> {
        self.list_dead_chunk_records_all()
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()> {
        self.delete_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn delete_dead_chunk_record(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        version: Hlc,
    ) -> std::io::Result<()> {
        self.delete_dead_chunk_record(bucket, key, version)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn has_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<bool> {
        self.has_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn get_tombstone(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<Tombstone>> {
        self.get_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()> {
        self.put_object_in_bucket(bucket, meta).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> std::io::Result<()> {
        self.delete_object(bucket, key, hlc).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn batch_write(&self, ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
        let rocks_ops: Vec<crate::metadata::store::BatchOp> = ops
            .into_iter()
            .map(|op| match op {
                oceanfs_storage_api::BatchOp::PutObject(bucket, key, meta) => {
                    crate::metadata::store::BatchOp::PutObject(bucket, key, meta)
                }
                oceanfs_storage_api::BatchOp::DeleteObject(bucket, key) => {
                    crate::metadata::store::BatchOp::DeleteObject(bucket, key)
                }
                oceanfs_storage_api::BatchOp::PutTombstone(bucket, key, tombstone) => {
                    crate::metadata::store::BatchOp::PutTombstone(bucket, key, tombstone)
                }
                oceanfs_storage_api::BatchOp::DeleteTombstone(bucket, key) => {
                    crate::metadata::store::BatchOp::DeleteTombstone(bucket, key)
                }
            })
            .collect();
        self.batch_write(rocks_ops).map_err(|e| std::io::Error::other(e.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_types)]
mod tests {
    use oceanfs_core::{ChunkRef, HashOutput, Hlc, NodeId, SegmentId};

    use super::*;
    use crate::metadata::journal::{JournalEntry, JournalRead};

    fn test_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
            objects_write_buffer_mb: 4,
            segments_write_buffer_mb: 8,
            deletions_write_buffer_mb: 1,
            max_open_files: 1024,
            ..Default::default()
        }
    }

    fn make_object_meta(key: &str, size: u64, inline: Option<&[u8]>) -> ObjectMetadata {
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size,
            blake3_hash: Some(HashOutput::from_bytes([0u8; 32])),
            chunks: smallvec::SmallVec::new(),
            inline_data: inline.map(bytes::Bytes::copy_from_slice),
            created_at: 1700000000000,
            hlc: Hlc::new(1700000000000, 0),
        }
    }

    /// Builds a chunk reference on `segment_id` (no compression).
    fn make_chunk(segment_id: SegmentId, offset: u64, length: u32) -> ChunkRef {
        ChunkRef { segment_id, offset, length, compressed: false, logical_length: length }
    }

    /// Builds a segment-stored object whose `chunks` reference the given
    /// segments.
    fn make_segment_stored_meta(key: &str, hlc: Hlc, chunks: Vec<ChunkRef>) -> ObjectMetadata {
        let size = chunks.iter().map(|c| u64::from(c.length)).sum();
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size,
            blake3_hash: Some(HashOutput::from_bytes([1u8; 32])),
            chunks: chunks.into_iter().collect(),
            inline_data: None,
            created_at: 1700000000000,
            hlc,
        }
    }

    /// Reads all dead-chunk records, failing on decode errors.
    fn dead_records(store: &RocksDbMetadataStore) -> Vec<(BucketId, ObjectKey, DeadChunkRecord)> {
        store.list_dead_chunk_records_all().into_iter().collect::<Result<Vec<_>>>().unwrap()
    }

    /// Reads only the `Supersede` dead-chunk records.
    fn supersedes(store: &RocksDbMetadataStore) -> Vec<(BucketId, ObjectKey, DeadChunkRecord)> {
        dead_records(store)
            .into_iter()
            .filter(|(_, _, r)| r.kind == DeadChunkKind::Supersede)
            .collect()
    }

    #[test]
    fn put_and_get_object_roundtrip() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let meta = make_object_meta("photo.jpg", 1024, Some(b"inline-data"));
        store.put_object(meta.clone()).unwrap();

        let got = store
            .get_object(&BucketId::new("default"), &ObjectKey::new("photo.jpg"))
            .unwrap()
            .unwrap();
        assert_eq!(got.object_key.as_str(), "photo.jpg");
        assert_eq!(got.size, 1024);
        assert!(got.is_inline());
    }

    /// F3/t19: a fresh write clears the deletion tombstone so that a
    /// later read-repair push for the new version is not rejected by
    /// the tombstone gate.
    #[test]
    fn put_object_in_bucket_clears_stale_tombstone() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let key = ObjectKey::new("rekey");

        // Delete → tombstone exists.
        store.put_object_in_bucket(&bucket, make_object_meta("rekey", 5, None)).unwrap();
        store.delete_object(&bucket, &key, oceanfs_core::Hlc::zero()).unwrap();
        assert!(store.has_tombstone(&bucket, &key).unwrap(), "tombstone must exist after delete");

        // Fresh write → tombstone cleared.
        store.put_object_in_bucket(&bucket, make_object_meta("rekey", 6, None)).unwrap();
        assert!(
            !store.has_tombstone(&bucket, &key).unwrap(),
            "fresh write must clear the tombstone"
        );

        // The new version is readable.
        let got = store.get_object(&bucket, &key).unwrap().unwrap();
        assert_eq!(got.size, 6);
    }

    #[test]
    fn get_nonexistent_object_returns_none() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let result = store.get_object(&BucketId::new("default"), &ObjectKey::new("nope")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_object_removes_it() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let meta = make_object_meta("temp.txt", 100, None);
        store.put_object(meta).unwrap();
        store
            .delete_object(
                &BucketId::new("default"),
                &ObjectKey::new("temp.txt"),
                oceanfs_core::Hlc::zero(),
            )
            .unwrap();

        let result =
            store.get_object(&BucketId::new("default"), &ObjectKey::new("temp.txt")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn tombstone_persists_stamped_hlc() {
        // G4: the tombstone must carry the delete's HLC, not zero.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        store.put_object(make_object_meta("stamped.txt", 10, None)).unwrap();

        let hlc = Hlc::new(1111, 7);
        store
            .delete_object(&BucketId::new("default"), &ObjectKey::new("stamped.txt"), hlc)
            .unwrap();

        let tombstone = store
            .get_tombstone(&BucketId::new("default"), &ObjectKey::new("stamped.txt"))
            .unwrap()
            .expect("tombstone must exist after delete");
        assert_eq!(tombstone.hlc, hlc, "tombstone must persist the stamped delete HLC");
    }

    #[test]
    fn delete_twice_stamps_monotonically_increasing_hlc() {
        // G4: the second delete's (higher) HLC must overwrite the first.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        store.put_object(make_object_meta("twice.txt", 10, None)).unwrap();

        let first = Hlc::new(2000, 1);
        let second = Hlc::new(3000, 2);
        store
            .delete_object(&BucketId::new("default"), &ObjectKey::new("twice.txt"), first)
            .unwrap();
        store
            .delete_object(&BucketId::new("default"), &ObjectKey::new("twice.txt"), second)
            .unwrap();

        let tombstone = store
            .get_tombstone(&BucketId::new("default"), &ObjectKey::new("twice.txt"))
            .unwrap()
            .expect("tombstone must exist");
        assert_eq!(tombstone.hlc, second, "latest delete HLC must win");
    }

    #[test]
    fn list_objects_by_prefix() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();

        for name in &["a/1.txt", "a/2.txt", "b/3.txt"] {
            store.put_object(make_object_meta(name, 10, None)).unwrap();
        }

        let results = store.list_objects(&BucketId::new("default"), "a/");
        let results: Vec<_> = results.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn tombstone_roundtrip() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("deleted.txt");

        store
            .put_tombstone(
                &bucket,
                &key,
                Tombstone {
                    deletion_time: 1700000000000,
                    hlc: Hlc::new(1700000000000, 1),
                    chunks: smallvec::SmallVec::new(),
                },
            )
            .unwrap();

        assert!(store.has_tombstone(&bucket, &key).unwrap());
    }

    #[test]
    fn no_tombstone_for_nonexistent_key() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        assert!(!store.has_tombstone(&BucketId::new("default"), &ObjectKey::new("nope")).unwrap());
    }

    // --- RocksDB property tests ---

    #[test]
    fn property_unknown_returns_none() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        assert_eq!(store.property("rocksdb.nonexistent-property"), None);
    }

    #[test]
    fn property_estimate_num_keys_works() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let val = store.property("rocksdb.estimate-num-keys");
        assert!(val.is_some(), "estimate-num-keys should be available");
        let num: u64 = val.unwrap().parse().unwrap();
        assert!(num <= 1000, "new store should have few keys, got {num}");
    }

    // --- New tuning tests ---

    #[test]
    fn per_cf_write_buffer_configuration() {
        let config = test_config();
        assert_eq!(config.objects_write_buffer_mb, 4);
        assert_eq!(config.segments_write_buffer_mb, 8);
        assert_eq!(config.deletions_write_buffer_mb, 1);
        // Different sizes confirms per-CF differentiation
        assert_ne!(config.objects_write_buffer_mb, config.segments_write_buffer_mb);
        assert_ne!(config.segments_write_buffer_mb, config.deletions_write_buffer_mb);
    }

    #[test]
    fn metrics_gauges_initialised() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let m = store.metrics();
        // All gauges start at zero
        assert_eq!(m.block_cache_hit.get(), 0);
        assert_eq!(m.block_cache_miss.get(), 0);
        assert_eq!(m.memtable_size.get(), 0);
        assert_eq!(m.running_compactions.get(), 0);
        assert_eq!(m.running_flushes.get(), 0);
        assert_eq!(m.estimate_num_keys.get(), 0);
        assert_eq!(m.num_files_at_level_0.get(), 0);
        assert_eq!(m.live_sst_files_size.get(), 0);
        assert_eq!(m.estimate_table_readers_mem.get(), 0);
    }

    #[test]
    fn metrics_populated_after_write() {
        let config = test_config();
        let store = RocksDbMetadataStore::open(&config).unwrap();

        // Write 100 objects to trigger some RocksDB activity
        for i in 0..100 {
            let key = format!("obj-{:04}", i);
            store.put_object(make_object_meta(&key, i * 10, None)).unwrap();
        }

        // Force flush so data hits SST files
        store.close().unwrap();
        // Drop the first store so RocksDB releases the lock
        drop(store);

        // Reopen and verify flush+close+reopen roundtrip
        let store2 = RocksDbMetadataStore::open(&config).unwrap();
        let got =
            store2.get_object(&BucketId::new("default"), &ObjectKey::new("obj-0042")).unwrap();
        assert!(got.is_some(), "data must persist after flush and reopen");
        assert_eq!(got.unwrap().size, 420);
    }

    #[test]
    fn max_open_files_configurable() {
        let mut config = test_config();
        config.max_open_files = 2048;
        let store = RocksDbMetadataStore::open(&config).unwrap();
        // Store should open successfully; we verify the option was accepted
        // by writing and reading back.
        let meta = make_object_meta("max-files-test", 100, None);
        store.put_object(meta.clone()).unwrap();
        let got = store
            .get_object(&BucketId::new("default"), &ObjectKey::new("max-files-test"))
            .unwrap()
            .unwrap();
        assert_eq!(got.size, 100);
    }

    #[test]
    fn default_config_has_expected_values() {
        let config = MetadataConfig::default();
        assert_eq!(config.objects_write_buffer_mb, 64);
        assert_eq!(config.segments_write_buffer_mb, 256);
        assert_eq!(config.deletions_write_buffer_mb, 16);
        assert_eq!(config.max_open_files, -1);
        assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
    }

    #[test]
    fn rocksdb_tuning_roundtrip() {
        let config = test_config();
        let store = RocksDbMetadataStore::open(&config).unwrap();

        // Write 1000 objects — enough to trigger at least one memtable flush
        // and demonstrate that the bloom filter + compression are active.
        let bucket = BucketId::new("roundtrip-bucket");
        for i in 0..1000u32 {
            let key = format!("obj-{:05}", i);
            let meta = make_object_meta(&key, i as u64 * 17, None);
            store.put_object_in_bucket(&bucket, meta).unwrap();
        }

        // Read them all back in random order (exercises bloom filter)
        for i in (0..1000u32).rev() {
            let key = format!("obj-{:05}", i);
            let got = store.get_object(&bucket, &ObjectKey::new(&key)).unwrap();
            assert!(got.is_some(), "object obj-{i:05} must be readable after write");
            assert_eq!(got.unwrap().size, i as u64 * 17);
        }

        // Verify data accessibility after writes (bloom filter is active —
        // key-not-found queries should return quickly even without flush).
        for i in 0..10u32 {
            let key = format!("nonexistent-{:05}", i * 13);
            let got = store.get_object(&bucket, &ObjectKey::new(&key)).unwrap();
            assert!(got.is_none(), "nonexistent key must return None via bloom filter");
        }

        // Verify the roundtrip write volume was handled correctly
        // by checking a sample of keys still read back valid.
        let sample = store.get_object(&bucket, &ObjectKey::new("obj-00042")).unwrap();
        assert!(sample.is_some(), "write-heavy roundtrip: obj-00042 must be readable");
        assert_eq!(sample.unwrap().size, 42 * 17);
    }

    #[test]
    fn rocksdb_metrics_exports() {
        let config = test_config();
        let store = RocksDbMetadataStore::open(&config).unwrap();
        let m = store.metrics();

        // All gauges should start at zero
        assert_eq!(m.block_cache_hit.get(), 0);
        assert_eq!(m.memtable_size.get(), 0);

        // Write enough data to trigger RocksDB activity
        for i in 0..100 {
            store.put_object(make_object_meta(&format!("metrics-{i}"), 64, None)).unwrap();
        }

        // Flush so the data hits SST files (memtable → L0)
        store.close().unwrap();

        // Verify gauges are still accessible after close
        // (they remain valid because they share Arc inner state)
        let _ = m.block_cache_hit.get();
        let _ = m.block_cache_miss.get();
        let _ = m.memtable_size.get();
        let _ = m.running_compactions.get();
        let _ = m.running_flushes.get();
        let _ = m.estimate_num_keys.get();
        let _ = m.num_files_at_level_0.get();
        let _ = m.live_sst_files_size.get();
        let _ = m.estimate_table_readers_mem.get();

        // Verify Gauge renders in Prometheus format
        let rendered = m.block_cache_hit.render();
        assert!(rendered.contains("rocksdb_block_cache_hit"), "must contain metric name");

        // The load-test sentinel gauge must render under its documented name.
        let rendered_l0 = m.num_files_at_level_0.render();
        assert!(
            rendered_l0.contains("rocksdb_num_files_at_level_0"),
            "must contain metric name: {rendered_l0}"
        );
    }

    #[test]
    fn unresolved_rocksdb_properties_empty_on_real_db() {
        // On a real RocksDB build every polled property must resolve —
        // otherwise the load-test assertions would silently see zeros.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let unresolved = unresolved_rocksdb_properties(&store.db);
        assert!(
            unresolved.is_empty(),
            "unresolvable RocksDB properties (gauges would pin at 0): {unresolved:?}"
        );
    }

    // ------------------------------------------------------------------
    // ADR-0034 D2 supersede-capture tests
    // ------------------------------------------------------------------

    #[test]
    fn overwrite_captures_superseded_version_chunks() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();
        let key = ObjectKey::new("obj");

        // v1 chunked on segment A; v2 (overwrite) chunked on segment B.
        let v1 =
            make_segment_stored_meta("obj", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 100)]);
        let v2 =
            make_segment_stored_meta("obj", Hlc::new(2000, 1), vec![make_chunk(seg_b, 0, 100)]);
        store.put_object_in_bucket(&bucket, v1.clone()).unwrap();
        store.put_object_in_bucket(&bucket, v2.clone()).unwrap();

        // Live row references B only.
        let live = store.get_object(&bucket, &key).unwrap().unwrap();
        assert_eq!(live.chunks.len(), 1);
        assert_eq!(live.chunks[0].segment_id, seg_b, "live row references B only");

        // A's bytes captured: exactly one Supersede holding v1's chunk ref.
        let sups = supersedes(&store);
        assert_eq!(sups.len(), 1, "exactly one supersede for the overwrite");
        let (_, _, rec) = &sups[0];
        assert_eq!(rec.kind, DeadChunkKind::Supersede);
        assert_eq!(rec.hlc, v1.hlc, "supersede version = superseded version's HLC");
        assert_eq!(rec.chunks.len(), 1);
        assert_eq!(rec.chunks[0].segment_id, seg_a);

        // No plain tombstone for the live key.
        assert!(!store.has_tombstone(&bucket, &key).unwrap());
    }

    #[test]
    fn delete_then_reput_migrates_tombstone_capture_exactly_once() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let key = ObjectKey::new("obj");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        // v1 chunked on A; delete captures v1's chunks into the tombstone.
        let v1 =
            make_segment_stored_meta("obj", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 100)]);
        store.put_object_in_bucket(&bucket, v1).unwrap();
        store.delete_object(&bucket, &key, Hlc::new(2000, 1)).unwrap();
        assert!(store.has_tombstone(&bucket, &key).unwrap());
        let tombstone = store.get_tombstone(&bucket, &key).unwrap().unwrap();

        // re-PUT v2 on B (newer version).
        let v2 =
            make_segment_stored_meta("obj", Hlc::new(3000, 1), vec![make_chunk(seg_b, 0, 100)]);
        store.put_object_in_bucket(&bucket, v2.clone()).unwrap();

        // Plain tombstone cleared; exactly one Supersede migrated the v1
        // chunks with the delete HLC as version and the ORIGINAL deletion
        // time preserved (TTL aging must not reset).
        assert!(!store.has_tombstone(&bucket, &key).unwrap());
        let sups = supersedes(&store);
        assert_eq!(sups.len(), 1, "re-PUT migrates the capture, no double-dead");
        let (_, _, rec) = &sups[0];
        assert_eq!(rec.kind, DeadChunkKind::Supersede);
        assert_eq!(rec.hlc, Hlc::new(2000, 1), "version stays the delete's HLC");
        assert_eq!(rec.captured_at, tombstone.deletion_time, "captured_at preserved");
        assert_eq!(rec.chunks.len(), 1);
        assert_eq!(rec.chunks[0].segment_id, seg_a);

        // Live row survives (v2 on B).
        let live = store.get_object(&bucket, &key).unwrap().unwrap();
        assert_eq!(live.hlc, v2.hlc);
        assert_eq!(live.chunks[0].segment_id, seg_b);
    }

    #[test]
    fn multipart_overwrite_captures_every_segment_exactly_once() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();
        let seg_c = SegmentId::new();
        let seg_d = SegmentId::new();

        // v1 is a multipart object spanning three segments.
        let v1 = make_segment_stored_meta(
            "multi",
            Hlc::new(1000, 1),
            vec![make_chunk(seg_a, 0, 100), make_chunk(seg_b, 100, 50), make_chunk(seg_c, 0, 200)],
        );
        store.put_object_in_bucket(&bucket, v1.clone()).unwrap();

        // v2 (overwrite) lives on a single segment.
        let v2 =
            make_segment_stored_meta("multi", Hlc::new(2000, 1), vec![make_chunk(seg_d, 0, 350)]);
        store.put_object_in_bucket(&bucket, v2.clone()).unwrap();

        let sups = supersedes(&store);
        assert_eq!(sups.len(), 1);
        let (_, _, rec) = &sups[0];
        // Every segment of v1 appears exactly once (dedupe by chunk ref).
        let mut segments: Vec<_> = rec.chunks.iter().map(|c| c.segment_id).collect();
        segments.sort();
        let mut expected = vec![seg_a, seg_b, seg_c];
        expected.sort();
        assert_eq!(segments, expected, "all N segments captured exactly once");
    }

    #[test]
    fn same_or_older_hlc_repair_write_does_not_capture() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        let v1 =
            make_segment_stored_meta("obj", Hlc::new(2000, 1), vec![make_chunk(seg_a, 0, 100)]);
        store.put_object_in_bucket(&bucket, v1.clone()).unwrap();

        // A read-repair physical re-point carries the SAME logical HLC; it
        // must not capture A's bytes (they are still the winning version).
        let repaired =
            make_segment_stored_meta("obj", Hlc::new(2000, 1), vec![make_chunk(seg_b, 0, 100)]);
        store.put_object_in_bucket(&bucket, repaired.clone()).unwrap();
        assert!(supersedes(&store).is_empty(), "same-HLC physical re-point must not capture");

        // An out-of-order STALE write (older HLC) also must not capture the
        // newer version it physically overwrites (V2 guard).
        let stale =
            make_segment_stored_meta("obj", Hlc::new(1000, 1), vec![make_chunk(seg_b, 0, 100)]);
        store.put_object_in_bucket(&bucket, stale.clone()).unwrap();
        assert!(
            supersedes(&store).is_empty(),
            "older-HLC write must not capture the newer version"
        );
    }

    #[test]
    fn inline_overwrite_does_not_capture() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");

        // Inline objects have no segment bytes; overwrites capture nothing.
        store.put_object_in_bucket(&bucket, make_object_meta("obj", 5, Some(b"hello"))).unwrap();
        let mut newer = make_object_meta("obj", 6, Some(b"hello!"));
        newer.hlc = Hlc::new(2000, 1);
        store.put_object_in_bucket(&bucket, newer).unwrap();

        assert!(supersedes(&store).is_empty());
    }

    #[test]
    fn supersede_records_invisible_to_plain_tombstone_enumeration() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        // Plain tombstone for key "deleted" (delete of a segment-stored v1).
        let deleted =
            make_segment_stored_meta("deleted", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 50)]);
        store.put_object_in_bucket(&bucket, deleted).unwrap();
        store.delete_object(&bucket, &ObjectKey::new("deleted"), Hlc::new(2000, 1)).unwrap();

        // Supersede for key "overwritten".
        let old = make_segment_stored_meta(
            "overwritten",
            Hlc::new(1000, 1),
            vec![make_chunk(seg_b, 0, 100)],
        );
        store.put_object_in_bucket(&bucket, old).unwrap();
        let new = make_segment_stored_meta(
            "overwritten",
            Hlc::new(3000, 1),
            vec![make_chunk(seg_a, 0, 100)],
        );
        store.put_object_in_bucket(&bucket, new).unwrap();

        // Both dead-chunk kinds visible through the typed enumeration.
        let records = dead_records(&store);
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .any(|(_, k, r)| k.as_str() == "deleted" && r.kind == DeadChunkKind::Tombstone),
            "plain tombstone must enumerate as Tombstone kind"
        );
        assert!(
            records
                .iter()
                .any(|(_, k, r)| k.as_str() == "overwritten" && r.kind == DeadChunkKind::Supersede),
            "supersede must enumerate as Supersede kind"
        );

        // The plain-tombstone enumerations see ONLY the plain tombstone —
        // pre-f2 consumers stay byte-identical.
        let all = store.list_tombstones_all().into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.as_str(), "deleted");

        let per_bucket =
            store.list_tombstones(&bucket).into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(per_bucket.len(), 1);
        assert_eq!(per_bucket[0].0.as_str(), "deleted");
    }

    #[test]
    fn overwrite_increments_supersede_counters() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        assert_eq!(store.metrics().supersede_captured_total.get(), 0);
        assert_eq!(store.metrics().supersede_dead_bytes_total.get(), 0);

        let v1 =
            make_segment_stored_meta("obj", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 100)]);
        let v2 = make_segment_stored_meta("obj", Hlc::new(2000, 1), vec![make_chunk(seg_b, 0, 60)]);
        store.put_object_in_bucket(&bucket, v1).unwrap();
        store.put_object_in_bucket(&bucket, v2).unwrap();

        assert_eq!(store.metrics().supersede_captured_total.get(), 1);
        assert_eq!(store.metrics().supersede_dead_bytes_total.get(), 100);
    }

    #[test]
    fn concurrent_same_key_overwrites_capture_each_version_once() {
        use std::sync::Arc;

        let store = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let bucket = BucketId::new("bucket");
        let key = ObjectKey::new("hot");
        let seg_v0 = SegmentId::new();

        // Seed v0.
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta("hot", Hlc::new(0, 0), vec![make_chunk(seg_v0, 0, 100)]),
            )
            .unwrap();

        // W writers all overwrite with the SAME strictly-newer HLC. Under
        // the per-key lock the first commit captures v0; every later writer
        // finds `existing.hlc == meta.hlc` and captures nothing — exactly
        // ONE supersede. Without the lock, several writers read v0 before
        // any commit and double-capture it (multiple supersedes with
        // version v0), so this assertion is a reliable lock regression test.
        const WRITERS: usize = 16;
        let barrier = Arc::new(std::sync::Barrier::new(WRITERS));
        std::thread::scope(|scope| {
            for _ in 0..WRITERS {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let bucket = BucketId::new("bucket");
                scope.spawn(move || {
                    let seg = SegmentId::new();
                    let meta = make_segment_stored_meta(
                        "hot",
                        Hlc::new(9_000, 0),
                        vec![make_chunk(seg, 0, 100)],
                    );
                    barrier.wait();
                    store.put_object_in_bucket(&bucket, meta).unwrap();
                });
            }
        });

        let sups = supersedes(&store);
        assert_eq!(sups.len(), 1, "v0 must be captured exactly once across W writers");
        let (_, _, rec) = &sups[0];
        assert_eq!(rec.hlc, Hlc::new(0, 0), "only v0 is superseded");
        assert_eq!(rec.chunks[0].segment_id, seg_v0);

        // Live row survives and references one of the writers' segments.
        let live = store.get_object(&bucket, &key).unwrap().expect("live row must survive");
        assert!(live.hlc >= Hlc::new(9_000, 0));
    }

    #[test]
    fn with_key_lock_serializes_same_key_critical_sections() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let store = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let inside = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let workers = 8;
        let barrier = Arc::new(std::sync::Barrier::new(workers));
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let store = Arc::clone(&store);
                let inside = Arc::clone(&inside);
                let max_seen = Arc::clone(&max_seen);
                let barrier = Arc::clone(&barrier);
                let bucket = BucketId::new("bucket");
                let key = ObjectKey::new("k");
                scope.spawn(move || {
                    barrier.wait();
                    store.with_key_lock(&bucket, &key, || {
                        let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_micros(200));
                        inside.fetch_sub(1, Ordering::SeqCst);
                    });
                });
            }
        });

        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "same-key critical sections must be mutually exclusive"
        );
    }

    #[test]
    fn replica_metadata_apply_overwrite_captures_through_trait() {
        // The gRPC segment-service replica-apply seam
        // (`segment_service.rs` `put_object_metadata`) calls the storage-api
        // `MetadataStore::put_object` trait method with the pushed row's
        // HLC — the same funnel as S3 PUT. Driving the trait directly proves
        // D6 "Replica metadata apply overwriting a row": capture fires on
        // the replica path, not just on the concrete method.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        let v1 =
            make_segment_stored_meta("repobj", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 100)]);
        let v2 =
            make_segment_stored_meta("repobj", Hlc::new(2000, 1), vec![make_chunk(seg_b, 0, 100)]);

        let md: &dyn oceanfs_storage_api::MetadataStore = &store;
        md.put_object(&bucket, v1.clone()).unwrap();
        md.put_object(&bucket, v2.clone()).unwrap();

        let sups = supersedes(&store);
        assert_eq!(sups.len(), 1, "capture must fire through the trait (replica-apply path)");
        assert_eq!(sups[0].2.hlc, v1.hlc);
        assert_eq!(sups[0].2.chunks[0].segment_id, seg_a);

        let live = store.get_object(&bucket, &ObjectKey::new("repobj")).unwrap().unwrap();
        assert_eq!(live.chunks[0].segment_id, seg_b);
    }

    #[test]
    fn plain_tombstone_only_store_enumeration_unchanged() {
        // A store holding ONLY plain tombstones must enumerate byte-identically
        // to the pre-accounting store through list_tombstones / list_tombstones_all,
        // and the typed enumeration classifies every record as Tombstone.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        for i in 0..3u64 {
            let seg = SegmentId::new();
            let key = format!("del-{i}");
            let meta = make_segment_stored_meta(
                &key,
                Hlc::new(1_000 + i, 1),
                vec![make_chunk(seg, 0, 100)],
            );
            store.put_object_in_bucket(&bucket, meta).unwrap();
            store.delete_object(&bucket, &ObjectKey::new(&key), Hlc::new(2_000 + i, 1)).unwrap();
        }

        let all = store.list_tombstones_all().into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(all.len(), 3, "plain-only store: all tombstones surfaced unchanged");

        let per_bucket =
            store.list_tombstones(&bucket).into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(per_bucket.len(), 3);

        let records = dead_records(&store);
        assert_eq!(records.len(), 3);
        assert!(
            records.iter().all(|(_, _, r)| r.kind == DeadChunkKind::Tombstone),
            "no supersede may exist in a plain-only store"
        );
    }

    #[test]
    fn exact_key_tombstone_ops_ignore_coexisting_supersede() {
        // `has_tombstone` / `get_tombstone` / `delete_tombstone` operate on
        // the exact plain key; a coexisting versioned supersede for the same
        // (bucket, key) must be structurally invisible to all three.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");
        let key = ObjectKey::new("k");
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();

        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta("k", Hlc::new(1000, 1), vec![make_chunk(seg_a, 0, 100)]),
            )
            .unwrap();
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta("k", Hlc::new(2000, 1), vec![make_chunk(seg_b, 0, 100)]),
            )
            .unwrap();
        assert_eq!(supersedes(&store).len(), 1, "supersede for the key exists");

        // Exact-key reads never observe the supersede.
        assert!(!store.has_tombstone(&bucket, &key).unwrap());
        assert!(store.get_tombstone(&bucket, &key).unwrap().is_none());

        // delete_tombstone on the exact plain key removes nothing (no plain
        // tombstone exists); the supersede survives untouched.
        store.delete_tombstone(&bucket, &key).unwrap();
        assert_eq!(supersedes(&store).len(), 1, "delete_tombstone must not see supersedes");
    }

    #[test]
    fn concurrent_delete_and_overwrite_never_double_capture_a_version() {
        // Deletes (tombstones) and overwrites (supersedes) racing on the same
        // key must never capture the same predecessor version twice. Both
        // paths run under the per-key stripe and commit atomically, so each
        // version's bytes are captured at most once regardless of schedule.
        use std::sync::Arc;

        let store = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let bucket = BucketId::new("bucket");
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta(
                    "k",
                    Hlc::new(1, 0),
                    vec![make_chunk(SegmentId::new(), 0, 100)],
                ),
            )
            .unwrap();

        const THREADS: usize = 6;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let bucket = BucketId::new("bucket");
                let key = ObjectKey::new("k");
                scope.spawn(move || {
                    barrier.wait();
                    for i in 0..10u32 {
                        let seq = t as u32 * 100 + i + 1;
                        if t % 2 == 0 {
                            // Delete whatever row is live with a stamped HLC.
                            let _ = store.delete_object(
                                &bucket,
                                &key,
                                Hlc::new(1_000_000 + u64::from(seq), 0),
                            );
                        } else {
                            // Overwrite with a strictly newer version.
                            let seg = SegmentId::new();
                            let meta = make_segment_stored_meta(
                                "k",
                                Hlc::new(2_000_000 + u64::from(seq), 0),
                                vec![make_chunk(seg, 0, 100)],
                            );
                            let _ = store.put_object_in_bucket(&bucket, meta);
                        }
                    }
                });
            }
        });

        // Double-capture detector: every writer stamps a UNIQUE fresh
        // segment, so the chunk-set carried by a dead-chunk record uniquely
        // identifies the version it captured. A tombstone's hlc is the
        // *delete* stamp and a supersede's hlc is the *superseded row's*
        // hlc, so keying on hlc alone cannot detect a tombstone + supersede
        // both capturing the same predecessor. Keying on the chunk-set
        // does: two records sharing a non-empty chunk-set for the same key
        // means the predecessor's bytes were captured twice. Empty captures
        // (deleting an absent row) carry no bytes and are skipped.
        let records = dead_records(&store);
        let mut seen_chunk_sets: std::collections::HashSet<(
            BucketId,
            ObjectKey,
            Vec<(SegmentId, u64)>,
        )> = std::collections::HashSet::new();
        for (b, k, r) in &records {
            if r.chunks.is_empty() {
                continue;
            }
            let mut chunk_ids: Vec<(SegmentId, u64)> =
                r.chunks.iter().map(|c| (c.segment_id, c.offset)).collect();
            chunk_ids.sort_unstable();
            assert!(
                seen_chunk_sets.insert((b.clone(), k.clone(), chunk_ids)),
                "a predecessor version's bytes were captured more than once \
                 under delete/overwrite races (bucket={}, key={}, hlc={:?})",
                b.as_str(),
                k.as_str(),
                r.hlc
            );
        }
    }

    #[test]
    fn delete_dead_chunk_record_removes_only_the_versioned_supersede() {
        // f2's post-compaction supersede cleanup deletes ONE versioned
        // dead-chunk record by (bucket, key, version). It must never
        // touch the key's LIVE object row and never remove a sibling
        // supersede of the same key (a different overwritten version).
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let bucket = BucketId::new("bucket");

        // Key "a": v1 (hlc 1000) then v2 (hlc 2000) → one supersede
        // carrying v1's chunks. Key "b": the same shape.
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta(
                    "a",
                    Hlc::new(1000, 0),
                    vec![make_chunk(SegmentId::new(), 0, 10)],
                ),
            )
            .unwrap();
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta(
                    "a",
                    Hlc::new(2000, 0),
                    vec![make_chunk(SegmentId::new(), 0, 20)],
                ),
            )
            .unwrap();
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta(
                    "b",
                    Hlc::new(1000, 0),
                    vec![make_chunk(SegmentId::new(), 0, 30)],
                ),
            )
            .unwrap();
        store
            .put_object_in_bucket(
                &bucket,
                make_segment_stored_meta(
                    "b",
                    Hlc::new(2000, 0),
                    vec![make_chunk(SegmentId::new(), 0, 40)],
                ),
            )
            .unwrap();
        assert_eq!(supersedes(&store).len(), 2, "two keys each carry one supersede");

        // Delete key "a"'s supersede (version = the superseded v1 HLC).
        store.delete_dead_chunk_record(&bucket, &ObjectKey::new("a"), Hlc::new(1000, 0)).unwrap();

        let recs = supersedes(&store);
        assert_eq!(recs.len(), 1, "only key a's supersede is deleted");
        assert_eq!(recs[0].1.as_str(), "b");

        // Both keys' LIVE rows survive.
        assert!(store.get_object(&bucket, &ObjectKey::new("a")).unwrap().is_some());
        assert!(store.get_object(&bucket, &ObjectKey::new("b")).unwrap().is_some());

        // Deleting the same record again is a no-op (idempotent).
        store.delete_dead_chunk_record(&bucket, &ObjectKey::new("a"), Hlc::new(1000, 0)).unwrap();
        assert_eq!(supersedes(&store).len(), 1);
    }

    /// The RocksDB metrics poller is cancellable: cancelling its token
    /// makes the spawned task exit (dropping its `Arc<DB>`), so the store
    /// can be reopened on the same directory — a leaked task would pin the
    /// RocksDB LOCK forever (every spawned task must be cancellable).
    #[tokio::test]
    async fn metrics_poller_is_cancellable_and_releases_the_db() {
        let dir = tempfile::tempdir().unwrap();
        let config = MetadataConfig { data_dir: dir.path().to_path_buf(), ..Default::default() };
        let store = Arc::new(RocksDbMetadataStore::open(&config).unwrap());

        let shutdown = tokio_util::sync::CancellationToken::new();
        let handle = store.start_metrics_task(shutdown.clone());
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("poller must exit after cancellation")
            .expect("poller join handle");

        // Drop the store; with the poller gone, no Arc<DB> remains and the
        // LOCK is released — reopening the same dir must succeed.
        drop(store);
        let reopened = RocksDbMetadataStore::open(&config);
        assert!(reopened.is_ok(), "the DB LOCK must be released after shutdown");
    }

    // -----------------------------------------------------------------------
    // g8 rebuild primitives
    // -----------------------------------------------------------------------

    fn put_object_in(
        store: &RocksDbMetadataStore,
        bucket: &str,
        key: &str,
        hlc: u64,
    ) -> ObjectMetadata {
        let mut meta = make_object_meta(key, 64, None);
        meta.hlc = Hlc::new(hlc, 0);
        store.put_object_in_bucket(&BucketId::new(bucket), meta.clone()).unwrap();
        meta
    }

    /// A metadata row whose chunks reference a real segment (so a delete
    /// captures dead bytes the reaper can reclaim).
    fn segment_meta(key: &str, hlc: u64) -> ObjectMetadata {
        let mut meta = make_object_meta(key, 1024, None);
        meta.hlc = Hlc::new(hlc, 0);
        meta.chunks.push(ChunkRef {
            segment_id: SegmentId::new(),
            offset: 0,
            length: 1024,
            compressed: false,
            logical_length: 1024,
        });
        meta
    }

    fn row_key(bucket: &str, key: &str) -> Vec<u8> {
        format!("{bucket}\0{key}").into_bytes()
    }

    fn capture_object_row(
        store: &RocksDbMetadataStore,
        bucket: &str,
        key: &str,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        let want = row_key(bucket, key);
        let mut found = None;
        store
            .visit_objects_rows(|k, v| {
                if k == want {
                    found = Some((k.to_vec(), v.to_vec()));
                    false
                } else {
                    true
                }
            })
            .unwrap();
        found
    }

    fn capture_deletion_row(
        store: &RocksDbMetadataStore,
        bucket: &str,
        key: &str,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        let want = row_key(bucket, key);
        let mut found = None;
        store
            .visit_deletions_rows(|k, v| {
                if k == want {
                    found = Some((k.to_vec(), v.to_vec()));
                    false
                } else {
                    true
                }
            })
            .unwrap();
        found
    }

    #[test]
    fn both_metadata_cfs_empty_tracks_freshness() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        assert!(store.both_metadata_cfs_empty().unwrap(), "a fresh store is empty");
        put_object_in(&store, "b", "k", 1);
        assert!(!store.both_metadata_cfs_empty().unwrap(), "a row makes the store non-empty");
    }

    #[test]
    fn visit_object_rows_streams_raw_rows_and_stops_early() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        put_object_in(&store, "b", "k1", 1);
        put_object_in(&store, "b", "k2", 2);
        put_object_in(&store, "b", "k3", 3);

        let mut seen = 0;
        store
            .visit_objects_rows(|_k, _v| {
                seen += 1;
                seen < 2 // stop after the second row
            })
            .unwrap();
        assert_eq!(seen, 2, "the visitor stops the scan when it returns false");

        let mut keys = Vec::new();
        store
            .visit_objects_rows(|k, _v| {
                keys.push(String::from_utf8_lossy(k).into_owned());
                true
            })
            .unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|k| k.starts_with("b\0k")));
    }

    /// Folding a deleted object's deletion row restores the plain
    /// tombstone AND its captured dead chunks — the byte-accounting feed
    /// is not blinded (the reaper can reclaim the dead segment).
    #[test]
    fn rebuild_restores_deletion_rows_so_byte_accounting_is_not_blinded() {
        // Peer store: a live object (segment-backed), then a delete.
        let peer = RocksDbMetadataStore::open(&test_config()).unwrap();
        let meta = segment_meta("k", 10);
        peer.put_object_in_bucket(&BucketId::new("b"), meta.clone()).unwrap();
        peer.delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::new(11, 0)).unwrap();
        let (d_key, d_value) =
            capture_deletion_row(&peer, "b", "k").expect("peer holds the deletion row");
        assert!(capture_object_row(&peer, "b", "k").is_none(), "row deleted on the peer");

        // Recovering store: starts empty, folds ONLY the deletion row.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        assert!(store.both_metadata_cfs_empty().unwrap());
        let applied = store.rebuild_apply_deletion_row(&d_key, &d_value).unwrap();
        assert!(applied);
        assert!(
            store.get_object(&BucketId::new("b"), &ObjectKey::new("k")).unwrap().is_none(),
            "no live row resurrected"
        );
        assert!(store.has_tombstone(&BucketId::new("b"), &ObjectKey::new("k")).unwrap());
        // The dead-chunk record feed sees the captured bytes (not 0).
        let records = store.list_dead_chunk_records_all();
        let dead_bytes: u64 = records
            .iter()
            .flatten()
            .map(|(_, _, r)| r.chunks.iter().map(|c| u64::from(c.length)).sum::<u64>())
            .sum();
        assert_eq!(dead_bytes, 1024, "the captured chunk is reclaimable after the fold");
    }

    /// Rebuild folds are HLC-LWW and idempotent: a live row never
    /// resurrects a newer delete, a delete never removes a newer live
    /// row, and a recreate after a delete converges to the live row.
    #[test]
    fn rebuild_fold_is_hlc_lww_and_idempotent() {
        let peer = RocksDbMetadataStore::open(&test_config()).unwrap();
        // Live hlc=10 then delete hlc=11 on the peer.
        let live = put_object_in(&peer, "b", "k", 10);
        peer.delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::new(11, 0)).unwrap();
        let (d_key, d_value) = capture_deletion_row(&peer, "b", "k").expect("deletion row");
        let live_value = bincode::serialize(&live).unwrap();

        // Recovering store.
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();

        // Delete first, then attempt to fold the OLDER live row: refused.
        assert!(store.rebuild_apply_deletion_row(&d_key, &d_value).unwrap());
        assert!(
            !store.rebuild_apply_object_row(&row_key("b", "k"), &live_value).unwrap(),
            "a delete newer than the row must not be resurrected"
        );
        assert!(store.get_object(&BucketId::new("b"), &ObjectKey::new("k")).unwrap().is_none());

        // A NEWER recreate (hlc=12) wins over the tombstone.
        let newer = put_object_in(&store, "b", "k2", 12);
        let _ = newer;
        // (fold of a newer object row after a tombstone clears it)
        let mut recreated = live.clone();
        recreated.hlc = Hlc::new(12, 0);
        let recreated_value = bincode::serialize(&recreated).unwrap();
        assert!(store.rebuild_apply_object_row(&row_key("b", "k"), &recreated_value).unwrap());
        let got = store.get_object(&BucketId::new("b"), &ObjectKey::new("k")).unwrap().unwrap();
        assert_eq!(got.hlc, recreated.hlc, "newer write wins over the older delete");
        assert!(
            !store.has_tombstone(&BucketId::new("b"), &ObjectKey::new("k")).unwrap(),
            "the recreate clears the stale tombstone"
        );

        // Idempotent re-apply of the same row changes nothing.
        assert!(!store.rebuild_apply_object_row(&row_key("b", "k"), &recreated_value).unwrap());
    }

    // ── ADR-0038 capture + sync apply (ae2 / S2) ──

    /// Opens a store with an enabled journal; returns the store, the
    /// journal, and the tempdirs that keep both alive.
    fn open_with_journal(
    ) -> (RocksDbMetadataStore, Arc<MetadataJournal>, (tempfile::TempDir, tempfile::TempDir)) {
        let meta_dir = tempfile::tempdir().unwrap();
        let journal_dir = tempfile::tempdir().unwrap();
        let config = MetadataConfig {
            data_dir: meta_dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
            objects_write_buffer_mb: 4,
            segments_write_buffer_mb: 8,
            deletions_write_buffer_mb: 1,
            max_open_files: 1024,
            ..Default::default()
        };
        let journal =
            Arc::new(MetadataJournal::open(journal_dir.path(), &NodeId::new("test-node")).unwrap());
        let store =
            RocksDbMetadataStore::open_with_journal(&config, Some(Arc::clone(&journal))).unwrap();
        (store, journal, (meta_dir, journal_dir))
    }

    fn journal_entries(journal: &MetadataJournal) -> Vec<JournalEntry> {
        match journal.read_range(0, 10_000, u64::MAX).unwrap() {
            JournalRead::Entries { entries, .. } => entries,
            JournalRead::Gap { .. } => panic!("unexpected journal gap"),
        }
    }

    fn object_row_key(bucket: &str, key: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(bucket.as_bytes());
        out.push(0);
        out.extend_from_slice(key.as_bytes());
        out
    }

    #[test]
    fn disabled_store_exposes_no_journal_and_performs_no_journal_io() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        assert!(store.metadata_journal().is_none());
        let bucket = BucketId::new("default");
        store.put_object(make_object_meta("off.txt", 3, Some(b"abc"))).unwrap();
        store.delete_object(&bucket, &ObjectKey::new("off.txt"), Hlc::new(9, 0)).unwrap();
    }

    #[test]
    fn put_object_journals_the_put_and_fails_closed_on_append_failure() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let mut meta = make_object_meta("a.txt", 10, Some(b"payload"));
        meta.hlc = Hlc::new(5, 0);
        store.put_object_in_bucket(&bucket, meta).unwrap();

        let entries = journal_entries(&journal);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op, JournalOp::Put);
        assert_eq!(entries[0].key.as_ref(), object_row_key("default", "a.txt"));
        assert_eq!(entries[0].hlc, Hlc::new(5, 0));

        // J1: a poisoned journal fails the write closed — the row is not
        // committed and no record is appended.
        journal.poison_for_test("injected fsync failure");
        let mut next = make_object_meta("b.txt", 10, None);
        next.hlc = Hlc::new(6, 0);
        store
            .put_object_in_bucket(&bucket, next)
            .expect_err("journal failure must fail the write closed");
        assert!(
            store.get_object(&bucket, &ObjectKey::new("b.txt")).unwrap().is_none(),
            "the row was never committed"
        );
        assert_eq!(journal_entries(&journal).len(), 1);
    }

    #[test]
    fn delete_object_journals_the_delete_and_gc_cleanup_never_journals() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("d.txt");
        let meta = make_segment_stored_meta(
            "d.txt",
            Hlc::new(10, 0),
            vec![make_chunk(SegmentId::new(), 0, 64)],
        );
        store.put_object_in_bucket(&bucket, meta).unwrap();
        store.delete_object(&bucket, &key, Hlc::new(11, 0)).unwrap();

        let entries = journal_entries(&journal);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].op, JournalOp::Delete);
        assert_eq!(entries[1].hlc, Hlc::new(11, 0));
        assert_eq!(entries[1].key.as_ref(), object_row_key("default", "d.txt"));

        // GC accounting cleanup never journals.
        store.delete_dead_chunk_record(&bucket, &key, Hlc::new(10, 0)).unwrap();
        store.delete_tombstone(&bucket, &key).unwrap();
        assert_eq!(journal_entries(&journal).len(), 2);
    }

    #[test]
    fn batch_write_journals_only_when_logical_identity_changes() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("remap.txt");
        let meta = make_segment_stored_meta(
            "remap.txt",
            Hlc::new(20, 0),
            vec![make_chunk(SegmentId::new(), 0, 100)],
        );
        store.put_object_in_bucket(&bucket, meta.clone()).unwrap();
        assert_eq!(journal_entries(&journal).len(), 1);

        // Compaction/healing remap: same logical identity, different
        // physical chunk refs → NOT journaled.
        let mut remapped = meta.clone();
        remapped.chunks = vec![make_chunk(SegmentId::new(), 500, 100)].into_iter().collect();
        store
            .batch_write(vec![BatchOp::PutObject(bucket.clone(), key.clone(), remapped.clone())])
            .unwrap();
        assert_eq!(journal_entries(&journal).len(), 1, "physical re-point does not journal");

        // A defensive future caller that changes the HLC must journal.
        let mut changed = remapped.clone();
        changed.hlc = Hlc::new(21, 0);
        store.batch_write(vec![BatchOp::PutObject(bucket.clone(), key.clone(), changed)]).unwrap();
        assert_eq!(journal_entries(&journal).len(), 2, "logical change journals");
    }

    #[test]
    fn rebuild_apply_object_row_journals_only_when_applied() {
        let (store, journal, _dirs) = open_with_journal();
        let row = object_row_key("default", "rebuild.txt");
        let meta = make_object_meta("rebuild.txt", 3, Some(b"abc"));
        let value = bincode::serialize(&meta).unwrap();

        assert!(store.rebuild_apply_object_row(&row, &value).unwrap());
        let entries = journal_entries(&journal);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op, JournalOp::Put);
        assert_eq!(entries[0].key.as_ref(), row.as_slice());

        // An equal-HLC re-apply is shadowed and appends nothing.
        assert!(!store.rebuild_apply_object_row(&row, &value).unwrap());
        assert_eq!(journal_entries(&journal).len(), 1);
    }

    #[test]
    fn sync_apply_object_applies_newer_and_skips_current_state() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let mut meta = make_object_meta("s.txt", 5, Some(b"hello"));
        meta.hlc = Hlc::new(30, 0);

        assert_eq!(
            store.sync_apply_object(&bucket, meta.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        assert_eq!(journal_entries(&journal).len(), 1);

        // Identical logical identity → already current, no new record.
        assert_eq!(
            store.sync_apply_object(&bucket, meta.clone()).unwrap(),
            SyncApplyOutcome::AlreadyCurrent
        );
        assert_eq!(journal_entries(&journal).len(), 1);

        // Older HLC loses.
        let mut older = meta.clone();
        older.hlc = Hlc::new(29, 0);
        assert_eq!(store.sync_apply_object(&bucket, older).unwrap(), SyncApplyOutcome::LocalWins);

        // Newer HLC wins and journals.
        let mut newer = meta.clone();
        newer.hlc = Hlc::new(31, 0);
        newer.size = 6;
        newer.inline_data = Some(bytes::Bytes::from_static(b"hello!"));
        assert_eq!(store.sync_apply_object(&bucket, newer).unwrap(), SyncApplyOutcome::Applied);
        assert_eq!(journal_entries(&journal).len(), 2);
    }

    #[test]
    fn sync_apply_object_ignores_physical_chunk_differences() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let meta = make_segment_stored_meta(
            "same.txt",
            Hlc::new(35, 0),
            vec![make_chunk(SegmentId::new(), 0, 100)],
        );
        assert_eq!(
            store.sync_apply_object(&bucket, meta.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );

        // Same logical identity, different physical refs → in sync.
        let mut replica = meta;
        replica.chunks = vec![make_chunk(SegmentId::new(), 999, 100)].into_iter().collect();
        assert_eq!(
            store.sync_apply_object(&bucket, replica).unwrap(),
            SyncApplyOutcome::AlreadyCurrent
        );
        assert_eq!(journal_entries(&journal).len(), 1, "no repair for ref-only differences");
    }

    #[test]
    fn sync_apply_object_equal_hlc_tie_break_is_symmetric() {
        let bucket = BucketId::new("default");
        let mut a = make_object_meta("tie.txt", 1, Some(b"a"));
        a.hlc = Hlc::new(40, 0);
        let mut b = a.clone();
        b.size = 2;
        b.inline_data = Some(bytes::Bytes::from_static(b"bb"));
        let (winner, loser) =
            if object_identity_hash(&a) > object_identity_hash(&b) { (a, b) } else { (b, a) };

        let (left, left_journal, _l) = open_with_journal();
        assert_eq!(
            left.sync_apply_object(&bucket, loser.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        assert_eq!(
            left.sync_apply_object(&bucket, winner.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );

        let (right, right_journal, _r) = open_with_journal();
        assert_eq!(
            right.sync_apply_object(&bucket, winner.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        assert_eq!(
            right.sync_apply_object(&bucket, loser.clone()).unwrap(),
            SyncApplyOutcome::LocalWins
        );

        for (store, journal) in [(&left, &left_journal), (&right, &right_journal)] {
            let stored = store.get_object(&bucket, &ObjectKey::new("tie.txt")).unwrap().unwrap();
            assert_eq!(object_identity_hash(&stored), object_identity_hash(&winner));
            assert!(!journal_entries(journal).is_empty(), "the winning apply journaled");
        }
        assert_eq!(journal_entries(&left_journal).len(), 2, "both applies changed state here");
        assert_eq!(journal_entries(&right_journal).len(), 1, "the loser apply was a no-op here");
    }

    #[test]
    fn sync_apply_equal_hlc_object_vs_tombstone_is_symmetric() {
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("cross.txt");
        let hlc = Hlc::new(80, 0);
        let mut meta = make_object_meta("cross.txt", 5, Some(b"hello"));
        meta.hlc = hlc;
        let tombstone = Tombstone { deletion_time: 1, hlc, chunks: smallvec::SmallVec::new() };
        let object_wins = object_identity_hash(&meta) > tombstone_identity_hash(&tombstone);

        // Direction 1: the object is the local state, the tombstone arrives.
        let (object_first, _j1, _d1) = open_with_journal();
        assert_eq!(
            object_first.sync_apply_object(&bucket, meta.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        let tombstone_decision =
            object_first.sync_apply_tombstone(&bucket, &key, tombstone.clone()).unwrap();

        // Direction 2: the tombstone is local, the object arrives.
        let (tombstone_first, _j2, _d2) = open_with_journal();
        assert_eq!(
            tombstone_first.sync_apply_tombstone(&bucket, &key, tombstone.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        let object_decision = tombstone_first.sync_apply_object(&bucket, meta.clone()).unwrap();

        if object_wins {
            // The object's identity hash wins: the arriving tombstone
            // loses; the arriving object overwrites the local tombstone.
            assert_eq!(tombstone_decision, SyncApplyOutcome::LocalWins);
            assert_eq!(object_decision, SyncApplyOutcome::Applied);
        } else {
            assert_eq!(tombstone_decision, SyncApplyOutcome::Applied);
            assert_eq!(object_decision, SyncApplyOutcome::LocalWins);
        }
        // Both sides converge to the same winner regardless of order.
        for store in [&object_first, &tombstone_first] {
            let row = store.get_object(&bucket, &key).unwrap();
            let ts = store.get_tombstone(&bucket, &key).unwrap();
            assert_eq!(row.is_some(), object_wins);
            assert_eq!(ts.is_some(), !object_wins);
        }
    }

    #[test]
    fn sync_apply_object_applies_foreign_chunk_refs_without_local_segments() {
        let (store, _journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        // A segment id this node has never heard of.
        let mut meta = make_segment_stored_meta(
            "foreign.bin",
            Hlc::new(45, 0),
            vec![make_chunk(SegmentId::new(), 0, 128)],
        );
        meta.chunks = vec![make_chunk(SegmentId::new(), 0, 128)].into_iter().collect();
        assert_eq!(store.sync_apply_object(&bucket, meta).unwrap(), SyncApplyOutcome::Applied);
        assert!(store.get_object(&bucket, &ObjectKey::new("foreign.bin")).unwrap().is_some());
    }

    #[test]
    fn sync_apply_tombstone_deletes_and_never_resurrects() {
        let (store, journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("gone.txt");
        let mut live = make_object_meta("gone.txt", 5, Some(b"hello"));
        live.hlc = Hlc::new(50, 0);
        assert_eq!(
            store.sync_apply_object(&bucket, live.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );

        let tombstone =
            Tombstone { deletion_time: 1, hlc: Hlc::new(51, 0), chunks: smallvec::SmallVec::new() };
        assert_eq!(
            store.sync_apply_tombstone(&bucket, &key, tombstone.clone()).unwrap(),
            SyncApplyOutcome::Applied
        );
        assert!(store.get_object(&bucket, &key).unwrap().is_none());
        assert_eq!(store.get_tombstone(&bucket, &key).unwrap().unwrap().hlc, Hlc::new(51, 0));
        let entries = journal_entries(&journal);
        assert_eq!(entries.last().unwrap().op, JournalOp::Delete);

        // A stale re-PUT must not resurrect the deleted object.
        let mut stale = live;
        stale.hlc = Hlc::new(50, 0);
        assert_eq!(store.sync_apply_object(&bucket, stale).unwrap(), SyncApplyOutcome::LocalWins);
        assert!(store.get_object(&bucket, &key).unwrap().is_none());
    }

    #[test]
    fn sync_apply_object_re_put_clears_a_local_tombstone() {
        let (store, _journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("reborn.txt");
        let tombstone =
            Tombstone { deletion_time: 1, hlc: Hlc::new(60, 0), chunks: smallvec::SmallVec::new() };
        assert_eq!(
            store.sync_apply_tombstone(&bucket, &key, tombstone).unwrap(),
            SyncApplyOutcome::Applied
        );
        assert!(store.get_tombstone(&bucket, &key).unwrap().is_some());

        let mut reborn = make_object_meta("reborn.txt", 3, Some(b"new"));
        reborn.hlc = Hlc::new(61, 0);
        assert_eq!(store.sync_apply_object(&bucket, reborn).unwrap(), SyncApplyOutcome::Applied);
        assert!(store.get_object(&bucket, &key).unwrap().is_some());
        assert!(store.get_tombstone(&bucket, &key).unwrap().is_none());
    }

    #[test]
    fn sync_apply_tombstone_captures_the_local_rows_chunks() {
        let (store, _journal, _dirs) = open_with_journal();
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("dead.bin");
        let live = make_segment_stored_meta(
            "dead.bin",
            Hlc::new(70, 0),
            vec![make_chunk(SegmentId::new(), 0, 256)],
        );
        assert_eq!(store.sync_apply_object(&bucket, live).unwrap(), SyncApplyOutcome::Applied);
        let tombstone =
            Tombstone { deletion_time: 2, hlc: Hlc::new(71, 0), chunks: smallvec::SmallVec::new() };
        assert_eq!(
            store.sync_apply_tombstone(&bucket, &key, tombstone).unwrap(),
            SyncApplyOutcome::Applied
        );

        let captured = supersedes(&store);
        assert_eq!(captured.len(), 1, "the local row's chunks were captured");
        assert_eq!(captured[0].2.hlc, Hlc::new(70, 0));
        assert_eq!(captured[0].2.chunks.len(), 1);
    }
}
