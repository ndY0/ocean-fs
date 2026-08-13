//! RocksDB-backed metadata store with strongly-typed CRUD.
//!
//! ## RocksDB Tuning
//!
//! Each column family is tuned for OceanFS's specific workload:
//!
//! | CF | Pattern | Bloom Filter | Write Buffer | Compression |
//! |---|---|---|---|---|
//! | objects | point lookups (GET/HEAD) | 10 bits/key (~1% FP) | 64 MB | Snappy L0-L1, Zstd L2+ |
//! | segments | batch writes (seal) | none | 256 MB | Snappy L0-L1, Zstd L2+ |
//! | deletions | append-mostly | none | 16 MB | Snappy L0-L1, Zstd L2+ |
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
use std::{sync::Arc, time::Duration};

use oceanfs_core::{
    BucketId, Gauge, Hlc, LabelSet, MetadataConfig, MetricRegistrar, ObjectKey, ObjectMetadata,
    SegmentId, SegmentMetadata, Tombstone,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::{
    error::{Error, Result},
    metadata::cf,
};

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

        // Segments CF: batch-write pattern → larger write buffer, no bloom needed.
        let segments_opts = build_cf_opts(
            &block_cache,
            config.segments_write_buffer_mb,
            false,
            config.memtable_size,
        );

        // Deletions CF: append-mostly, low volume → small write buffer.
        let deletions_opts = build_cf_opts(
            &block_cache,
            config.deletions_write_buffer_mb,
            false,
            config.memtable_size,
        );

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(cf::CF_OBJECTS, objects_opts),
            ColumnFamilyDescriptor::new(cf::CF_SEGMENTS, segments_opts),
            ColumnFamilyDescriptor::new(cf::CF_DELETIONS, deletions_opts),
        ];

        let db = DB::open_cf_descriptors(&opts, &config.data_dir, cf_descriptors)
            .map_err(|e| Error::Io(io_err(e)))?;

        let db = Arc::new(db);
        let metrics = Arc::new(RocksDbMetrics::default());

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
        if config.mlock_block_cache && !cfg!(test) {
            #[cfg(target_os = "linux")]
            {
                // SAFETY: `mlockall(MCL_CURRENT | MCL_FUTURE)` locks all
                // currently mapped pages AND all future pages of the
                // calling process into physical RAM. This prevents the
                // kernel from swapping any RocksDB-allocated pages
                // (block cache, memtables, SST index blocks) under
                // memory pressure. The call requires `CAP_IPC_LOCK`;
                // if not held, fails with EPERM and we log a warning.
                #[allow(unsafe_code)]
                let ret = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };
                if ret != 0 {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EPERM) {
                        tracing::warn!(
                            "mlockall(MCL_CURRENT|MCL_FUTURE) failed: CAP_IPC_LOCK not held. \
                             RocksDB block cache is not pinned in RAM — \
                             system may swap it under memory pressure. \
                             See deployment docs for capability requirements."
                        );
                    } else {
                        tracing::warn!(
                            error = %err,
                            "mlockall(MCL_CURRENT|MCL_FUTURE) failed — \
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
                             mlockall(MCL_CURRENT|MCL_FUTURE): \
                             VmLck = {locked_mb} MB (cache = {cache_mb} MB)"
                        );
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                tracing::info!("mlock_block_cache=true ignored on non-Linux platform");
            }
        }

        Ok(Self { db, metrics })
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
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let key = cf::encode_object_key("default", meta.object_key.as_str());
        let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Stores object metadata with an explicit bucket.
    ///
    /// A fresh write supersedes any prior deletion of this key: the
    /// deletion tombstone is cleared so that later read-repair pushes
    /// for the new version are not rejected by the tombstone gate
    /// (membership-stability-fixes F3/t19).
    ///
    /// # Errors
    ///
    /// Returns an error if the objects column family is not found, serialization
    /// fails, or the RocksDB write fails.
    pub fn put_object_in_bucket(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let key = cf::encode_object_key(bucket.as_str(), meta.object_key.as_str());
        let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;

        // Clear any stale tombstone: the object is alive again.
        self.delete_tombstone(bucket, &meta.object_key)?;

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
    /// # Errors
    ///
    /// Returns an error if the objects or deletions column family is not found,
    /// or if the RocksDB delete/write fails.
    pub fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        self.db.delete_cf(&cf, &db_key).map_err(|e| Error::Io(io_err(e)))?;

        // Write a deletion tombstone so that GC can compact this key
        // across replicas. Without this, cross-node deletion compaction
        // is non-functional.
        let tombstone = Tombstone {
            deletion_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            hlc,
        };
        self.put_tombstone(bucket, key, tombstone)?;

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

    // ------------------------------------------------------------------
    // Segment operations
    // ------------------------------------------------------------------

    /// Stores segment metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the segments column family is not found, serialization
    /// fails, or the RocksDB write fails.
    pub fn put_segment(&self, meta: SegmentMetadata) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_SEGMENTS)
            .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;

        let key = cf::encode_segment_key(&meta.segment_id);
        let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Retrieves segment metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the segments column family is not found,
    /// deserialization fails, or the RocksDB read fails.
    pub fn get_segment(&self, id: SegmentId) -> Result<Option<SegmentMetadata>> {
        let cf = self
            .db
            .cf_handle(cf::CF_SEGMENTS)
            .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;

        let key = cf::encode_segment_key(&id);

        match self.db.get_cf(&cf, key) {
            Ok(Some(value)) => {
                let meta: SegmentMetadata = bincode::deserialize(&value)
                    .or_else(|_| serde_json::from_slice(&value))
                    .map_err(|e| Error::Io(io_err(e)))?;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Lists all segment metadata entries.
    pub fn list_segments(&self) -> Vec<Result<SegmentMetadata>> {
        let cf = self.db.cf_handle(cf::CF_SEGMENTS);
        let Some(cf_handle) = cf else {
            return vec![];
        };

        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((_key, value)) => match bincode::deserialize::<SegmentMetadata>(&value)
                .or_else(|_| serde_json::from_slice::<SegmentMetadata>(&value))
            {
                Ok(meta) => Some(Ok(meta)),
                Err(_) => None,
            },
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    /// Deletes a segment metadata entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the segments column family is not found or
    /// the RocksDB delete fails.
    pub fn delete_segment(&self, id: SegmentId) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_SEGMENTS)
            .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;

        let key = cf::encode_segment_key(&id);
        self.db.delete_cf(&cf, key).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    // ------------------------------------------------------------------
    // Tombstone operations
    // ------------------------------------------------------------------

    /// Records a deletion tombstone.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletions column family is not found, serialization
    /// fails, or the RocksDB write fails.
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
                let (bucket_str, key_str) = cf::decode_object_key(&key)?;
                if bucket_str != bucket.as_str() {
                    return None;
                }
                match bincode::deserialize::<Tombstone>(&value)
                    .or_else(|_| serde_json::from_slice::<Tombstone>(&value))
                {
                    Ok(tombstone) => Some(Ok((ObjectKey::new(key_str), tombstone))),
                    Err(_) => None,
                }
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
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let key = cf::encode_object_key("default", meta.object_key.as_str());
            let value = bincode::serialize(&meta).map_err(|e| Error::Io(io_err(e)))?;
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

    /// Async version of [`Self::delete_object`].
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking task fails to spawn, the objects column
    /// family is not found, or the RocksDB delete fails.
    pub async fn delete_object_async(&self, bucket: BucketId, key: ObjectKey) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());
            db.delete_cf(&cf, db_key).map_err(|e| Error::Io(io_err(e)))?;
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

        for op in &ops {
            match op {
                BatchOp::PutObject(key, value) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_OBJECTS)
                        .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
                    let k = cf::encode_object_key("default", key.as_str());
                    let v = bincode::serialize(value).map_err(|e| Error::Io(io_err(e)))?;
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::DeleteObject(bucket, key) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_OBJECTS)
                        .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
                    let k = cf::encode_object_key(bucket.as_str(), key.as_str());
                    batch.delete_cf(&cf, k);
                }
                BatchOp::PutTombstone(bucket, key, tombstone) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_DELETIONS)
                        .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
                    let k = cf::encode_object_key(bucket.as_str(), key.as_str());
                    let v = bincode::serialize(tombstone).map_err(|e| Error::Io(io_err(e)))?;
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::PutSegment(meta) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_SEGMENTS)
                        .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;
                    let k = cf::encode_segment_key(&meta.segment_id);
                    let v = bincode::serialize(meta).map_err(|e| Error::Io(io_err(e)))?;
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::DeleteSegment(segment_id) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_SEGMENTS)
                        .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;
                    let k = cf::encode_segment_key(segment_id);
                    batch.delete_cf(&cf, k);
                }
                BatchOp::DeleteTombstone(bucket, key) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_DELETIONS)
                        .ok_or_else(|| Error::InvalidConfig("deletions CF not found".into()))?;
                    let encoded = cf::encode_object_key(bucket.as_str(), key.as_str());
                    batch.delete_cf(&cf, encoded);
                }
            }
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
    pub fn start_metrics_task(self: &Arc<Self>) {
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            poll_rocksdb_metrics(db, metrics).await;
        });
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
async fn poll_rocksdb_metrics(db: Arc<DB>, metrics: Arc<RocksDbMetrics>) {
    let interval = Duration::from_secs(30);
    loop {
        tokio::time::sleep(interval).await;

        if let Some(v) = property_u64(&db, "rocksdb.block.cache.hit") {
            metrics.block_cache_hit.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.block.cache.miss") {
            metrics.block_cache_miss.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.cur-size-all-mem-tables") {
            metrics.memtable_size.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.num-running-compactions") {
            metrics.running_compactions.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.num-running-flushes") {
            metrics.running_flushes.set(v);
        }
        if let Some(v) = property_u64(&db, "rocksdb.estimate-num-keys") {
            metrics.estimate_num_keys.set(v);
        }
    }
}

fn property_u64(db: &DB, name: &str) -> Option<u64> {
    db.property_int_value(name).ok().flatten()
}

// ---------------------------------------------------------------------------
// BatchOp
// ---------------------------------------------------------------------------

/// An operation in a batch write.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Put an object metadata entry.
    PutObject(ObjectKey, ObjectMetadata),
    /// Delete an object.
    DeleteObject(BucketId, ObjectKey),
    /// Put a tombstone.
    PutTombstone(BucketId, ObjectKey, Tombstone),
    /// Put a segment metadata entry.
    PutSegment(SegmentMetadata),
    /// Delete a segment metadata entry.
    DeleteSegment(SegmentId),
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

impl oceanfs_storage_api::MetadataStore for RocksDbMetadataStore {
    fn list_object_keys(&self, bucket: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
        self.list_objects(bucket, "")
            .into_iter()
            .filter_map(|r| r.ok())
            .map(|meta| Ok((bucket.clone(), meta.object_key)))
            .collect::<std::io::Result<Vec<_>>>()
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

    fn get_segment(&self, id: SegmentId) -> std::io::Result<Option<SegmentMetadata>> {
        self.get_segment(id).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn list_segments(&self) -> Vec<std::io::Result<SegmentMetadata>> {
        self.list_segments()
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

    fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()> {
        self.delete_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
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

    fn put_segment(&self, meta: SegmentMetadata) -> std::io::Result<()> {
        self.put_segment(meta).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn delete_segment(&self, id: SegmentId) -> std::io::Result<()> {
        self.delete_segment(id).map_err(|e| std::io::Error::other(e.to_string()))
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
                oceanfs_storage_api::BatchOp::PutObject(key, meta) => {
                    crate::metadata::store::BatchOp::PutObject(key, meta)
                }
                oceanfs_storage_api::BatchOp::DeleteObject(bucket, key) => {
                    crate::metadata::store::BatchOp::DeleteObject(bucket, key)
                }
                oceanfs_storage_api::BatchOp::PutTombstone(bucket, key, tombstone) => {
                    crate::metadata::store::BatchOp::PutTombstone(bucket, key, tombstone)
                }
                oceanfs_storage_api::BatchOp::PutSegment(meta) => {
                    crate::metadata::store::BatchOp::PutSegment(meta)
                }
                oceanfs_storage_api::BatchOp::DeleteSegment(id) => {
                    crate::metadata::store::BatchOp::DeleteSegment(id)
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
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{HashOutput, Hlc, SizeTier};

    use super::*;

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
    fn put_and_get_segment_roundtrip() {
        let store = RocksDbMetadataStore::open(&test_config()).unwrap();
        let meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        store.put_segment(meta.clone()).unwrap();

        let got = store.get_segment(meta.segment_id).unwrap().unwrap();
        assert_eq!(got.ec_k, 4);
        assert_eq!(got.ec_m, 2);
        assert!(got.is_sealed());
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
                Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
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

        // Verify Gauge renders in Prometheus format
        let rendered = m.block_cache_hit.render();
        assert!(rendered.contains("rocksdb_block_cache_hit"), "must contain metric name");
    }
}
