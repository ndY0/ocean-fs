//! Segment sealer — finalizes active segments into immutable sealed segments.
//!
//! The sealer monitors active segments for fullness or timeout, builds the
//! blob index, writes the segment to disk, truncates the WAL, and persists
//! segment metadata to the metadata store.

use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
#[cfg(test)]
use oceanfs_core::SegmentSizeConfig;
use oceanfs_core::{Counter, LabelSet, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_hash::Blake3Hasher;

use crate::{
    error::{Error, Result},
    io::IoReadMode,
    metadata::RocksDbMetadataStore,
    segment::{
        buffer::ActiveSegment,
        handle::SegmentHandle,
        header::SegmentHeader,
        index::{SegmentIndex, SegmentIndexEntry},
    },
    wal::WalWriter,
};

/// Configuration for the segment sealer.
#[derive(Debug, Clone)]
pub struct SealConfig {
    /// Target size in bytes — seal when the segment exceeds this.
    pub target_size_bytes: u64,
    /// Maximum time in milliseconds before sealing a non-empty segment.
    pub seal_timeout_ms: u64,
    /// Directory where sealed segment files are written.
    pub data_dir: PathBuf,
    /// I/O read mode for segment data I/O.
    ///
    /// When `Direct`, segment data files are opened with `O_DIRECT`
    /// to bypass the OS page cache. When `Mmap`, segment files are
    /// memory-mapped for zero-copy reads. Default is `Buffered`.
    pub io_mode: IoReadMode,
}

/// Orchestrates the sealing of active segments.
pub struct SegmentSealer {
    config: SealConfig,
    metadata: Arc<RocksDbMetadataStore>,
    wal: Arc<WalWriter>,
    /// Segment seal error counter.
    seal_errors: Counter,
}

impl SegmentSealer {
    /// Creates a new segment sealer.
    pub fn new(
        config: SealConfig,
        metadata: Arc<RocksDbMetadataStore>,
        wal: Arc<WalWriter>,
    ) -> Self {
        Self {
            config,
            metadata,
            wal,
            seal_errors: Counter::new(
                "segment_seal_errors_total".into(),
                "Number of segment sealing failures".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Sets an optional blob store for unified segment data access.
    ///
    /// Attempts to seal an active segment.
    ///
    /// `entries` are the blob index entries mapping (offset, length, hash) for
    /// each blob stored in this segment. The caller (write path) computes the
    /// blob key hashes.
    ///
    /// Returns `None` if the segment is not ready to seal (not full, not timed out,
    /// or empty). Returns a `SegmentHandle` on successful seal.
    ///
    /// # Errors
    ///
    /// Returns an error if the seal process fails (disk I/O, metadata write, etc.).
    pub async fn try_seal(
        &self,
        active: &mut ActiveSegment,
        elapsed_ms: u64,
        entries: &[SegmentIndexEntry],
    ) -> Result<Option<SegmentHandle>> {
        // Don't seal empty segments.
        if active.size() == 0 {
            return Ok(None);
        }

        // Check seal conditions.
        let should_seal = active.is_full() || elapsed_ms >= self.config.seal_timeout_ms;
        if !should_seal {
            return Ok(None);
        }

        let result = self.seal(active, entries).await;
        if result.is_err() {
            self.seal_errors.inc();
        }
        result.map(Some)
    }

    /// Seals an active segment unconditionally.
    async fn seal(
        &self,
        active: &mut ActiveSegment,
        entries: &[SegmentIndexEntry],
    ) -> Result<SegmentHandle> {
        let segment_id = active.id();
        let tier = active.tier();
        let data = Bytes::copy_from_slice(active.data());
        self.seal_from_data(segment_id, tier, data, entries).await
    }

    /// Seals a segment from raw data bytes, without requiring an `ActiveSegment`.
    ///
    /// This is the primary sealing entry point — works with segments that have
    /// already been extracted from the pool. Accepts the segment's identity,
    /// data bytes, tier, and blob index entries. Writes the segment file to
    /// disk, persists metadata, and truncates the WAL past the sealed boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails, metadata persistence fails, or
    /// WAL truncation fails.
    pub async fn seal_from_data(
        &self,
        segment_id: SegmentId,
        tier: SizeTier,
        data: Bytes,
        entries: &[SegmentIndexEntry],
    ) -> Result<SegmentHandle> {
        let size = data.len() as u64;
        let blob_count = entries.len() as u32;

        // Build the blob index from the provided entries.
        let index = SegmentIndex::new(entries.to_vec())?;

        // Compute checksum (BLAKE3 of segment data).
        let checksum = Blake3Hasher::hash(&data);
        let checksum_bytes: [u8; 32] = *checksum.as_bytes();

        // Build Merkle tree for anti-entropy integrity verification.
        // The Merkle tree is now computed by oceanfs-durability (ADR-0009).
        let merkle_root: Option<oceanfs_core::HashOutput> = None;

        // Serialize header and index.
        let header = SegmentHeader::new(segment_id, size, blob_count, size, checksum_bytes);
        let header_bytes = header.to_bytes();
        let index_bytes = index.to_bytes();

        // Write segment file: header + data + index.
        let path = self.config.data_dir.join(format!("{segment_id}.dat"));
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file_data = Vec::with_capacity(header_bytes.len() + data.len() + index_bytes.len());
        file_data.extend_from_slice(&header_bytes);
        file_data.extend_from_slice(&data);
        file_data.extend_from_slice(&index_bytes);

        // Select the I/O path based on the configured read mode.
        match self.config.io_mode {
            IoReadMode::Direct => {
                // O_DIRECT requires the buffer to be 512-byte aligned
                // AND the I/O size to be a multiple of 512 bytes.
                const BLOCK_SIZE: usize = 512;
                let pad = (BLOCK_SIZE - (file_data.len() % BLOCK_SIZE)) % BLOCK_SIZE;
                file_data.resize(file_data.len() + pad, 0);

                #[cfg(target_os = "linux")]
                {
                    use std::io::Write;

                    use crate::io::{direct::OpenOptionsDirectExt, DirectIoBuf};
                    // Copy into a page-aligned buffer for O_DIRECT.
                    let mut aligned = DirectIoBuf::new(file_data.len())?;
                    aligned.copy_from_slice(&file_data);

                    let mut file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .with_direct()
                        .open(&path)?;
                    file.write_all(aligned.as_bytes())?;
                    file.flush()?;
                }
                #[cfg(not(target_os = "linux"))]
                {
                    tokio::fs::write(&path, &file_data).await?;
                }
            }
            _ => {
                tokio::fs::write(&path, &file_data).await?;
            }
        }

        // Persist segment metadata.
        let meta = SegmentMetadata {
            segment_id,
            ec_k: 0, // set during EC encoding (Phase 3)
            ec_m: 0,
            size_tier: tier,
            merkle_root,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ),
        };
        self.metadata
            .put_segment(meta)
            .map_err(|e| Error::Io(std::io::Error::other(format!("metadata write failed: {e}"))))?;

        // Truncate the WAL (entries for this segment are no longer needed).
        let wal_pos = self.wal.global_position().await;
        self.wal.truncate(wal_pos).await?;

        Ok(SegmentHandle::new(segment_id, vec![]))
    }

    /// Registers the segment sealer counter with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.seal_errors.clone());
    }

    /// Returns a reference to the WAL writer for crash-recovery
    /// durability. Callers use this to append WAL entries alongside
    /// active segment writes.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::WalConfig;

    use super::*;
    use crate::buffer_pool::BufferPool;

    async fn setup() -> (SegmentSealer, ActiveSegment, Vec<SegmentIndexEntry>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );

        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
            })
            .await
            .unwrap(),
        );

        let config = SealConfig {
            target_size_bytes: 100,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Buffered,
        };

        let pool = BufferPool::new(65536, 4);
        let size_config =
            SegmentSizeConfig { default_target_size: 100, ..SegmentSizeConfig::default() };
        let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();

        // Write some data so it's not empty.
        active.append(&[0u8; 50]).unwrap();

        // Build an index entry covering the appended data.
        let entries = vec![SegmentIndexEntry { offset: 0, length: 50, blob_key_hash: [0xAB; 32] }];

        let sealer = SegmentSealer::new(config, metadata, wal);
        (sealer, active, entries, dir)
    }

    #[tokio::test]
    async fn try_seal_returns_none_when_not_full_and_not_timed_out() {
        let (sealer, mut active, entries, _dir) = setup().await;
        let result = sealer.try_seal(&mut active, 0, &entries).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn try_seal_returns_handle_when_full() {
        let (sealer, mut active, entries, _dir) = setup().await;
        // Fill it up.
        active.append(&[0u8; 60]).unwrap();
        assert!(active.is_full());

        let result = sealer.try_seal(&mut active, 0, &entries).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn try_seal_returns_handle_when_timed_out() {
        let (sealer, mut active, entries, _dir) = setup().await;
        // Not full, but timed out.
        let result = sealer.try_seal(&mut active, 2000, &entries).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn try_seal_returns_none_for_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(
            RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
            })
            .await
            .unwrap(),
        );
        let config = SealConfig {
            target_size_bytes: 100,
            seal_timeout_ms: 1000,
            data_dir: dir.path().join("segments"),
            io_mode: IoReadMode::Buffered,
        };
        let pool = BufferPool::new(65536, 4);
        let size_config =
            SegmentSizeConfig { default_target_size: 100, ..SegmentSizeConfig::default() };
        let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();
        let sealer = SegmentSealer::new(config, metadata, wal);

        // Empty segment should not seal.
        let result = sealer.try_seal(&mut active, 2000, &[]).await.unwrap();
        assert!(result.is_none());
    }

    // --- Metrics tests ---

    #[tokio::test]
    async fn register_metrics_registers_seal_errors() {
        use oceanfs_core::MetricRegistrar;

        struct TestRegistrar {
            counter_names: std::sync::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, counter: oceanfs_core::Counter) {
                self.counter_names.lock().unwrap().push(counter.name().to_string());
            }
            fn register_gauge(&self, _: oceanfs_core::Gauge) {}
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let (sealer, _active, _entries, _dir) = setup().await;
        let reg = TestRegistrar { counter_names: std::sync::Mutex::new(Vec::new()) };

        sealer.register_metrics(&reg);

        let names = reg.counter_names.lock().unwrap();
        assert!(
            names.contains(&"segment_seal_errors_total".to_string()),
            "seal_errors counter should be registered, got: {names:?}"
        );
    }
}
