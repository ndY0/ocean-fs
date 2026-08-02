//! Segment sealer — finalizes active segments into immutable sealed segments.
//!
//! The sealer monitors active segments for fullness or timeout, builds the
//! blob index, writes the segment to disk, truncates the WAL, and persists
//! segment metadata to the metadata store.

use std::{path::PathBuf, sync::Arc};

use oceanfs_core::SegmentMetadata;
#[cfg(test)]
use oceanfs_core::{SegmentSizeConfig, SizeTier};

use crate::{
    error::{Error, Result},
    metadata::MetadataStore,
    segment::{
        buffer::ActiveSegment,
        handle::SegmentHandle,
        header::SegmentHeader,
        index::{SegmentIndex, SegmentIndexEntry},
    },
    wal::WalWriter,
    MerkleTree,
};

/// Configuration for the segment sealer.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SealConfig {
    /// Target size in bytes — seal when the segment exceeds this.
    pub target_size_bytes: u64,
    /// Maximum time in milliseconds before sealing a non-empty segment.
    pub seal_timeout_ms: u64,
    /// Directory where sealed segment files are written.
    pub data_dir: PathBuf,
}

/// Orchestrates the sealing of active segments.
/// Orchestrates the sealing of active segments.
#[allow(dead_code)]
pub struct SegmentSealer {
    config: SealConfig,
    metadata: Arc<MetadataStore>,
    wal: Arc<WalWriter>,
}

#[allow(dead_code)]
impl SegmentSealer {
    /// Creates a new segment sealer.
    pub fn new(config: SealConfig, metadata: Arc<MetadataStore>, wal: Arc<WalWriter>) -> Self {
        Self { config, metadata, wal }
    }

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

        self.seal(active, entries).await.map(Some)
    }

    /// Seals an active segment unconditionally.
    async fn seal(
        &self,
        active: &mut ActiveSegment,
        entries: &[SegmentIndexEntry],
    ) -> Result<SegmentHandle> {
        let segment_id = active.id();
        let tier = active.tier();
        let data = active.data().to_vec();
        let size = active.size();
        let blob_count = entries.len() as u32;

        // Build the blob index from the provided entries.
        let index = SegmentIndex::new(entries.to_vec())?;

        // Compute checksum (BLAKE3 of segment data).
        let checksum = blake3::hash(&data);
        let checksum_bytes: [u8; 32] = *checksum.as_bytes();

        // Build Merkle tree for anti-entropy integrity verification.
        // Uses the default 64 KB leaf size for consistent tree construction.
        let merkle_root = MerkleTree::build(&data, 65536).map(|tree| tree.root().hash());

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
        tokio::fs::write(&path, &file_data).await?;

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
            MetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
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
            MetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
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
}
