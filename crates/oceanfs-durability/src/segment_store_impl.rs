//! Trait impl bridge: implements `SegmentDataStore` using segment files
//! from the authoritative segments directory — the legacy `segments/`
//! dir or a data pool root (ADR-0029 f5).
//!
//! Replaces the previous `BlobStore` bridge (`blob_store_impl.rs`) which
//! read from a redundant `blobs/` directory.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::Error;

use crate::anti_entropy::SegmentDataStore;

// [review][cleanup][high]
// no legacy mode
// [end]
/// A `SegmentDataStore` backed by segment files under the legacy
/// segments dir or a data pool root.
///
/// Reads the full raw segment data (skipping the 76-byte header).
/// Writes include a minimal 76-byte header for compatibility. The pool
/// root is resolved per segment from its durable `pool_id` (injected
/// resolver backed by the lifecycle registry); pool_id 0 / unknown ids
/// resolve to the legacy dir.
pub struct DiskSegmentStore {
    /// Data pool roots (ADR-0029 f5). Empty = legacy mode.
    data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
    /// Legacy segments directory (pool_id 0 / no pools).
    legacy_dir: std::path::PathBuf,
    /// Resolves a segment's durable pool id; `None`/0 → legacy dir.
    pool_id_for: oceanfs_storage::PoolIdResolver,
}

impl DiskSegmentStore {
    /// Creates a pool-aware store rooted at `legacy_dir` with the node's
    /// data pools and the pool-id resolver (ADR-0029 f5).
    ///
    /// `legacy_dir` is the directory containing `{segment_id}.dat` files
    /// written by the legacy path; in pool mode each segment resolves to
    /// the root of the pool its metadata names.
    pub fn new(
        data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
        legacy_dir: std::path::PathBuf,
        pool_id_for: oceanfs_storage::PoolIdResolver,
    ) -> Self {
        Self { data_pools, legacy_dir, pool_id_for }
    }

    /// Resolves the directory holding a segment's `.dat` (f5): the
    /// owning pool root, or the legacy dir when pool_id is 0/unknown or
    /// no pools are configured. Plain join over the pool snapshot — no
    /// locks, no I/O (f5 perf 2.3/7.2).
    fn resolve(&self, segment_id: &SegmentId) -> std::path::PathBuf {
        let pool_id = if self.data_pools.is_empty() {
            0
        } else {
            (self.pool_id_for)(segment_id).unwrap_or(0)
        };
        oceanfs_storage::resolve_pool_root(&self.data_pools, pool_id, &self.legacy_dir)
    }
}

impl SegmentDataStore for DiskSegmentStore {
    fn read_segment_data(&self, segment_id: &SegmentId) -> std::result::Result<Bytes, Error> {
        let path = self.resolve(segment_id).join(format!("{segment_id}.dat"));
        // Preserve the original io::Error (including its ErrorKind): scrub
        // distinguishes NotFound (shard not yet sealed / already reclaimed —
        // NOT corruption) from genuine I/O failures by matching the kind.
        // Wrapping with `Error::other(format!(...))` would collapse every
        // error into ErrorKind::Other and hide the NotFound signal.
        let data = std::fs::read(&path).map_err(Error::Io)?;

        // The on-disk header size depends on the format version (v1 = 76
        // bytes, v2 = 92 bytes); the returned data is the DATA section
        // only (header..index_offset — excluding any parity section).
        let header = oceanfs_storage::SegmentHeader::from_bytes(&data).ok_or_else(|| {
            Error::InvalidConfig(format!("segment file {segment_id} has a bad header"))
        })?;
        let hdr_size = oceanfs_storage::SegmentHeader::header_size(header.version);
        let data_end = header.data_end() as usize;
        if data.len() < hdr_size || data_end > data.len() {
            return Err(Error::InvalidConfig(format!(
                "segment file {segment_id} too short: {} bytes",
                data.len()
            )));
        }
        Ok(Bytes::from(data[hdr_size..data_end].to_vec()))
    }

    // [review][architecture][critical]
    // this does not use the disk optmimisations.
    // also, this only handle the v1 format (format not carrying the processed payload real size, for exemple after decompression),
    // what happens when we write a compressed segment after compaction ? we need to stop the versionning approach, this does not feet the problem :
    // we are no in production yet : we do not version, we refactor. if we need to carry more information, we need to factor it in previous code, no shortcut..
    // [end]
    fn write_segment_data(
        &self,
        segment_id: &SegmentId,
        data: &[u8],
    ) -> std::result::Result<(), Error> {
        let dir = self.resolve(segment_id);
        let path = dir.join(format!("{segment_id}.dat"));
        // Ensure the segment directory exists — the gRPC append_segment
        // handler must be able to write segments even if no local segment
        // sealing has ever created the directory.
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        // Write a valid v1 segment file: the read path verifies the
        // header (magic, version, checksum) on first touch, so a
        // zeroed header would be rejected as corrupt. Heal/anti-entropy
        // repaired segments therefore carry a real v1 header.
        let mut file_data = vec![0u8; 76];
        file_data[0..4].copy_from_slice(b"OFSG");
        file_data[4..6].copy_from_slice(&1u16.to_le_bytes());
        file_data[22..30].copy_from_slice(&(data.len() as u64).to_le_bytes());
        file_data[30..34].copy_from_slice(&0u32.to_le_bytes()); // blob_count
        file_data[34..42].copy_from_slice(&((76 + data.len()) as u64).to_le_bytes());
        let checksum = *blake3::hash(data).as_bytes();
        file_data[42..74].copy_from_slice(&checksum);
        file_data.extend_from_slice(data);
        std::fs::write(&path, &file_data)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    fn legacy_store(dir: &std::path::Path) -> DiskSegmentStore {
        DiskSegmentStore::new(Vec::new(), dir.to_path_buf(), Arc::new(|_| None))
    }

    #[test]
    fn write_then_read_roundtrip_is_header_valid() {
        let dir = tempfile::tempdir().unwrap();
        let store = legacy_store(dir.path());
        let id = SegmentId::new();
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

        store.write_segment_data(&id, &data).unwrap();
        let read_back = store.read_segment_data(&id).unwrap();
        assert_eq!(&read_back[..], &data[..], "heal-written data must round-trip");

        // The file must be a valid v1 segment (the read path's strict
        // header verification accepts it).
        let file = std::fs::read(dir.path().join(format!("{id}.dat"))).unwrap();
        let header = oceanfs_storage::SegmentHeader::from_bytes(&file).expect("valid header");
        assert_eq!(header.version, 1);
        assert_eq!(header.data_end() as usize, 76 + data.len());
    }

    /// Pool mode: a known pool id resolves to that pool's root; an
    /// unknown id and the no-pools case fall back to the legacy dir
    /// (ADR-0029 f5 resolve — the f2 config-order id scheme: 0 names the
    /// first data pool, never the legacy root, when pools exist).
    #[test]
    fn resolve_uses_pool_id_to_pick_root() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = tmp.path().join("segments");
        let pool_root = tmp.path().join("pool-1");
        std::fs::create_dir_all(&pool_root).unwrap();

        // Build the pool through the public registry API (probe + capacity).
        let data_dir = tmp.path().join("data");
        let registry = oceanfs_storage::PoolRegistry::from_config(
            &oceanfs_core::StorageConfig {
                pools: vec![
                    oceanfs_core::StoragePoolConfig {
                        name: "pool-1".into(),
                        role: oceanfs_core::PoolRole::Data,
                        root: pool_root.clone(),
                        weight: Some(1),
                        tech: oceanfs_core::PoolTech::Auto,
                        health: Default::default(),
                    },
                    oceanfs_core::StoragePoolConfig {
                        name: "journal".into(),
                        role: oceanfs_core::PoolRole::Wal,
                        root: tmp.path().join("optane0"),
                        weight: Some(1),
                        tech: oceanfs_core::PoolTech::Auto,
                        health: Default::default(),
                    },
                    oceanfs_core::StoragePoolConfig {
                        name: "meta".into(),
                        role: oceanfs_core::PoolRole::Metadata,
                        root: tmp.path().join("optane1"),
                        weight: Some(1),
                        tech: oceanfs_core::PoolTech::Auto,
                        health: Default::default(),
                    },
                    oceanfs_core::StoragePoolConfig {
                        name: "hints".into(),
                        role: oceanfs_core::PoolRole::Hints,
                        root: tmp.path().join("hints0"),
                        weight: Some(1),
                        tech: oceanfs_core::PoolTech::Auto,
                        health: Default::default(),
                    },
                ],
                missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
            },
            &data_dir,
        )
        .expect("registry");
        let pools = registry.data_pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id(), 0, "config-order id");

        let store = DiskSegmentStore::new(pools.clone(), legacy.clone(), Arc::new(|_| Some(0)));
        let id = SegmentId::new();
        assert_eq!(
            store.resolve(&id),
            pool_root,
            "a registered segment must resolve to its pool root"
        );

        // Unknown pool id (stale mapping) → the legacy dir.
        let store_unknown =
            DiskSegmentStore::new(pools.clone(), legacy.clone(), Arc::new(|_| Some(99)));
        assert_eq!(
            store_unknown.resolve(&id),
            legacy,
            "an unknown pool id falls back to the legacy dir"
        );

        // No pools configured (legacy mode) → the legacy dir for every id.
        let store_legacy = DiskSegmentStore::new(Vec::new(), legacy.clone(), Arc::new(|_| None));
        assert_eq!(store_legacy.resolve(&id), legacy);
    }
}
