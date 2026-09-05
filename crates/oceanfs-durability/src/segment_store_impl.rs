//! Trait impl bridge: implements `oceanfs_storage_api::SegmentDataStore`
//! using segment files from the owning data pool root (ADR-0029 f5,
//! ADR-0031 pools-only).
//!
//! Transition note (store-unification f1 → f2): during the dual-impl
//! window this durability-side `DiskSegmentStore` and the
//! `DiskSegmentShardStore` in `gc/garbage_collector.rs` BOTH implement
//! the unified trait (ADR-0032: "dual-impl during transition is
//! acceptable"). The impl merge into a single
//! `oceanfs_storage::DiskSegmentStore` (io-layer reads/writes,
//! per-segment write serialization) is store-unification f2; this file
//! is deleted there.
//!
//! Replaces the previous `BlobStore` bridge (`blob_store_impl.rs`) which
//! read from a redundant `blobs/` directory.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::{resolve_pool_root, SegmentHeader};
use oceanfs_storage_api::{
    error::{Error, Result},
    SegmentDataStore, SegmentFile,
};

/// A `SegmentDataStore` backed by segment files under a data pool root.
///
/// Reads the full raw segment data (parsed header + data section).
/// Writes include a minimal v1 header for compatibility. The pool root
/// is resolved per segment from its durable `pool_id` (injected
/// resolver backed by the lifecycle registry).
///
/// This is the transition-window impl (see the module doc); f2 merges
/// it with `DiskSegmentShardStore` into the storage-side impl.
pub struct DiskSegmentStore {
    /// Data pool roots (ADR-0029 f5). Pools are mandatory since f1
    /// (ADR-0031) — never empty.
    data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
    /// Resolves a segment's durable pool id.
    pool_id_for: oceanfs_storage::PoolIdResolver,
}

impl DiskSegmentStore {
    /// Creates a pools-only store over the node's data pools and the
    /// pool-id resolver (ADR-0029 f5).
    ///
    /// Every segment resolves to the root of the pool its metadata
    /// names. A segment whose `pool_id` no registered pool carries is
    /// surfaced as an explicit `Internal` data-integrity error — there
    /// is no legacy `data_dir` fallback (ADR-0031 D2).
    pub fn new(
        data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
        pool_id_for: oceanfs_storage::PoolIdResolver,
    ) -> Self {
        Self { data_pools, pool_id_for }
    }

    /// Resolves the `.dat` path for a segment (f5): the owning pool
    /// root joined with `{segment_id}.dat`. Plain join over the pool
    /// snapshot — no locks, no I/O (f5 perf 2.3/7.2).
    ///
    /// A missing resolver mapping (a segment not yet registered in the
    /// lifecycle) defaults to pool 0 — the first configured pool. This
    /// is the **write-before-register bridge**: the sealed-segment push
    /// receiver and the re-rep worker persist a replica's `.dat` before
    /// `request_reserve`/`request_seal` and stamp `pool_id: 0` in the
    /// durable metadata (sealed-segment-replication, ADR-0030). The
    /// bridge disappears with those flows when store-unification f2
    /// (ADR-0032 D3) serializes and lifecycle-routes every `.dat` write.
    fn resolve(&self, segment_id: &SegmentId) -> Result<PathBuf> {
        let pool_id = (self.pool_id_for)(segment_id).unwrap_or(0);
        resolve_pool_root(&self.data_pools, pool_id)
            .map(|root| root.join(format!("{segment_id}.dat")))
            .ok_or_else(|| {
                Error::Internal(format!("segment {segment_id} references unknown pool {pool_id}"))
            })
    }

    /// Resolves a segment's `.dat` from a caller-held pool id — the GC
    /// fast path (no resolver call; f5 perf 1.3). The pool id must name
    /// a registered pool; an unknown id is an explicit
    /// `InvalidArgument` error.
    fn resolve_with_pool(&self, pool_id: u32, segment_id: &SegmentId) -> Result<PathBuf> {
        resolve_pool_root(&self.data_pools, pool_id)
            .map(|root| root.join(format!("{segment_id}.dat")))
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "segment {segment_id} references unknown pool {pool_id}"
                ))
            })
    }

    /// Unlinks one `.dat` file and returns the reclaimed bytes (0 when
    /// no file existed — a missing file is not an error for deletes).
    fn unlink(&self, path: &Path) -> Result<u64> {
        let metadata = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::Io(e)),
        };
        std::fs::remove_file(path).map_err(Error::Io)?;
        Ok(metadata)
    }
}

#[async_trait::async_trait]
impl SegmentDataStore for DiskSegmentStore {
    async fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Option<SegmentFile>> {
        let path = self.resolve(segment_id)?;
        // A missing `.dat` is Ok(None), not an error: scrub/heal use it
        // to distinguish "not yet sealed / already reclaimed" (NOT
        // corruption) from genuine I/O failures. Genuine I/O errors
        // keep their kind (Error::Io wraps the raw io::Error).
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };

        // The on-disk header size depends on the format version (v1 = 76
        // bytes, v2 = 92 bytes); the data section is
        // `header..data_end` (excluding any parity section — v2's
        // `data_end` is the parity offset).
        let header = SegmentHeader::from_bytes(&data).ok_or_else(|| {
            Error::Internal(format!("segment file {segment_id} has a bad header"))
        })?;
        let hdr_size = SegmentHeader::header_size(header.version);
        let data_end = header.data_end() as usize;
        if data.len() < hdr_size || data_end > data.len() {
            return Err(Error::Internal(format!(
                "segment file {segment_id} too short: {} bytes",
                data.len()
            )));
        }
        Ok(Some(SegmentFile {
            segment_id: *segment_id,
            version: header.version,
            header_len: hdr_size,
            data_end: header.data_end(),
            data: Bytes::from(data[hdr_size..data_end].to_vec()),
        }))
    }

    async fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()> {
        let path = self.resolve(segment_id)?;
        let dir = path
            .parent()
            .ok_or_else(|| Error::Internal(format!("cannot resolve directory for {segment_id}")))?
            .to_path_buf();
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

    async fn delete_shards(&self, segment_id: &SegmentId) -> Result<u64> {
        let path = self.resolve(segment_id)?;
        self.unlink(&path)
    }

    async fn delete_shards_with_pool(&self, segment_id: &SegmentId, pool_id: u32) -> Result<u64> {
        let path = self.resolve_with_pool(pool_id, segment_id)?;
        self.unlink(&path)
    }

    fn list_segment_files(&self, root: &Path) -> Result<Vec<PathBuf>> {
        let entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".dat") {
                out.push(entry.path());
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    /// A pools-only store backed by one data pool (config-order id 0)
    /// plus the mandatory wal/metadata/hints siblings; returns the store
    /// and the data pool root.
    fn pools_store(tmp: &tempfile::TempDir) -> (DiskSegmentStore, PathBuf) {
        let data_root = tmp.path().join("nvme0");
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                oceanfs_core::StoragePoolConfig {
                    name: "pool-a".into(),
                    role: oceanfs_core::PoolRole::Data,
                    root: data_root.clone(),
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
        };
        let registry =
            oceanfs_storage::PoolRegistry::from_config(&storage, &tmp.path().join("data"))
                .expect("registry");
        let pools = registry.data_pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id(), 0, "config-order id");
        let store = DiskSegmentStore::new(pools, Arc::new(|_| Some(0)));
        (store, data_root)
    }

    #[tokio::test]
    async fn write_then_read_roundtrip_is_header_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

        store.write_segment_data(&id, &data).await.unwrap();
        let read_back = store.read_segment_data(&id).await.unwrap().expect("segment present");
        assert_eq!(&read_back.data[..], &data[..], "heal-written data must round-trip");
        // The parsed header must describe the v1 file the writer
        // synthesized (76-byte header, data section at [76..76+len]).
        assert_eq!(read_back.version, 1);
        assert_eq!(read_back.header_len, 76);
        assert_eq!(read_back.data_end as usize, 76 + data.len());

        // The file must be a valid v1 segment (the read path's strict
        // header verification accepts it), living on the pool root.
        let file = std::fs::read(data_root.join(format!("{id}.dat"))).unwrap();
        let header = SegmentHeader::from_bytes(&file).expect("valid header");
        assert_eq!(header.version, 1);
        assert_eq!(header.data_end() as usize, 76 + data.len());
    }

    /// Missing `.dat` reads as `Ok(None)` — the unified trait's
    /// NotFound contract (regression for the scrub/heal distinction
    /// between "not present" and genuine I/O failure).
    #[tokio::test]
    async fn read_missing_dat_returns_ok_none() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, _) = pools_store(&tmp);
        let id = SegmentId::new();
        assert!(store.read_segment_data(&id).await.unwrap().is_none());
    }

    /// A v2 file (92-byte header with a parity section) parses into a
    /// `SegmentFile` whose data section stops at the parity offset.
    #[tokio::test]
    async fn read_parses_v2_header_data_section() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        let parity: Vec<u8> = (0..512u32).map(|i| (i % 253) as u8).collect();
        let header = oceanfs_storage::SegmentHeader::with_parity(
            id,
            data.len() as u64,
            0,
            (92 + data.len() + parity.len()) as u64,
            *blake3::hash(&data).as_bytes(),
            92 + data.len() as u64,
            parity.len() as u64,
        );
        let mut file = header.to_bytes();
        file.extend_from_slice(&data);
        file.extend_from_slice(&parity);
        std::fs::write(data_root.join(format!("{id}.dat")), file).unwrap();

        let parsed = store.read_segment_data(&id).await.unwrap().expect("segment present");
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.header_len, 92);
        assert_eq!(parsed.data_end as usize, 92 + data.len());
        assert_eq!(&parsed.data[..], &data[..]);
    }

    /// Pools-only resolution: a registered pool id resolves to that
    /// pool's root (f2 config-order id scheme: 0 names the first data
    /// pool); an id no registered pool carries is an explicit
    /// `Internal` error — never a legacy `data_dir` fallback
    /// (ADR-0031 D2).
    #[test]
    fn resolve_uses_pool_id_to_pick_root() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();

        let store_mapped = DiskSegmentStore::new(store.data_pools.clone(), Arc::new(|_| Some(0)));
        assert_eq!(
            store_mapped.resolve(&id).unwrap(),
            data_root.join(format!("{id}.dat")),
            "a registered segment must resolve to its pool root"
        );

        // An unknown pool id (stale mapping) is a data-integrity error.
        let store_unknown = DiskSegmentStore::new(store.data_pools.clone(), Arc::new(|_| Some(99)));
        let err = store_unknown.resolve(&id).expect_err("unknown pool id must error");
        assert!(
            err.to_string().contains("unknown pool 99"),
            "error must name the missing pool: {err}"
        );

        // No resolver mapping (unregistered segment — the
        // write-before-register bridge) defaults to pool 0: the first
        // configured data pool.
        let store_unmapped = DiskSegmentStore::new(store.data_pools.clone(), Arc::new(|_| None));
        assert_eq!(
            store_unmapped.resolve(&id).unwrap(),
            data_root.join(format!("{id}.dat")),
            "unmapped segments land on pool 0 (bridge until store-unification f2)"
        );
    }

    #[tokio::test]
    async fn delete_shards_removes_file_and_reports_reclaimed_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data = vec![7u8; 4096];
        store.write_segment_data(&id, &data).await.unwrap();
        let path = data_root.join(format!("{id}.dat"));
        assert!(path.exists());

        // Resolver-based delete removes the file from the owning pool.
        let reclaimed = store.delete_shards(&id).await.unwrap();
        assert_eq!(reclaimed, 4096 + 76, "header + data bytes reclaimed");
        assert!(!path.exists());

        // Deleting a missing file reports 0 (not an error).
        assert_eq!(store.delete_shards(&id).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn delete_shards_with_pool_unlinks_from_named_pool_root() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data = vec![9u8; 128];
        store.write_segment_data(&id, &data).await.unwrap();
        let path = data_root.join(format!("{id}.dat"));
        assert!(path.exists());

        assert_eq!(store.delete_shards_with_pool(&id, 0).await.unwrap(), 128 + 76);
        assert!(!path.exists());

        // Unknown pool ids are caller errors.
        let err = store.delete_shards_with_pool(&id, 42).await.expect_err("unknown pool");
        assert!(err.to_string().contains("unknown pool 42"), "{err}");
    }

    #[tokio::test]
    async fn list_segment_files_lists_dat_files_under_root() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data = vec![1u8; 64];
        // Write through the store (also creates the directory).
        store.write_segment_data(&id, &data).await.unwrap();

        // A stray non-.dat file must be ignored.
        std::fs::write(data_root.join("not-a-segment.txt"), b"x").unwrap();

        let listed = store.list_segment_files(&data_root).unwrap();
        assert_eq!(listed, vec![data_root.join(format!("{id}.dat"))]);

        // A missing root lists nothing (not an error).
        assert!(store.list_segment_files(&tmp.path().join("absent")).unwrap().is_empty());
    }
}
