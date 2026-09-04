//! Trait impl bridge: implements `SegmentDataStore` using segment files
//! from the owning data pool root (ADR-0029 f5, ADR-0031 pools-only).
//!
//! Replaces the previous `BlobStore` bridge (`blob_store_impl.rs`) which
//! read from a redundant `blobs/` directory.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::Error;

use crate::anti_entropy::SegmentDataStore;

/// A `SegmentDataStore` backed by segment files under a data pool root.
///
/// Reads the full raw segment data (skipping the header). Writes include
/// a minimal v1 header for compatibility. The pool root is resolved per
/// segment from its durable `pool_id` (injected resolver backed by the
/// lifecycle registry).
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
    /// surfaced as an explicit `InvalidConfig` data-integrity error —
    /// there is no legacy `data_dir` fallback (ADR-0031 D2).
    pub fn new(
        data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
        pool_id_for: oceanfs_storage::PoolIdResolver,
    ) -> Self {
        Self { data_pools, pool_id_for }
    }

    /// Resolves the directory holding a segment's `.dat` (f5): the
    /// owning pool root. Plain join over the pool snapshot — no locks,
    /// no I/O (f5 perf 2.3/7.2).
    ///
    /// A missing resolver mapping (a segment not yet registered in the
    /// lifecycle) defaults to pool 0 — the first configured pool. This
    /// is the **write-before-register bridge**: the sealed-segment push
    /// receiver and the re-rep worker persist a replica's `.dat` before
    /// `request_reserve`/`request_seal` and stamp `pool_id: 0` in the
    /// durable metadata (sealed-segment-replication, ADR-0030). The
    /// bridge disappears with those flows when store-unification f2
    /// (ADR-0032 D3) serializes and lifecycle-routes every `.dat` write.
    fn resolve(&self, segment_id: &SegmentId) -> std::result::Result<std::path::PathBuf, Error> {
        let pool_id = (self.pool_id_for)(segment_id).unwrap_or(0);
        oceanfs_storage::resolve_pool_root(&self.data_pools, pool_id).ok_or_else(|| {
            Error::InvalidConfig(format!("segment {segment_id} references unknown pool {pool_id}"))
        })
    }
}

impl SegmentDataStore for DiskSegmentStore {
    fn read_segment_data(&self, segment_id: &SegmentId) -> std::result::Result<Bytes, Error> {
        let path = self.resolve(segment_id)?.join(format!("{segment_id}.dat"));
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
        let dir = self.resolve(segment_id)?;
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

    /// A pools-only store backed by one data pool (config-order id 0)
    /// plus the mandatory wal/metadata/hints siblings; returns the store
    /// and the data pool root.
    fn pools_store(tmp: &tempfile::TempDir) -> (DiskSegmentStore, std::path::PathBuf) {
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

    #[test]
    fn write_then_read_roundtrip_is_header_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

        store.write_segment_data(&id, &data).unwrap();
        let read_back = store.read_segment_data(&id).unwrap();
        assert_eq!(&read_back[..], &data[..], "heal-written data must round-trip");

        // The file must be a valid v1 segment (the read path's strict
        // header verification accepts it), living on the pool root.
        let file = std::fs::read(data_root.join(format!("{id}.dat"))).unwrap();
        let header = oceanfs_storage::SegmentHeader::from_bytes(&file).expect("valid header");
        assert_eq!(header.version, 1);
        assert_eq!(header.data_end() as usize, 76 + data.len());
    }

    /// Pools-only resolution: a registered pool id resolves to that
    /// pool's root (f2 config-order id scheme: 0 names the first data
    /// pool); an id no registered pool carries is an explicit
    /// `InvalidConfig` error — never a legacy `data_dir` fallback
    /// (ADR-0031 D2).
    #[test]
    fn resolve_uses_pool_id_to_pick_root() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, data_root) = pools_store(&tmp);
        let id = SegmentId::new();

        let store_mapped = DiskSegmentStore::new(store.data_pools.clone(), Arc::new(|_| Some(0)));
        assert_eq!(
            store_mapped.resolve(&id).unwrap(),
            data_root,
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
            data_root,
            "unmapped segments land on pool 0 (bridge until store-unification f2)"
        );
    }
}
