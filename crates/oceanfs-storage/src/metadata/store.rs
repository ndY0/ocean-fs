//! RocksDB-backed metadata store with strongly-typed CRUD.

use std::sync::Arc;

use oceanfs_core::{
    BucketId, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId, SegmentMetadata, Tombstone,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::{
    error::{Error, Result},
    metadata::cf,
};

/// A RocksDB-backed metadata store with three column families.
///
/// Manages object metadata (`objects` CF), segment metadata (`segments` CF),
/// and deletion tombstones (`deletions` CF).
///
/// # Examples
///
/// ```ignore
/// // Requires a running RocksDB instance; examples are in unit tests.
/// use oceanfs_core::{MetadataConfig, ObjectKey, BucketId, Hlc};
/// use oceanfs_storage::MetadataStore;
/// let config = MetadataConfig::default();
/// let store = MetadataStore::open(&config).unwrap();
/// ```
pub struct MetadataStore {
    db: Arc<DB>,
}

fn io_err(e: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl MetadataStore {
    /// Opens or creates a metadata store at the given data directory.
    ///
    /// # Errors
    ///
    /// Returns an error if RocksDB cannot open the database or create
    /// the required column families.
    ///
    /// # Panics
    ///
    /// Panics if a column family handle is missing after creation
    /// (indicates a programming error).
    pub fn open(config: &MetadataConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.increase_parallelism(num_cpus::get() as i32);
        opts.optimize_level_style_compaction(config.memtable_size);

        // Configure block cache.
        let block_cache =
            rocksdb::Cache::new_lru_cache(config.block_cache_size);
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(&block_cache);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf::ALL_COLUMN_FAMILIES
            .iter()
            .map(|&name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
                cf_opts.set_block_based_table_factory(&block_opts);
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&opts, &config.data_dir, cf_descriptors)
            .map_err(|e| Error::Io(io_err(e)))?;

        Ok(Self { db: Arc::new(db) })
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    /// Stores object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying RocksDB write fails.
    pub fn put_object(&self, meta: ObjectMetadata) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let key = cf::encode_object_key("default", meta.object_key.as_str());
        let value = serde_json::to_vec(&meta).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Retrieves object metadata.
    ///
    /// Returns `None` if the object does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB read or deserialization fails.
    pub fn get_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        match self.db.get_cf(&cf, db_key) {
            Ok(Some(value)) => {
                let meta: ObjectMetadata =
                    serde_json::from_slice(&value).map_err(|e| Error::Io(io_err(e)))?;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Deletes object metadata.
    ///
    /// This is a soft delete — the data remains in segments until GC.
    /// Use [`Self::put_tombstone`] to record the deletion for GC.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB delete fails.
    pub fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_OBJECTS)
            .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;

        let db_key = cf::encode_object_key(bucket.as_str(), key.as_str());

        self.db.delete_cf(&cf, db_key).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Lists objects whose keys start with the given prefix.
    ///
    /// Results are in key order. Suitable for S3 `LIST` operations.
    ///
    /// # Errors
    ///
    /// Individual entries that fail to deserialize are skipped.
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
            Ok((_key, value)) => match serde_json::from_slice::<ObjectMetadata>(&value) {
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
    /// Returns an error if the RocksDB write fails.
    pub fn put_segment(&self, meta: SegmentMetadata) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf::CF_SEGMENTS)
            .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;

        let key = cf::encode_segment_key(&meta.segment_id);
        let value = serde_json::to_vec(&meta).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Retrieves segment metadata.
    ///
    /// Returns `None` if the segment does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB read or deserialization fails.
    pub fn get_segment(&self, id: SegmentId) -> Result<Option<SegmentMetadata>> {
        let cf = self
            .db
            .cf_handle(cf::CF_SEGMENTS)
            .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;

        let key = cf::encode_segment_key(&id);

        match self.db.get_cf(&cf, key) {
            Ok(Some(value)) => {
                let meta: SegmentMetadata =
                    serde_json::from_slice(&value).map_err(|e| Error::Io(io_err(e)))?;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::Io(io_err(e))),
        }
    }

    /// Lists all segment metadata entries.
    ///
    /// # Errors
    ///
    /// Individual entries that fail to deserialize are skipped.
    pub fn list_segments(&self) -> Vec<Result<SegmentMetadata>> {
        let cf = self.db.cf_handle(cf::CF_SEGMENTS);
        let Some(cf_handle) = cf else {
            return vec![];
        };

        let iter = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);

        iter.filter_map(|item| match item {
            Ok((_key, value)) => match serde_json::from_slice::<SegmentMetadata>(&value) {
                Ok(meta) => Some(Ok(meta)),
                Err(_) => None,
            },
            Err(e) => Some(Err(Error::Io(io_err(e)))),
        })
        .collect()
    }

    // ------------------------------------------------------------------
    // Tombstone operations
    // ------------------------------------------------------------------

    /// Records a deletion tombstone.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB write fails.
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
        let value = serde_json::to_vec(&tombstone).map_err(|e| Error::Io(io_err(e)))?;

        self.db.put_cf(&cf, db_key, value).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }

    /// Checks if a deletion tombstone exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB read fails.
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

    // ------------------------------------------------------------------
    // Async wrappers
    // ------------------------------------------------------------------

    /// Async version of [`Self::put_object`].
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB write fails or the blocking
    /// task panics.
    pub async fn put_object_async(&self, meta: ObjectMetadata) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(cf::CF_OBJECTS)
                .ok_or_else(|| Error::InvalidConfig("objects CF not found".into()))?;
            let key = cf::encode_object_key("default", meta.object_key.as_str());
            let value = serde_json::to_vec(&meta).map_err(|e| Error::Io(io_err(e)))?;
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
    /// Returns an error if the RocksDB read fails or the blocking
    /// task panics.
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
                    let meta: ObjectMetadata =
                        serde_json::from_slice(&value).map_err(|e| Error::Io(io_err(e)))?;
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
    /// Returns an error if the RocksDB delete fails or the blocking
    /// task panics.
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
    /// All put/delete operations in the batch succeed or fail together.
    ///
    /// # Errors
    ///
    /// Returns an error if the RocksDB write batch fails.
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
                    let v = serde_json::to_vec(value).map_err(|e| Error::Io(io_err(e)))?;
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
                    let v = serde_json::to_vec(tombstone).map_err(|e| Error::Io(io_err(e)))?;
                    batch.put_cf(&cf, k, v);
                }
                BatchOp::PutSegment(meta) => {
                    let cf = self
                        .db
                        .cf_handle(cf::CF_SEGMENTS)
                        .ok_or_else(|| Error::InvalidConfig("segments CF not found".into()))?;
                    let k = cf::encode_segment_key(&meta.segment_id);
                    let v = serde_json::to_vec(meta).map_err(|e| Error::Io(io_err(e)))?;
                    batch.put_cf(&cf, k, v);
                }
            }
        }

        self.db.write(batch).map_err(|e| Error::Io(io_err(e)))?;

        Ok(())
    }
}

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
        let store = MetadataStore::open(&test_config()).unwrap();
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

    #[test]
    fn get_nonexistent_object_returns_none() {
        let store = MetadataStore::open(&test_config()).unwrap();
        let result = store.get_object(&BucketId::new("default"), &ObjectKey::new("nope")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_object_removes_it() {
        let store = MetadataStore::open(&test_config()).unwrap();
        let meta = make_object_meta("temp.txt", 100, None);
        store.put_object(meta).unwrap();
        store.delete_object(&BucketId::new("default"), &ObjectKey::new("temp.txt")).unwrap();

        let result =
            store.get_object(&BucketId::new("default"), &ObjectKey::new("temp.txt")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn list_objects_by_prefix() {
        let store = MetadataStore::open(&test_config()).unwrap();

        for name in &["a/1.txt", "a/2.txt", "b/3.txt"] {
            store.put_object(make_object_meta(name, 10, None)).unwrap();
        }

        let results = store.list_objects(&BucketId::new("default"), "a/");
        let results: Vec<_> = results.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn put_and_get_segment_roundtrip() {
        let store = MetadataStore::open(&test_config()).unwrap();
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
        let store = MetadataStore::open(&test_config()).unwrap();
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
        let store = MetadataStore::open(&test_config()).unwrap();
        assert!(!store.has_tombstone(&BucketId::new("default"), &ObjectKey::new("nope")).unwrap());
    }
}
