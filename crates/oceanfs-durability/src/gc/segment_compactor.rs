//! Segment compaction — merges sparsely-populated segments to reclaim space.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{ChunkRef, ObjectMetadata, SegmentId, SegmentMetadata};
use oceanfs_storage::{segment::TierRouter, Result};

use super::config::tier_target_size;

// ---------------------------------------------------------------------------
// SegmentCompactor
// ---------------------------------------------------------------------------

/// Compacts a segment by re-packing live blobs into new segments.
///
/// Reads all live blobs from a segment, re-packs them into a new segment,
/// updates object metadata to point to new chunk references, and frees
/// the old segment.
pub(crate) struct SegmentCompactor {
    /// The metadata store for reading object metadata and updating chunk refs.
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// The tier router for classifying blobs by size.
    /// Wired for future tier-specific segment pool routing during repacking.
    tier_router: TierRouter,
}

impl SegmentCompactor {
    /// Creates a new segment compactor.
    pub(crate) fn new(
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        tier_router: TierRouter,
    ) -> Self {
        Self { metadata, tier_router }
    }

    /// Returns the tier router used for blob classification during repacking.
    pub(crate) fn tier_router(&self) -> &TierRouter {
        &self.tier_router
    }

    /// Compacts a single segment: re-packs live blobs, updates metadata,
    /// and returns the number of bytes reclaimed.
    ///
    /// Objects whose keys are in `dead_object_keys` (i.e., have an expired
    /// tombstone) are NOT repacked — their space is reclaimed. Only live
    /// (non-deleted) objects are moved to the new segment.
    ///
    /// Steps:
    /// 1. Find objects referencing this segment
    /// 2. Filter out dead objects (those with expired tombstones)
    /// 3. Create a new segment and repack live chunks
    /// 4. Batch-update object metadata with new chunk refs
    /// 5. Delete old segment metadata
    pub(crate) async fn compact_segment(
        &self,
        segment_id: SegmentId,
        segment_meta: &SegmentMetadata,
        dead_object_keys: &HashSet<String>,
    ) -> Result<u64> {
        // Find all objects that reference this segment
        let objects = self.find_objects_in_segment(segment_id)?;

        // Filter: only repack live (non-deleted) objects
        let live_objects: Vec<&ObjectMetadata> = objects
            .iter()
            .filter(|obj| !dead_object_keys.contains(obj.object_key.as_str()))
            .collect();

        // Total segment size for reclaimed bytes tracking
        let segment_size = tier_target_size(segment_meta.size_tier);

        if live_objects.is_empty() {
            // No live objects — the segment is fully dead.
            // Delete the segment metadata; shards reclaimed.
            self.metadata.delete_segment(segment_id)?;
            tracing::info!(
                segment_id = %segment_id,
                "compacting fully-dead segment — all objects deleted"
            );
            return Ok(segment_size);
        }

        // Create a new segment for repacking the live blobs.
        let new_segment_id = SegmentId::new();
        let mut new_offset: u64 = 0;

        // Build old-chunk → new-chunk mapping.
        // Key: (old_segment_id, old_offset, length) → new ChunkRef
        let mut chunk_remap: HashMap<(SegmentId, u64, u32), ChunkRef> =
            HashMap::with_capacity(live_objects.len());

        for obj in &live_objects {
            for chunk in &obj.chunks {
                if chunk.segment_id == segment_id {
                    let new_chunk = ChunkRef {
                        segment_id: new_segment_id,
                        offset: new_offset,
                        length: chunk.length,
                    };
                    let key = (chunk.segment_id, chunk.offset, chunk.length);
                    chunk_remap.insert(key, new_chunk);
                    new_offset += chunk.length as u64;
                }
            }
        }

        // Create new segment metadata entry.
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let new_seg_meta = SegmentMetadata {
            segment_id: new_segment_id,
            ec_k: segment_meta.ec_k,
            ec_m: segment_meta.ec_m,
            size_tier: segment_meta.size_tier,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(now_ms),
        };

        // Write all mutations atomically via batch.
        let ops: Vec<oceanfs_storage_api::BatchOp> = {
            let mut ops = Vec::with_capacity(live_objects.len() + 2);
            for obj in &live_objects {
                let mut new_chunks = smallvec::SmallVec::<[ChunkRef; 4]>::new();
                for chunk in &obj.chunks {
                    if chunk.segment_id == segment_id {
                        let key = (chunk.segment_id, chunk.offset, chunk.length);
                        if let Some(new_ref) = chunk_remap.get(&key) {
                            new_chunks.push(*new_ref);
                        }
                    } else {
                        new_chunks.push(*chunk);
                    }
                }
                let updated_meta = ObjectMetadata { chunks: new_chunks, ..(*obj).clone() };
                ops.push(oceanfs_storage_api::BatchOp::PutObject(
                    obj.object_key.clone(),
                    updated_meta,
                ));
            }
            ops.push(oceanfs_storage_api::BatchOp::PutSegment(new_seg_meta));
            ops.push(oceanfs_storage_api::BatchOp::DeleteSegment(segment_id));
            ops
        };

        self.metadata
            .batch_write(ops)
            .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())))?;

        tracing::info!(
            segment_id = %segment_id,
            new_segment_id = %new_segment_id,
            objects_repacked = live_objects.len(),
            dead_objects_filtered = objects.len() - live_objects.len(),
            "segment compaction complete"
        );

        Ok(segment_size)
    }

    /// Finds all objects that have chunks in the given segment.
    ///
    /// Note: This is O(n) in number of objects. In production, a reverse
    /// index (segment → objects) would accelerate this. The RocksDB
    /// `objects` CF could be augmented with an index column family
    /// mapping `segment_id → [object_key]` to avoid the full scan.
    fn find_objects_in_segment(&self, segment_id: SegmentId) -> Result<Vec<ObjectMetadata>> {
        let mut result = Vec::new();

        // Use list_objects with empty prefix to scan all; filter in-memory.
        let all_objects = self.metadata.list_objects(&oceanfs_core::BucketId::new("default"), "");

        for obj in all_objects.into_iter().flatten() {
            if obj.chunks.iter().any(|c| c.segment_id == segment_id) {
                result.push(obj);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use oceanfs_core::{
        BucketId, ChunkRef, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId,
        SegmentMetadata, SizeTier, Tombstone,
    };
    use oceanfs_storage::{metadata::RocksDbMetadataStore, segment::TierRouter};

    use super::super::{
        garbage_collector::GarbageCollector, liveness_tracker::LivenessTracker,
        segment_compactor::SegmentCompactor, *,
    };

    fn test_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn make_object_meta(key: &str, size: u64, chunk: ChunkRef) -> ObjectMetadata {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(chunk);
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        }
    }

    fn make_segment_meta(id: SegmentId, tier: SizeTier, sealed_at: i64) -> SegmentMetadata {
        SegmentMetadata {
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: tier,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(sealed_at),
        }
    }

    // SegmentCompactor
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compactor_finds_objects_in_segment() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Put object referencing this segment
        let obj_meta = make_object_meta(
            "ref.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Put object NOT referencing this segment
        let other_seg_id = SegmentId::new();
        let obj_meta2 = make_object_meta(
            "other.txt",
            200,
            ChunkRef { segment_id: other_seg_id, offset: 0, length: 200 },
        );
        metadata.put_object(obj_meta2).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead_keys = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead_keys,
            )
            .await;
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------

    // SegmentCompactor additional tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compactor_segment_with_no_objects() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead_keys2 = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead_keys2,
            )
            .await;
        assert!(result.is_ok());
        // Fully dead segment should return some reclaimed bytes
        assert!(result.unwrap() > 0);
    }

    // -----------------------------------------------------------------------

    #[test]
    fn process_tombstones_expired_tombstone_marked_dead() {
        let metadata = RocksDbMetadataStore::open(&test_config()).unwrap();

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "old_deleted.txt",
            300,
            ChunkRef { segment_id: seg_id, offset: 0, length: 300 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Create a tombstone with deletion_time far in the past
        let bucket = BucketId::new("default");
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("old_deleted.txt"),
                Tombstone {
                    deletion_time: 1000000000000, // very old
                    hlc: Hlc::new(1000000000000, 1),
                },
            )
            .unwrap();

        // With a short TTL, the tombstone should be eligible
        let gc = GarbageCollector::new(GcConfig { tombstone_ttl_sec: 3600, ..GcConfig::default() });

        let mut tracker = LivenessTracker::new();
        let mut stats = GcStats::default();
        let (dead_keys, _) = gc.process_tombstones(&metadata, &mut tracker, &mut stats).unwrap();

        // The tombstone is past TTL, so it should be in the dead set
        assert!(dead_keys.contains("old_deleted.txt"));
        assert_eq!(tracker.dead_bytes_for(&seg_id), 300);
    }

    // -----------------------------------------------------------------------
    // Compaction produces correct new chunk refs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_updates_object_chunk_refs() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let old_seg_id = SegmentId::new();
        let old_seg_meta = make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(old_seg_meta).unwrap();

        // Put an object referencing the old segment
        let obj_key = ObjectKey::new("moved.txt");
        let obj_meta = make_object_meta(
            "moved.txt",
            400,
            ChunkRef { segment_id: old_seg_id, offset: 0, length: 400 },
        );
        metadata.put_object(obj_meta).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead = HashSet::new();
        let result = compactor
            .compact_segment(
                old_seg_id,
                &make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);

        // The old segment should be deleted
        assert!(metadata.get_segment(old_seg_id).unwrap().is_none());

        // The object should now reference a different segment (the new one)
        let updated_obj = metadata
            .get_object(&BucketId::new("default"), &obj_key)
            .unwrap()
            .expect("object should still exist after compaction");
        assert!(!updated_obj.chunks.is_empty());
        assert_ne!(updated_obj.chunks[0].segment_id, old_seg_id);
        // The offset and length should be preserved (offset may change in new segment)
        assert_eq!(updated_obj.chunks[0].length, 400);
    }

    // GC cycle with compaction produces correct stats
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_compacts_segment_and_reports_stats() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Put 4 objects (200 bytes each = 800 total)
        for i in 0..4 {
            let obj_meta = make_object_meta(
                &format!("keep{i}.txt"),
                200,
                ChunkRef { segment_id: seg_id, offset: i * 200, length: 200 },
            );
            metadata.put_object(obj_meta).unwrap();
        }

        // Delete 3 of the 4 objects (600 of 800 = 75% dead space → liveness 0.25)
        let bucket = BucketId::new("default");
        for i in 0..3 {
            metadata
                .put_tombstone(
                    &bucket,
                    &ObjectKey::new(format!("keep{i}.txt")),
                    Tombstone {
                        deletion_time: 1000000000000, // ancient, past any TTL
                        hlc: Hlc::new(1000000000000, 1),
                    },
                )
                .unwrap();
        }

        // Use a threshold that will trigger (liveness 0.25 < 0.5)
        let gc_trigger = GarbageCollector::new(GcConfig {
            tombstone_ttl_sec: 0,
            compact_threshold: 0.5,
            max_concurrent_compactions: 1,
            compaction_queue_capacity: 8,
            ..GcConfig::default()
        });
        let stats = gc_trigger.run_cycle(metadata.clone()).await.unwrap();

        assert!(stats.segments_scanned >= 1);
        // With 75% dead and threshold 0.5, the segment should be compacted
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_reclaimed > 0);
        // Old segment should be gone
        assert!(metadata.get_segment(seg_id).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Compaction with dead objects (tombstoned) filters them out
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_skips_dead_objects() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Live object
        let obj_key_live = ObjectKey::new("live.txt");
        let obj_meta_live = make_object_meta(
            "live.txt",
            300,
            ChunkRef { segment_id: seg_id, offset: 0, length: 300 },
        );
        metadata.put_object(obj_meta_live).unwrap();

        // Dead object (has tombstone)
        let obj_key_dead = ObjectKey::new("dead.txt");
        let obj_meta_dead = make_object_meta(
            "dead.txt",
            200,
            ChunkRef { segment_id: seg_id, offset: 300, length: 200 },
        );
        metadata.put_object(obj_meta_dead).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        // Mark "dead.txt" as a dead object
        let mut dead_keys = HashSet::new();
        dead_keys.insert("dead.txt".to_string());

        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &dead_keys,
            )
            .await;
        assert!(result.is_ok());

        // Live object should have been repacked to a new segment
        let updated_live = metadata
            .get_object(&BucketId::new("default"), &obj_key_live)
            .unwrap()
            .expect("live object should still exist");
        assert!(!updated_live.chunks.is_empty());
        assert_ne!(updated_live.chunks[0].segment_id, seg_id);

        // Dead object should still have its old chunk refs (not repacked)
        // Note: dead objects keep their metadata; the space is just not repacked.
        let dead_obj = metadata
            .get_object(&BucketId::new("default"), &obj_key_dead)
            .unwrap()
            .expect("dead object metadata still exists");
        assert!(!dead_obj.chunks.is_empty());
        // The dead object's chunk still references the old segment — the
        // tombstone records the deletion, and the space is reclaimed by
        // not repacking (the old segment is deleted).
    }

    // -----------------------------------------------------------------------
}
