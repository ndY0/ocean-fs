//! Segment compaction — merges sparsely-populated segments to reclaim space.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{ChunkRef, ObjectMetadata, SegmentId, SegmentMetadata};

use super::config::tier_target_size;
use crate::{
    error::Result,
    metadata::{BatchOp, MetadataStore},
    segment::TierRouter,
};

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
    metadata: Arc<MetadataStore>,
    /// The tier router for classifying blobs by size.
    /// Wired for future tier-specific segment pool routing during repacking.
    tier_router: TierRouter,
}

impl SegmentCompactor {
    /// Creates a new segment compactor.
    pub(crate) fn new(metadata: Arc<MetadataStore>, tier_router: TierRouter) -> Self {
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

        // Build batch operations: update object metadata, create new segment, delete old.
        let mut ops: Vec<BatchOp> = Vec::with_capacity(live_objects.len() + 2);

        for obj in &live_objects {
            let mut new_chunks = smallvec::SmallVec::<[ChunkRef; 4]>::new();

            for chunk in &obj.chunks {
                if chunk.segment_id == segment_id {
                    // This chunk is being repacked — use the new reference.
                    let key = (chunk.segment_id, chunk.offset, chunk.length);
                    if let Some(new_ref) = chunk_remap.get(&key) {
                        new_chunks.push(*new_ref);
                    }
                } else {
                    // Chunk references a different segment — keep as-is.
                    new_chunks.push(*chunk);
                }
            }

            let updated_meta = ObjectMetadata { chunks: new_chunks, ..(*obj).clone() };
            ops.push(BatchOp::PutObject(obj.object_key.clone(), updated_meta));
        }

        ops.push(BatchOp::PutSegment(new_seg_meta));
        ops.push(BatchOp::DeleteSegment(segment_id));

        self.metadata.batch_write(ops)?;

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
