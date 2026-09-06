//! Segment compaction — merges sparsely-populated segments to reclaim space.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{
    BucketId, ChunkRef, ContainedObject, ObjectMetadata, RemappedChunk, SegmentId, SegmentMetadata,
};
use oceanfs_storage::{
    segment::{lifecycle::SegmentLifecycleCoordinator, TierRouter},
    Result,
};
use oceanfs_storage_api::SegmentDataStore;

use super::{
    compaction_recovery::{CompactionState, CompactionUnit},
    config::tier_target_size,
};

// ---------------------------------------------------------------------------
// SegmentCompactor
// ---------------------------------------------------------------------------

/// Compacts a segment by re-packing live blobs into new segments.
///
/// Reads all live blobs from a segment, re-packs them into a new segment,
/// updates object metadata to point to new chunk references, and frees
/// the old segment. The compactor is a state machine (ADR-0025
/// Decision 4) whose durable checkpoints are events:
///
/// ```text
/// Copying       → new .dat being written (no durable event yet)
/// NewSealed     → SealEvent(new) appended          [durable]
/// ObjectsMoved  → PutObject(new refs) committed    [RocksDB]
/// OldDeleted    → DeleteEvent(old) appended        [durable]
/// OldRemoved    → old .dat unlinked
/// ```
///
/// The compactor **requests** each transition from the
/// [`SegmentLifecycleCoordinator`] and never writes lifecycle state or
/// events itself — the coordinator enforces ADR-0024's compaction
/// ordering (new `.dat` → `SealEvent(new)` → `PutObject(new)` →
/// `DeleteEvent(old)` → unlink old), so the metadata-only-compaction
/// and BadDigest defects are structurally impossible.
pub(crate) struct SegmentCompactor {
    /// The metadata store for reading object metadata and updating chunk refs.
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// The tier router for classifying blobs by size.
    /// Wired for future tier-specific segment pool routing during repacking.
    tier_router: TierRouter,
    /// The segment data store: reads the old segment's bytes and writes
    /// the repacked new segment's `.dat` file. Without this the compactor
    /// only remapped metadata to a segment ID with no on-disk data —
    /// reads of repacked objects failed after restart (dormant bug: GC
    /// never compacted before tombstone-carried chunks made dead bytes
    /// detectable).
    store: Arc<dyn SegmentDataStore>,
    /// The lifecycle coordinator — the compactor's ONLY writer of
    /// segment lifecycle state (ADR-0025 Decision 4). Every milestone
    /// transition goes through it.
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    /// Optional sealed-segment notifier (sealed-segment-replication).
    ///
    /// Fired with the NEW segment id once its `SealEvent` is durable —
    /// the repacked segment is a fresh owner-side seal that the segment
    /// replicator must fan out to the new segment's ring replicas (the
    /// compactor seals OUTSIDE the write-path seal worker, so without
    /// this hook post-compaction objects would silently have zero
    /// replicas).
    sealed_notifier: Option<Arc<dyn Fn(SegmentId) + Send + Sync>>,
    /// Optional compaction-remap notifier (g3 `loss-announcement`,
    /// Option A — owner-authoritative compaction propagation).
    ///
    /// Fired with `(old_segment_id, new_segment_id, chunk_table,
    /// object_keys)` AFTER the `ObjectsMoved` milestone commits: the
    /// owner's metadata is authoritatively re-pointed to the new id, and
    /// the owner still holds the old segment's data (the `OldDeleted`
    /// milestone has not run yet), so a peer that has not yet processed
    /// the remap can still fetch the old segment from the owner via the
    /// read path's gRPC fallback. Without this hook, peers' metadata for
    /// the same objects silently diverges from the owner's after
    /// compaction — reads routed to a peer can reference a segment that
    /// exists nowhere (GAP-1).
    ///
    /// `chunk_table` maps every live chunk repacked from the old segment
    /// into the new one (`(old_offset, length) → new_offset`): the
    /// repacked layout is NOT offset-preserving, so peers must translate
    /// chunk refs through this table rather than re-point by segment id
    /// alone.
    ///
    /// `object_keys` carries the `(bucket, key)` of every live object the
    /// owner repacked (the repacked segment's own seal-time membership,
    /// ADR-0034 D5/2b). Each peer holder re-points exactly those keys via
    /// point lookups — it never scans its objects CF to rediscover which
    /// of its rows referenced the old segment.
    compaction_remap_notifier: Option<crate::gc::CompactionRemapFn>,
}

impl SegmentCompactor {
    /// Creates a new segment compactor.
    pub(crate) fn new(
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        tier_router: TierRouter,
        store: Arc<dyn SegmentDataStore>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
    ) -> Self {
        Self {
            metadata,
            tier_router,
            store,
            lifecycle,
            sealed_notifier: None,
            compaction_remap_notifier: None,
        }
    }

    /// Wires the sealed-segment notifier (composition root).
    pub(crate) fn with_sealed_notifier(
        mut self,
        notifier: Arc<dyn Fn(SegmentId) + Send + Sync>,
    ) -> Self {
        self.sealed_notifier = Some(notifier);
        self
    }

    /// Wires the compaction-remap notifier (composition root; g3
    /// `loss-announcement` Option A). Fired with `(old, new, chunks,
    /// object_keys)` after the `ObjectsMoved` metadata remap commits, so
    /// the owner's rows are authoritative at the new id before any peer
    /// is told to re-point. `object_keys` is the live repacked object set
    /// (ADR-0034 D5/2b) peers re-point via point lookups.
    pub(crate) fn with_compaction_remap_notifier(
        mut self,
        notifier: crate::gc::CompactionRemapFn,
    ) -> Self {
        self.compaction_remap_notifier = Some(notifier);
        self
    }

    /// Returns the tier router used for blob classification during repacking.
    pub(crate) fn tier_router(&self) -> &TierRouter {
        &self.tier_router
    }

    /// Returns the data store (tests only — re-wires the compactor with
    /// a remap notifier on the same store).
    #[cfg(test)]
    pub(crate) fn store(&self) -> Arc<dyn SegmentDataStore> {
        Arc::clone(&self.store)
    }

    /// Compacts a single segment: re-packs live blobs, updates metadata,
    /// and returns the number of bytes reclaimed.
    ///
    /// Objects whose (bucket, key) is in `dead_object_keys` (i.e., have an
    /// expired tombstone) are NOT repacked — their space is reclaimed.
    /// Only live (non-deleted) objects are moved to the new segment.
    ///
    /// Steps (the ADR-0025 Decision 4 milestone machine):
    /// 1. Find objects referencing this segment
    /// 2. Filter out dead objects (those with expired tombstones)
    /// 3. Read the old segment's data and build the repacked byte buffer
    ///    (live chunks copied to their new offsets)
    /// 4. `Copying` → `request_reserve(new)` then persist the new
    ///    segment's `.dat` via the data store — MUST happen before the
    ///    metadata swap: without the on-disk data the new segment ID
    ///    would be metadata-only and reads of repacked objects would
    ///    fail after restart
    /// 5. `NewSealed` → `request_seal(new, repacked metadata, marker)`:
    ///    the `SealEvent(new)` carries the full repacked metadata (the
    ///    seal-time merkle root + the `repacked_from` marker) and is
    ///    durable before the objects move
    /// 6. `ObjectsMoved` → batch-update object metadata with new chunk
    ///    refs (RocksDB — the one cross-store hop, ordered by
    ///    construction, not atomicity)
    /// 7. `OldDeleted` → `request_delete(old)` (durable)
    /// 8. `OldRemoved` → unlink the old `.dat` (only after the durable
    ///    delete returned)
    ///
    /// A fully-dead segment (no live objects) skips the repack: the
    /// durable delete precedes the unlink (ADR-0024 invariant 3).
    pub(crate) async fn compact_segment(
        &self,
        segment_id: SegmentId,
        segment_meta: &SegmentMetadata,
        contained_objects: Option<&[ContainedObject]>,
        dead_object_keys: &HashSet<(String, String)>,
    ) -> Result<u64> {
        // Enumerate the segment's objects from its seal-time membership
        // list (ADR-0034 D5) + per-key point lookups — never a full
        // objects-CF scan. A membership-less Sealed segment (WAL-replayed
        // or pre-feature) is not a compaction candidate; GC filters such
        // candidates before spawning, and this guard is the second line.
        let Some(contained) = contained_objects else {
            return Err(oceanfs_storage::Error::Io(std::io::Error::other(format!(
                "compact_segment refused: segment {segment_id} has no contained-objects membership \
                 (WAL-replayed/pre-feature segments are not compaction candidates)"
            ))));
        };

        // Live = membership rows that are still present, not tombstoned,
        // and reference this segment.
        let mut found: Vec<(BucketId, ObjectMetadata)> = Vec::with_capacity(contained.len());
        for co in contained {
            if dead_object_keys
                .contains(&(co.bucket.as_str().to_string(), co.key.as_str().to_string()))
            {
                continue;
            }
            if let Ok(Some(obj)) = self.metadata.get_object_metadata(&co.bucket, &co.key) {
                if obj.chunks.iter().any(|c| c.segment_id == segment_id) {
                    found.push((co.bucket.clone(), obj));
                }
            }
        }
        let live_objects: Vec<&(BucketId, ObjectMetadata)> = found.iter().collect();

        // Total segment size for reclaimed bytes tracking
        let segment_size = tier_target_size(segment_meta.size_tier);

        if live_objects.is_empty() {
            // No live objects — the segment is fully dead. The durable
            // DeleteEvent precedes the unlink (ADR-0024 invariant 3); a
            // crash between the two leaves a Deleted entry + a `.dat`
            // residue swept by the row-9 sweep.
            self.request_old_deletion(segment_id).await?;
            self.stall_at(5).await; // seam: after the DeleteEvent(old) (fully-dead path)
            self.store
                .delete_shards_with_pool(&segment_id, segment_meta.pool_id)
                .await
                .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())))?;
            tracing::info!(
                segment_id = %segment_id,
                "compacting fully-dead segment — all objects deleted"
            );
            return Ok(segment_size);
        }

        // Read the old segment's data section (header already parsed by
        // the store — `SegmentFile.data` is the data section; chunk
        // offsets are relative to it).
        let old_data = self
            .store
            .read_segment_data(&segment_id)
            .await
            .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())))?
            .ok_or_else(|| {
                // A compactor-chosen segment whose `.dat` is absent is a
                // genuine anomaly (the liveness scan saw the registry
                // entry): surface it as a NotFound read error so the
                // cycle reports the compaction failure.
                oceanfs_storage::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("segment {segment_id} data not present at compaction time"),
                ))
            })?;
        let old_data = old_data.data;

        // Create a new segment for repacking the live blobs.
        let new_segment_id = SegmentId::new();
        let mut new_offset: u64 = 0;

        // Build old-chunk → new-chunk mapping.
        // Key: (old_segment_id, old_offset, length) → new ChunkRef
        let mut chunk_remap: HashMap<(SegmentId, u64, u32), ChunkRef> =
            HashMap::with_capacity(live_objects.len());

        // Repacked byte buffer: live chunks copied to their new offsets
        // (perf 1.1 — bytes::BytesMut for blob data; pre-sized per
        // chunk, perf 1.3).
        let mut repacked: bytes::BytesMut = bytes::BytesMut::with_capacity(
            live_objects
                .iter()
                .map(|(_, obj)| {
                    obj.chunks
                        .iter()
                        .filter(|c| c.segment_id == segment_id)
                        .map(|c| c.length as usize)
                        .sum::<usize>()
                })
                .sum(),
        );

        for (_bucket, obj) in &live_objects {
            for chunk in &obj.chunks {
                if chunk.segment_id == segment_id {
                    // [review][architectural][high]
                    // rather than using a boolean for the compressed state, we should plan ahead for encryption,
                    // and supporting different compression / encryption backend : for this regard,
                    // we should leverage bit flags and masking
                    // [end]
                    let new_chunk = ChunkRef {
                        segment_id: new_segment_id,
                        offset: new_offset,
                        // Preserve the compression contract: a compressed
                        // chunk's length is the COMPRESSED size on disk,
                        // and the read path decompresses only when
                        // `compressed` is true, yielding `logical_length`
                        // bytes. Hardcoding false/equal here (as the
                        // original compactor did) made reads return raw
                        // compressed bytes — hash verification failed
                        // (BadDigest) for repacked compressed objects.
                        length: chunk.length,
                        compressed: chunk.compressed,
                        logical_length: chunk.logical_length,
                    };
                    let key = (chunk.segment_id, chunk.offset, chunk.length);
                    chunk_remap.insert(key, new_chunk);

                    // Copy the live bytes into the repacked buffer at the
                    // new offset. Chunk data is contiguous in the source
                    // segment (data section), so a direct slice copy is
                    // exact.
                    let start = chunk.offset as usize;
                    let end = start.saturating_add(chunk.length as usize).min(old_data.len());
                    repacked.extend_from_slice(&old_data[start..end]);
                    new_offset += chunk.length as u64;
                }
            }
        }

        // The compaction unit this run drives through the milestones.
        let unit = CompactionUnit {
            old_segment_id: segment_id,
            new_segment_id,
            tier: segment_meta.size_tier,
            ec_k: segment_meta.ec_k,
            ec_m: segment_meta.ec_m,
        };
        let mut state = CompactionState::Copying;
        tracing::debug!(
            old_segment_id = %unit.old_segment_id,
            new_segment_id = %unit.new_segment_id,
            ?state,
            "compaction unit started"
        );

        // Copying → reserve the new segment (the reserve-before-data
        // invariant: the seal transition is Reserved-only), then persist
        // the new `.dat`. The reserve is the first durable event; a
        // crash here leaves a Reserved entry the data-WAL pass drops
        // (row 1) — the old segment is untouched.
        self.lifecycle
            .request_reserve(new_segment_id, unit.tier, unit.ec_k, unit.ec_m)
            .await
            .map_err(|e| {
                oceanfs_storage::Error::Io(std::io::Error::other(format!(
                    "compaction reserve failed: {e}"
                )))
            })?;
        self.stall_at(1).await; // seam: after the reserve (Copying start)

        // The `.dat` write outcome decides the error path: after the
        // reserve, a failed run must clean its own registry entry
        // (best-effort — the durable delete keeps recovery
        // deterministic).
        let write_result = self
            .store
            .write_segment_data(&new_segment_id, &repacked)
            .await
            .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())));
        if let Err(e) = write_result {
            self.cleanup_reserved_new(new_segment_id).await;
            return Err(e);
        }
        self.stall_at(2).await; // seam: after the .dat write (Copying done)

        // NewSealed → the SealEvent(new) is the durable checkpoint that
        // makes the new segment real. The seal carries the FULL repacked
        // metadata — merkle root computed at seal time over the repacked
        // data — and the `repacked_from` marker (ADR-0025 Decision 4).
        // A crash before this event leaves the unit at Copying: the
        // data-WAL pass adopts the `.dat` (row 3) and the reaper sweeps
        // the unreferenced replacement.
        state = CompactionState::NewSealed;
        tracing::debug!(?state, "compaction milestone: SealEvent(new) durable");
        let merkle_root = crate::MerkleTree::build(&repacked, 0)
            .map(|tree| tree.root().hash())
            .ok_or_else(|| {
                oceanfs_storage::Error::Io(std::io::Error::other(
                    "compaction seal: failed to build the seal-time merkle root",
                ))
            })?;
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let new_seg_meta = SegmentMetadata {
            pool_id: 0,
            total_bytes: repacked.len() as u64,
            segment_id: new_segment_id,
            ec_k: segment_meta.ec_k,
            ec_m: segment_meta.ec_m,
            size_tier: segment_meta.size_tier,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(now_ms),
        };
        // The repacked segment's own seal-time membership (ADR-0034 D5):
        // the compactor knows `(bucket, key)` of every object it repacks.
        let live_membership: Vec<ContainedObject> = live_objects
            .iter()
            .map(|(bucket, obj)| ContainedObject {
                bucket: (*bucket).clone(),
                key: obj.object_key.clone(),
            })
            .collect();
        let live_membership = ContainedObject::sorted_dedup(live_membership);
        if let Err(e) = self
            .lifecycle
            .request_seal_with_contained(
                new_segment_id,
                new_seg_meta,
                Some(segment_id),
                Some(&live_membership),
            )
            .await
        {
            self.cleanup_reserved_new(new_segment_id).await;
            return Err(oceanfs_storage::Error::Io(std::io::Error::other(format!(
                "compaction seal failed: {e}"
            ))));
        }
        self.stall_at(3).await; // seam: after the SealEvent(new) (NewSealed)

        // The repacked segment is durable + registered: publish it for
        // segment replication (the replicator fans the new segment's data
        // out to its ring replicas — the compactor's seal bypasses the
        // write-path seal worker, so this is the only owner-side hook).
        if let Some(notifier) = &self.sealed_notifier {
            notifier(new_segment_id);
        }

        // ObjectsMoved → commit the object metadata remap (RocksDB; the
        // one cross-store hop). No lifecycle write rides in this batch —
        // the machine is the only writer of segment state (ADR-0025
        // Decision 1).
        state = CompactionState::ObjectsMoved;
        tracing::debug!(?state, "compaction milestone: objects remap committed");
        let ops: Vec<oceanfs_storage_api::BatchOp> = {
            let mut ops = Vec::with_capacity(live_objects.len());
            for (bucket, obj) in &live_objects {
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
                    bucket.clone(),
                    obj.object_key.clone(),
                    updated_meta,
                ));
            }
            ops
        };
        if let Err(e) = self.metadata.batch_write(ops) {
            // The unit is stuck at ObjectsMoved (new sealed, objects→old):
            // row 7. The next GC cycle re-selects the old segment and the
            // reaper eventually sweeps the unreferenced new `.dat` —
            // deterministic recovery, no special handling here.
            tracing::warn!(
                segment_id = %segment_id,
                new_segment_id = %new_segment_id,
                error = %e,
                "object metadata remap failed after the new segment was sealed; the unit stays at ObjectsMoved (row 7 recovery)"
            );
            return Err(oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())));
        }
        self.stall_at(4).await; // seam: after the objects remap (ObjectsMoved)

        // The owner's metadata is NOW authoritatively re-pointed to the
        // new id, and the old segment's `.dat` is still present (the
        // `OldDeleted` milestone has not run yet): publish the
        // compaction remap so peers re-point their OWN object rows
        // (g3 `loss-announcement` Option A — owner-authoritative
        // propagation). A peer that has not processed the remap yet can
        // still fetch the old segment from the owner via the read path's
        // gRPC fallback, so there is no read window here.
        //
        // The chunk table carries EVERY live chunk repacked from the old
        // segment into the new one — the repacked layout is not
        // offset-preserving, so peers translate `(old_offset, length) →
        // new_offset` rather than re-pointing by segment id alone.
        //
        // The object-key list is the repacked segment's own seal-time
        // membership (ADR-0034 D5/2b): every live object the owner
        // repacked, ordered + deduplicated. A peer holder re-points
        // exactly these keys via point lookups — never an objects-CF
        // scan. It is moved into the notifier (no later use here).
        if let Some(remap_notifier) = &self.compaction_remap_notifier {
            let mut chunk_table: Vec<RemappedChunk> = Vec::with_capacity(chunk_remap.len());
            for (key, new_chunk) in &chunk_remap {
                // key = (old_segment_id, old_offset, length); all keys
                // reference the same old segment.
                let (_old_seg, old_offset, length) = *key;
                chunk_table.push(RemappedChunk {
                    old_offset,
                    length,
                    new_offset: new_chunk.offset,
                });
            }
            remap_notifier(unit.old_segment_id, new_segment_id, chunk_table, live_membership);
        }

        // OldDeleted → the DeleteEvent(old) is durable before the old
        // `.dat` is unlinked (ADR-0024 invariant 3; crash-window row 9
        // is the safe residue between the two).
        state = CompactionState::OldDeleted;
        tracing::debug!(?state, "compaction milestone: DeleteEvent(old) durable");
        self.request_old_deletion(segment_id).await?;
        self.stall_at(5).await; // seam: after the DeleteEvent(old) (OldDeleted)

        // OldRemoved → unlink the old `.dat` (only after the durable
        // delete returned) — from the old segment's pool root (the
        // metadata the caller holds; ADR-0029 f5).
        state = CompactionState::OldRemoved;
        tracing::debug!(?state, "compaction milestone: old .dat unlinked");
        self.store
            .delete_shards_with_pool(&segment_id, segment_meta.pool_id)
            .await
            .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())))?;

        tracing::info!(
            segment_id = %segment_id,
            new_segment_id = %new_segment_id,
            objects_repacked = live_objects.len(),
            dead_objects_filtered = contained.len() - live_objects.len(),
            "segment compaction complete"
        );

        Ok(segment_size)
    }

    /// Requests the durable deletion of a compacted segment, treating
    /// `Missing`/`AlreadyDeleted` (a previous run's delete landed, its
    /// unlink never did) as success — the unlink may proceed.
    async fn request_old_deletion(&self, segment_id: SegmentId) -> Result<()> {
        match self.lifecycle.request_delete(segment_id).await {
            Ok(()) => Ok(()),
            Err(oceanfs_storage::segment::lifecycle::TransitionError::Missing)
            | Err(oceanfs_storage::segment::lifecycle::TransitionError::AlreadyDeleted) => Ok(()),
            Err(e) => Err(oceanfs_storage::Error::Io(std::io::Error::other(format!(
                "compaction delete failed: {e}"
            )))),
        }
    }

    /// Best-effort cleanup of a new segment whose seal never became
    /// durable (the unit failed between the reserve and the seal): the
    /// durable delete keeps the registry clean and the fold
    /// deterministic (a `DeleteEvent` makes the residue garbage). The
    /// `.dat` (if any) is unlinked after the durable delete.
    async fn cleanup_reserved_new(&self, new_segment_id: SegmentId) {
        if let Err(e) = self.lifecycle.request_delete(new_segment_id).await {
            tracing::warn!(
                segment_id = %new_segment_id,
                error = %e,
                "compaction cleanup delete failed; the Reserved entry is dropped by the next recovery"
            );
        }
        // The new segment's `.dat` was written via `write_segment_data`
        // (resolver-based): for a not-yet-registered segment the resolver
        // yields None → pool_id 0 → the first data pool root when pools
        // are configured (the write-before-register bridge, gone in
        // store-unification f2) — consistent with this unlink (Phase A
        // compaction placement, ADR-0029 f5).
        let _ = self.store.delete_shards_with_pool(&new_segment_id, 0).await;
    }

    /// Test seam for the compaction crash-window matrix (rows 7–9):
    /// reports that the unit reached `milestone` and stalls there while
    /// the seam is armed at that exact milestone. Compiles to nothing in
    /// production builds.
    async fn stall_at(&self, milestone: u8) {
        #[cfg(test)]
        {
            stall_seam::REACHED.store(milestone, std::sync::atomic::Ordering::SeqCst);
            while stall_seam::STALL_AT.load(std::sync::atomic::Ordering::SeqCst) == milestone {
                tokio::task::yield_now().await;
            }
        }
        #[cfg(not(test))]
        {
            let _ = milestone;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use oceanfs_core::{
        BucketId, ChunkRef, ContainedObject, Hlc, LifecycleConfig, MetadataConfig, ObjectKey,
        ObjectMetadata, RemappedChunk, SegmentId, SegmentMetadata, SizeTier, Tombstone,
    };
    use oceanfs_storage::{
        metadata::RocksDbMetadataStore,
        segment::{
            lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
            TierRouter,
        },
    };
    use oceanfs_storage_api::SegmentDataStore;

    use super::super::{
        garbage_collector::GarbageCollector, liveness_tracker::LivenessTracker,
        segment_compactor::SegmentCompactor, *,
    };

    /// Builds the contained-objects membership for the given object keys
    /// (bucket "default"), as a fixture would record it at seal time
    /// (ADR-0034 D5).
    fn membership(keys: &[&str]) -> Vec<ContainedObject> {
        keys.iter()
            .map(|k| ContainedObject { bucket: BucketId::new("default"), key: ObjectKey::new(*k) })
            .collect()
    }

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
            pool_id: 0,
            total_bytes: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: tier,
            // A sealed entry carries its seal-time anchor (the event log
            // requires the root at seal time).
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(sealed_at),
        }
    }

    /// Builds a compactor with an in-memory data store preloaded with the
    /// given segment's data bytes (the data-section content the store's
    /// `read_segment_data` would return), a phase-1 lifecycle coordinator
    /// over the same metadata store (its CF mirror writes are the durable
    /// side-effects — the milestone ORDER is what these tests pin), and
    /// an in-memory shard store. Returns the coordinator too so tests can
    /// seed segments through the machine.
    async fn make_compactor(
        metadata: Arc<RocksDbMetadataStore>,
        entries: Vec<(SegmentId, Vec<u8>)>,
    ) -> (SegmentCompactor, Arc<SegmentLifecycleCoordinator>) {
        let store = Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        for (id, data) in entries {
            store.write_segment_data(&id, &data).await.unwrap();
        }
        let tmp = tempfile::TempDir::new().unwrap();
        let event_wal = Arc::new(
            oceanfs_storage::segment::event_wal::EventWal::open(
                tmp.path().join("event-wal"),
                &oceanfs_core::EventWalConfig {
                    event_wal_dir: tmp.path().join("event-wal"),
                    event_wal_file_size_bytes: 1024 * 1024,
                    event_wal_fsync_batch_timeout_ms: 10,
                    event_wal_checkpoint_bytes: 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry))
                .with_event_wal(event_wal),
        );
        let compactor = SegmentCompactor::new(
            metadata,
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
            store,
            lifecycle.clone(),
        );
        (compactor, lifecycle)
    }

    /// Seeds a sealed segment through the coordinator (the machine — the
    /// only writer of lifecycle state) so `request_delete` validates.
    async fn seed_sealed(lifecycle: &SegmentLifecycleCoordinator, meta: SegmentMetadata) {
        lifecycle
            .request_reserve(meta.segment_id, meta.size_tier, meta.ec_k, meta.ec_m)
            .await
            .unwrap();
        lifecycle.request_seal(meta.segment_id, meta, None).await.unwrap();
    }

    /// Seeds a sealed segment with a contained-objects membership
    /// (ADR-0034 D5), as the write path would.
    async fn seed_sealed_with_contained(
        lifecycle: &SegmentLifecycleCoordinator,
        meta: SegmentMetadata,
        keys: &[&str],
    ) {
        lifecycle
            .request_reserve(meta.segment_id, meta.size_tier, meta.ec_k, meta.ec_m)
            .await
            .unwrap();
        lifecycle
            .request_seal_with_contained(meta.segment_id, meta, None, Some(&membership(keys)))
            .await
            .unwrap();
    }

    // SegmentCompactor
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compactor_finds_objects_in_segment() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Put object referencing this segment
        let obj_meta = make_object_meta(
            "ref.txt",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // Put object NOT referencing this segment
        let other_seg_id = SegmentId::new();
        let obj_meta2 = make_object_meta(
            "other.txt",
            200,
            ChunkRef {
                segment_id: other_seg_id,
                offset: 0,
                length: 200,
                compressed: false,
                logical_length: 200,
            },
        );
        metadata.put_object(obj_meta2).unwrap();

        let (compactor, _lifecycle) =
            make_compactor(metadata.clone(), vec![(seg_id, vec![0xAB; 500])]).await;

        let empty_dead_keys = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                Some(&membership(&["ref.txt"])),
                &empty_dead_keys,
            )
            .await;
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------

    // SegmentCompactor additional tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_fires_remap_notifier_with_chunk_table() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let old_seg_id = SegmentId::new();
        let old_seg_meta = make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000);
        let (compactor, lifecycle) =
            make_compactor(metadata.clone(), vec![(old_seg_id, vec![0xEF; 400])]).await;
        seed_sealed(&lifecycle, old_seg_meta.clone()).await;

        // One live object at (old_seg_id, offset 0, length 400).
        let _bucket = BucketId::new("default");
        let _obj_key = ObjectKey::new("remapped.txt");
        let obj_meta = make_object_meta(
            "remapped.txt",
            400,
            ChunkRef {
                segment_id: old_seg_id,
                offset: 0,
                length: 400,
                compressed: false,
                logical_length: 400,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // Wire the remap notifier: capture (old, new, chunk_table,
        // object_keys).
        let fired = Arc::new(AtomicUsize::new(0));
        let captured_old = Arc::new(parking_lot::Mutex::new(None::<SegmentId>));
        let captured_new = Arc::new(parking_lot::Mutex::new(None::<SegmentId>));
        let captured_table = Arc::new(parking_lot::Mutex::new(Vec::<RemappedChunk>::new()));
        let captured_objects = Arc::new(parking_lot::Mutex::new(Vec::<ContainedObject>::new()));
        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
            compactor.store(),
            lifecycle.clone(),
        )
        .with_compaction_remap_notifier({
            let fired = Arc::clone(&fired);
            let captured_old = Arc::clone(&captured_old);
            let captured_new = Arc::clone(&captured_new);
            let captured_table = Arc::clone(&captured_table);
            let captured_objects = Arc::clone(&captured_objects);
            Arc::new(move |old, new, table, objects| {
                fired.fetch_add(1, Ordering::SeqCst);
                *captured_old.lock() = Some(old);
                *captured_new.lock() = Some(new);
                *captured_table.lock() = table;
                *captured_objects.lock() = objects;
            })
        });

        let empty_dead = HashSet::new();
        let result = compactor
            .compact_segment(
                old_seg_id,
                &old_seg_meta,
                Some(&membership(&["remapped.txt"])),
                &empty_dead,
            )
            .await;
        assert!(result.is_ok());

        // The notifier fired exactly once with the right ids + table.
        assert_eq!(fired.load(Ordering::SeqCst), 1, "remap notifier must fire once");
        assert_eq!(captured_old.lock().as_ref(), Some(&old_seg_id));
        let new_id = *captured_new.lock().as_ref().expect("new id captured");
        assert_ne!(new_id, old_seg_id);
        // The chunk (0, 400) was repacked → table contains (0, 400) → new_offset.
        let table = captured_table.lock();
        assert!(
            table.iter().any(|c| c.old_offset == 0 && c.length == 400),
            "chunk table must carry the repacked chunk: {table:?}"
        );
        assert!(
            table.iter().all(|c| c.length == 400 && c.old_offset == 0),
            "only the live chunk is in the table"
        );
        // The object-key list carries exactly the repacked live objects
        // (ADR-0034 D5/2b) — ordered + deduplicated membership.
        let objects = captured_objects.lock();
        assert_eq!(
            objects.as_slice(),
            membership(&["remapped.txt"]).as_slice(),
            "remap notifier must carry the exact repacked object-key list"
        );
    }

    #[tokio::test]
    async fn compactor_segment_with_no_objects() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let (compactor, _lifecycle) =
            make_compactor(metadata.clone(), vec![(seg_id, vec![0xCD; 300])]).await;

        let empty_dead_keys2 = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                Some(&membership(&[])),
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
        let mut seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        seg_meta.total_bytes = 300;
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let obj_meta = make_object_meta(
            "old_deleted.txt",
            300,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 300,
                compressed: false,
                logical_length: 300,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // Create an AGED tombstone that carries the deleted row's chunk —
        // the shape `delete_object` leaves after the capture (f1), but
        // with a deterministic ancient deletion_time.
        let bucket = BucketId::new("default");
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 300,
            compressed: false,
            logical_length: 300,
        });
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("old_deleted.txt"),
                Tombstone {
                    deletion_time: 1000000000000, // very old
                    hlc: Hlc::new(1000000000000, 1),
                    chunks,
                },
            )
            .unwrap();

        // With a short TTL, the tombstone should be eligible
        let gc = GarbageCollector::new(GcConfig { tombstone_ttl_sec: 3600, ..GcConfig::default() });

        let mut tracker = LivenessTracker::new();
        let mut stats = GcStats::default();
        let (dead_keys, _, _) =
            gc.process_tombstones(&metadata, &registry, &mut tracker, &mut stats).unwrap();

        // The tombstone is past TTL, so it should be in the dead set and
        // its captured chunk bytes marked dead (accounting, ADR-0034 D3).
        assert!(dead_keys.contains(&("default".to_string(), "old_deleted.txt".to_string())));
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
        let (compactor, lifecycle) =
            make_compactor(metadata.clone(), vec![(old_seg_id, vec![0xEF; 400])]).await;
        // Seed the old segment through the machine (the only writer of
        // lifecycle state) so the compactor's request_delete validates.
        seed_sealed(&lifecycle, old_seg_meta.clone()).await;

        // Put an object referencing the old segment
        let obj_key = ObjectKey::new("moved.txt");
        let obj_meta = make_object_meta(
            "moved.txt",
            400,
            ChunkRef {
                segment_id: old_seg_id,
                offset: 0,
                length: 400,
                compressed: false,
                logical_length: 400,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        let empty_dead = HashSet::new();
        let result = compactor
            .compact_segment(
                old_seg_id,
                &old_seg_meta,
                Some(&membership(&["moved.txt"])),
                &empty_dead,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);

        // The old segment should be deleted (durably, via the
        // coordinator) — the machine's entry is evicted.
        assert!(
            lifecycle.registry().get(old_seg_id).is_none(),
            "the delete folds and evicts (grace 0)"
        );

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
        let mut seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        // The seal-time logical total (ADR-0034 D1): 4 × 200-byte objects.
        seg_meta.total_bytes = 800;

        let store = Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        store.write_segment_data(&seg_id, &vec![0x11; 800]).await.unwrap();

        // Seed the candidate through the machine (the only writer of
        // lifecycle state) so the compactor's transitions validate.
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let tmp = tempfile::TempDir::new().unwrap();
        let event_wal = Arc::new(
            oceanfs_storage::segment::event_wal::EventWal::open(
                tmp.path().join("event-wal"),
                &oceanfs_core::EventWalConfig {
                    event_wal_dir: tmp.path().join("event-wal"),
                    event_wal_file_size_bytes: 1024 * 1024,
                    event_wal_fsync_batch_timeout_ms: 10,
                    event_wal_checkpoint_bytes: 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry))
                .with_event_wal(event_wal),
        );
        seed_sealed_with_contained(
            &lifecycle,
            seg_meta.clone(),
            &["keep0.txt", "keep1.txt", "keep2.txt", "keep3.txt"],
        )
        .await;

        // Put 4 objects (200 bytes each = 800 total).
        for i in 0..4u64 {
            let obj_meta = make_object_meta(
                &format!("keep{i}.txt"),
                200,
                ChunkRef {
                    segment_id: seg_id,
                    offset: i * 200,
                    length: 200,
                    compressed: false,
                    logical_length: 200,
                },
            );
            metadata.put_object(obj_meta).unwrap();
        }

        // "Delete" 3 of the 4 objects the production way: remove the row
        // via `delete_object` (which captures the chunks) and plant the
        // deterministic AGED chunk-carrying tombstone — the f1 capture
        // shape. 600 of 800 bytes dead → liveness 0.25 < 0.5.
        let bucket = BucketId::new("default");
        for i in 0..3u64 {
            let key = ObjectKey::new(format!("keep{i}.txt"));
            metadata.delete_object(&bucket, &key, Hlc::zero()).unwrap();
            let mut chunks = smallvec::SmallVec::new();
            chunks.push(ChunkRef {
                segment_id: seg_id,
                offset: i * 200,
                length: 200,
                compressed: false,
                logical_length: 200,
            });
            metadata
                .put_tombstone(
                    &bucket,
                    &key,
                    Tombstone {
                        deletion_time: 1000000000000, // ancient
                        hlc: Hlc::zero(),
                        chunks,
                    },
                )
                .unwrap();
        }

        // Use a threshold that will trigger (liveness 0.25 < 0.5).
        let gc_trigger = GarbageCollector::new(GcConfig {
            tombstone_ttl_sec: 0,
            compact_threshold: 0.5,
            max_concurrent_compactions: 1,
            compaction_queue_capacity: 8,
            ..GcConfig::default()
        })
        .with_data_store(store)
        .with_lifecycle(lifecycle);
        let stats = gc_trigger.run_cycle(metadata.clone(), &registry).await.unwrap();

        assert!(stats.segments_scanned >= 1);
        // With 75% dead and threshold 0.5, the segment should be compacted
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_reclaimed > 0);
        // Old segment should be gone (durably deleted via the machine)
        assert!(registry.get(seg_id).is_none());
    }

    // -----------------------------------------------------------------------
    // Compaction with dead objects (tombstoned) filters them out
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_skips_dead_objects() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Live object
        let obj_key_live = ObjectKey::new("live.txt");
        let obj_meta_live = make_object_meta(
            "live.txt",
            300,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 300,
                compressed: false,
                logical_length: 300,
            },
        );
        metadata.put_object(obj_meta_live).unwrap();

        // Dead object (has tombstone)
        let obj_key_dead = ObjectKey::new("dead.txt");
        let obj_meta_dead = make_object_meta(
            "dead.txt",
            200,
            ChunkRef {
                segment_id: seg_id,
                offset: 300,
                length: 200,
                compressed: false,
                logical_length: 200,
            },
        );
        metadata.put_object(obj_meta_dead).unwrap();

        let (compactor, _lifecycle) =
            make_compactor(metadata.clone(), vec![(seg_id, vec![0x42; 500])]).await;

        // Mark "dead.txt" as a dead object
        let mut dead_keys = HashSet::new();
        dead_keys.insert(("default".to_string(), "dead.txt".to_string()));

        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                Some(&membership(&["live.txt", "dead.txt"])),
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

/// Crash-window test seam for `compaction_crash` (rows 7–9 of ADR-0025
/// §Crash-window table).
///
/// `STALL_AT` arms a stall at exactly one milestone (0 = off); `REACHED`
/// reports the milestone the compactor last reached. The crash tests arm
/// the seam, spawn the compaction, wait for the milestone, then "kill"
/// (abort the task + drop every instance) — the on-disk state is exactly
/// what a crash at that milestone leaves behind.
#[cfg(test)]
pub(crate) mod stall_seam {
    use std::sync::atomic::{AtomicU8, Ordering};

    /// The milestone to stall at (0 = no stall; milestones are 1–5:
    /// 1 = after reserve, 2 = after `.dat` write, 3 = after
    /// `SealEvent(new)`, 4 = after objects remap, 5 = after
    /// `DeleteEvent(old)`).
    pub(crate) static STALL_AT: AtomicU8 = AtomicU8::new(0);
    /// The last milestone the compactor reached (for tests to wait on).
    pub(crate) static REACHED: AtomicU8 = AtomicU8::new(0);

    /// Arms the seam at exactly `milestone` and resets the reached marker.
    pub(crate) fn arm(milestone: u8) {
        STALL_AT.store(milestone, Ordering::SeqCst);
        REACHED.store(0, Ordering::SeqCst);
    }

    /// Disarms the seam (no more stalling).
    pub(crate) fn disarm() {
        STALL_AT.store(0, Ordering::SeqCst);
    }
}
