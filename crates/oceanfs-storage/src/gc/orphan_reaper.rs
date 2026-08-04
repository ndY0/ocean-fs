//! Orphan reaper — detects and reclaims segments with no live references.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::SegmentId;

use super::{config::GcConfig, garbage_collector::SegmentShardStore};
use crate::{error::Result, metadata::MetadataStore};

// ---------------------------------------------------------------------------
// OrphanStats
// ---------------------------------------------------------------------------

/// Statistics from an orphan reaper cycle.
#[derive(Debug, Default, Clone)]
pub struct OrphanStats {
    /// Number of segments scanned.
    pub segments_scanned: u64,
    /// Number of orphaned segments found.
    pub orphans_found: u64,
    /// Number of orphans successfully deleted.
    pub orphans_deleted: u64,
    /// Bytes reclaimed from orphan deletion.
    pub bytes_reclaimed: u64,
}

// ---------------------------------------------------------------------------
// OrphanReaper
// ---------------------------------------------------------------------------

/// Detects and reclaims orphaned segments.
///
/// Orphaned segments are segments that no longer have any referencing
/// objects (e.g., all objects were deleted but GC compaction never ran).
/// The reaper periodically scans the `segments` CF, cross-references
/// against `objects` CF, and deletes unreferenced segments.
///
/// # Examples
///
/// ```ignore
/// // This example requires a running MetadataStore; examples are in unit tests.
/// use oceanfs_storage::{OrphanReaper, GcConfig};
/// ```
pub struct OrphanReaper {
    metadata: Arc<MetadataStore>,
    store: Arc<dyn SegmentShardStore>,
    config: GcConfig,
}

impl OrphanReaper {
    /// Creates a new orphan reaper.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use oceanfs_storage::{OrphanReaper, GcConfig};
    /// ```
    pub fn new(
        metadata: Arc<MetadataStore>,
        store: Arc<dyn SegmentShardStore>,
        config: GcConfig,
    ) -> Self {
        Self { metadata, store, config }
    }

    /// Runs a single orphan reaper cycle.
    ///
    /// 1. Builds the set of all referenced segment IDs from objects CF
    /// 2. Scans segments CF for segments not in the referenced set
    /// 3. Deletes orphan segments that have been sealed longer than TTL,
    ///    including both shard data from disk and segment metadata from
    ///    RocksDB.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or shard-deletion operations fail.
    pub async fn run_cycle(&self) -> Result<OrphanStats> {
        let mut stats = OrphanStats::default();

        // Phase 1: Build referenced segment ID set from all objects
        let referenced = self.build_referenced_set()?;

        // Phase 2: Scan segments and find orphans
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let ttl_ms = (self.config.tombstone_ttl_sec * 1000) as i64;

        let segments = self.metadata.list_segments();
        let mut orphan_ids = Vec::new();

        for seg_result in segments {
            match seg_result {
                Ok(seg_meta) => {
                    stats.segments_scanned += 1;
                    if !referenced.contains(&seg_meta.segment_id) {
                        // Not referenced by any object — check if old enough
                        if let Some(sealed_at) = seg_meta.sealed_at {
                            if now_ms - sealed_at > ttl_ms {
                                orphan_ids.push(seg_meta.segment_id);
                                stats.orphans_found += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read segment metadata");
                }
            }
        }

        // Phase 3: Reclaim orphans with double-check
        for segment_id in &orphan_ids {
            // Double-check: re-verify segment still unreferenced
            let still_orphan = !self.is_segment_referenced(*segment_id)?;

            if still_orphan {
                // Delete shard data from disk first, then remove metadata.
                // Shard deletion happens before metadata deletion so that
                // a crash between the two leaves metadata pointing to
                // already-deleted shards (safe: the segment will be detected
                // as orphan again and retried).
                match self.store.delete_shards(*segment_id) {
                    Ok(bytes) => {
                        tracing::info!(
                            segment_id = %segment_id,
                            bytes_reclaimed = bytes,
                            "deleted orphan segment shards"
                        );
                        stats.bytes_reclaimed += bytes;
                    }
                    Err(e) => {
                        // Log but continue — metadata deletion should still happen.
                        // The orphan segment's shards may already be gone.
                        tracing::warn!(
                            error = %e,
                            segment_id = %segment_id,
                            "failed to delete orphan segment shards, continuing with metadata deletion"
                        );
                    }
                }

                // Delete segment metadata from RocksDB
                self.metadata.delete_segment(*segment_id)?;
                stats.orphans_deleted += 1;

                tracing::info!(segment_id = %segment_id, "reclaimed orphan segment");
            }
        }

        Ok(stats)
    }

    /// Starts the orphan reaper in the background.
    ///
    /// Runs cycles at the configured interval until cancelled. The
    /// returned [`tokio::task::JoinHandle`] can be aborted to stop
    /// the background task gracefully.
    pub async fn start_background(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(this.config.interval_sec)).await;
                match this.run_cycle().await {
                    Ok(stats) => {
                        if stats.orphans_found > 0 {
                            tracing::info!(
                                orphans_found = stats.orphans_found,
                                orphans_deleted = stats.orphans_deleted,
                                bytes_reclaimed = stats.bytes_reclaimed,
                                "orphan reaper cycle complete"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "orphan reaper cycle failed");
                    }
                }
            }
        })
    }

    /// Builds the set of all segment IDs referenced by objects.
    pub(crate) fn build_referenced_set(&self) -> Result<HashSet<SegmentId>> {
        let mut referenced = HashSet::new();

        let all_objects = self.metadata.list_objects(&oceanfs_core::BucketId::new("default"), "");

        for obj in all_objects.into_iter().flatten() {
            for chunk in &obj.chunks {
                referenced.insert(chunk.segment_id);
            }
        }

        Ok(referenced)
    }

    /// Checks whether a segment is still referenced by any object.
    /// Used as a double-check before deletion to prevent races with
    /// concurrent writers.
    pub(crate) fn is_segment_referenced(&self, segment_id: SegmentId) -> Result<bool> {
        let referenced = self.build_referenced_set()?;
        Ok(referenced.contains(&segment_id))
    }
}
