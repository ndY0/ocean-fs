//! The segment seal pipeline — the storage-side drainer of the segment
//! pools' seal queues (relocated from the write coordinator, c3-Option-A
//! groundwork).
//!
//! # Why it lives here
//!
//! The seal pipeline seals **storage-owned** state (the active segment
//! pools' frozen buffers, the sealer, the lifecycle coordinator) and is
//! required by **startup recovery**: WAL-replayed re-seals complete
//! asynchronously through the pool seal queues and recovery waits for
//! their `.dat` files. Parking the drainer on the write coordinator made
//! recovery depend on a server-side object and forced the coordinator's
//! construction before recovery (the historical `start_seal_worker`
//! ordering). The pipeline is a pure storage component: it takes the
//! pools, the sealer and the lifecycle, and the two cross-crate
//! concerns are injected — the merkle-root builder (the durability
//! crate's `MerkleTree`, which storage cannot depend on) and the
//! sealed-segment notifier (the node's AE-continuous + replicator
//! fan-out).
//!
//! Single consumer: `take_seal_rx` hands the pool's receiver to the
//! first spawned pipeline and returns `None` afterwards — exactly one
//! pipeline may run per pool (the node spawns it at startup, before
//! recovery).

use std::sync::Arc;

use oceanfs_core::{HashOutput, SegmentId, SizeTier};
use tokio::task::JoinHandle;

use crate::segment::{
    lifecycle::SegmentLifecycleCoordinator, pool::SegmentPool, sealer::SegmentSealer,
};

/// The seal-time merkle-root builder over a segment's data section
/// (64 KiB leaves — the shared seal/scrub/AE default). Injected: the
/// production builder is the durability crate's `MerkleTree`, which the
/// storage crate cannot depend on (same pattern as the recovery fold's
/// `merkle_root_fn`).
pub type SealMerkleBuilder = Arc<dyn Fn(&[u8]) -> Option<HashOutput> + Send + Sync>;

/// Fired once per successfully sealed segment with its seal-time merkle
/// root (the AE-continuous incremental-tree update + the seal-time
/// replicator fan-out, wired by the node).
pub type SealedSegmentNotifier = Arc<dyn Fn(SegmentId, HashOutput) + Send + Sync>;

/// Spawns the seal pipeline draining the small + standard segment
/// pools' seal queues.
///
/// The drain loop mirrors the historic `WriteCoordinator::start_seal_worker`
/// exactly: both receivers are merged with `tokio::select!`, each work
/// item seals under the owning pool's semaphore permit (bounded
/// concurrency), the race-closing reserve runs through the lifecycle
/// coordinator when the registry has no entry yet, the merkle root is
/// built on the blocking pool, `sealer.seal_from_data` persists the
/// segment (EC parity at seal time), the sealed notifier fires with the
/// root, and the frozen buffer is recycled into its pool. The blob-index
/// entries now travel ON the work item (drained by the pool at enqueue —
/// `SegmentPool::seal_work`), so the pipeline has no cross-component
/// entries lookup.
///
/// The returned handle is detached by the node (the loop exits when both
/// seal queues close, i.e. when the pools drop at shutdown) — same
/// lifetime contract as the coordinator-owned worker it replaces.
pub fn spawn_seal_pipeline(
    small_pool: Arc<SegmentPool>,
    standard_pool: Arc<SegmentPool>,
    sealer: Arc<SegmentSealer>,
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    merkle_builder: SealMerkleBuilder,
    sealed_notifier: Option<SealedSegmentNotifier>,
) -> JoinHandle<()> {
    // Take seal receivers from both pools.
    let rx_small = small_pool.take_seal_rx();
    let rx_standard = standard_pool.take_seal_rx();

    tokio::spawn(async move {
        // Merge both receivers into a single stream using select.
        match (rx_small, rx_standard) {
            (Some(mut small_rx), Some(mut standard_rx)) => {
                loop {
                    let work = tokio::select! {
                        maybe_work = small_rx.recv() => maybe_work,
                        maybe_work = standard_rx.recv() => maybe_work,
                    };
                    let work = match work {
                        Some(w) => w,
                        None => {
                            // Both channels closed — nothing left to seal.
                            tracing::info!("seal worker shutting down: both seal queues closed");
                            break;
                        }
                    };

                    let sealer_arc = Arc::clone(&sealer);
                    let sem = if work.tier == SizeTier::Small {
                        small_pool.seal_semaphore()
                    } else {
                        standard_pool.seal_semaphore()
                    };

                    // Drain the blob index entries synchronously — the
                    // writer's append hook guarantees they are already
                    // recorded when the work item was enqueued (the pool
                    // drains them into the work item at enqueue).
                    let segment_id = work.segment_id;
                    let tier = work.tier;
                    let entries = work.entries.clone();

                    // NOTE: an empty entry list is LEGITIMATE — a
                    // segment rebuilt by WAL replay carries data that was
                    // never appended through this coordinator (no blob
                    // entries were recorded for it). Sealing it with an
                    // empty index is correct: the data bytes are the
                    // drained buffer, readers locate chunks via the
                    // object metadata's ChunkRefs, and the seal makes the
                    // segment durable (and its WAL files sweepable).
                    // Skipping the seal left such segments
                    // registered-unsealed forever, pinning their WAL
                    // files indefinitely (2.5 GB leak).
                    if entries.is_empty() {
                        tracing::debug!(
                            segment_id = %segment_id,
                            "sealing segment with empty blob index (WAL-replayed data)"
                        );
                    }

                    // Acquire a permit to enforce bounded concurrency
                    // (perf §2.7/8.5), then seal on a spawned task so
                    // the worker keeps draining the queues. Sealing
                    // serially here let the bounded queue overflow
                    // under write bursts (try_send dropped data);
                    // concurrent seals keep the drain rate above the
                    // fill rate (read-path-integrity-under-load).
                    let small_pool = Arc::clone(&small_pool);
                    let standard_pool = Arc::clone(&standard_pool);
                    let lifecycle = Arc::clone(&lifecycle);
                    let merkle_builder = Arc::clone(&merkle_builder);
                    let sealed_notifier = sealed_notifier.clone();
                    tokio::spawn(async move {
                        let permit = sem.acquire().await;

                        // Race-closing reserve: the write path
                        // reserves the segment BEFORE its first WAL
                        // entry, but the fill-triggered seal work
                        // item is enqueued DURING the append — a
                        // seal can drain before that reserve lands,
                        // and the flush path's Reserved-only
                        // validation would reject it as Missing.
                        // Reserving here (idempotent, through the
                        // coordinator — still the only writer) only
                        // when the registry has no entry yet closes
                        // the race; the common case (the write
                        // path's reserve already folded) skips the
                        // extra durable write.
                        if lifecycle.registry().get(segment_id).is_none() {
                            match lifecycle
                                .request_reserve(segment_id, tier, work.ec_k, work.ec_m)
                                .await
                            {
                                Ok(())
                                | Err(crate::segment::lifecycle::TransitionError::AlreadySealed)
                                | Err(crate::segment::lifecycle::TransitionError::AlreadyDeleted) =>
                                    {}
                                Err(e) => {
                                    tracing::warn!(
                                        segment_id = %segment_id,
                                        error = %e,
                                        "seal-time reserve failed; seal deferred to replay"
                                    );
                                    // The segment's data remains
                                    // readable via the sealing-data
                                    // set and the WAL still holds its
                                    // entries — crash recovery
                                    // replays it. Do not seal: the
                                    // flush path would reject it.
                                    return;
                                }
                            }
                        }

                        // The seal-time EC parity is computed inside
                        // `seal_from_data` on the blocking pool
                        // (single scheduler — the write path never
                        // touches a second thread pool).
                        // Compute the seal-time Merkle root over the
                        // data section (64 KiB leaves — the shared
                        // default used by scrub and anti-entropy) and
                        // persist it in the segment metadata: it is
                        // the trusted anchor for scrub verification,
                        // anti-entropy's local-vs-stored comparison,
                        // and the startup rebuild of the incremental
                        // Merkle tree. Without it, every segment is
                        // "missing merkle root" (scrub inert,
                        // anti-entropy flags every segment).
                        //
                        // The build is CPU-bound (hashing the full
                        // segment data) — it runs on the blocking
                        // pool, never on a runtime worker.
                        let merkle_data = work.segment_data.clone();
                        let merkle_root =
                            match tokio::task::spawn_blocking(move || merkle_builder(&merkle_data))
                                .await
                            {
                                Ok(root) => root,
                                Err(e) => {
                                    tracing::warn!(
                                        segment_id = %segment_id,
                                        error = %e,
                                        "merkle build task failed; sealing without merkle root"
                                    );
                                    None
                                }
                            };

                        let result = sealer_arc
                            .seal_from_data(
                                segment_id,
                                tier,
                                work.segment_data.clone(),
                                &entries,
                                work.ec_k,
                                work.ec_m,
                                work.strip_size_bytes,
                                work.ec_encoder.clone(),
                                merkle_root,
                            )
                            .await;

                        match result {
                            Ok(_handle) => {
                                // The in-flight read window is closed
                                // by the seal transition itself (the
                                // coordinator's fold cleared the
                                // entry's in_flight — the `.dat` is
                                // durable), so no cross-crate
                                // remove_seal_buffer call exists any
                                // more (lifecycle-read-path).
                                // Notify the anti-entropy engine so the
                                // incremental Merkle tree covers this
                                // segment without waiting for the next
                                // startup rebuild (continuous AE).
                                if let Some(notifier) = &sealed_notifier {
                                    if let Some(root) = merkle_root {
                                        notifier(segment_id, root);
                                    }
                                }
                                // Recycle the segment's backing buffer.
                                // The sealing-data clone was just dropped
                                // and seal_from_data's clone went out of
                                // scope, so the work item now holds the
                                // last reference to the original BytesMut
                                // allocation: try_into_mut recovers it
                                // zero-copy for the next activation
                                // (pool-backpressure-and-buffer-recycling).
                                match work.segment_data.try_into_mut() {
                                    Ok(buf) => {
                                        if tier == SizeTier::Small {
                                            small_pool.release_buffer(buf);
                                        } else {
                                            standard_pool.release_buffer(buf);
                                        }
                                    }
                                    // Still referenced (e.g. an in-flight
                                    // read of the sealing set): drop.
                                    Err(bytes) => drop(bytes),
                                }
                                tracing::info!(
                                    segment_id = %segment_id,
                                    tier = ?tier,
                                    blob_count = entries.len(),
                                    "segment sealed successfully"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    segment_id = %segment_id,
                                    error = %e,
                                    "segment seal failed"
                                );
                                // The in-memory entries were drained at
                                // enqueue and are dropped. The segment's
                                // bytes remain readable via the pool's
                                // sealing set, and the WAL still holds the
                                // append entries, so crash recovery
                                // replays this segment on restart.
                            }
                        }
                        drop(permit); // permit released
                    });
                }
            }
            _ => {
                tracing::info!("seal worker: seal queues unavailable");
            }
        }
    })
}
