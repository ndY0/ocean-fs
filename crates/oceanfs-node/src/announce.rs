//! Loss-announcement + compaction-remap fan-out (g3 `loss-announcement`,
//! ADR-0029 §D4 fast path, Option A).
//!
//! Two primitives:
//!
//! - [`announce_pool_loss`] — after a data pool is confirmed Dead, tell
//!   the replica holders of the affected segments exactly that, so they
//!   cross-check their hold-set and enqueue re-replication (g5). Targets
//!   are the UNION of each affected segment's `storage_locations` minus
//!   self — NOT the whole cluster, NOT `ring.lookup` (the ring maps key
//!   hashes, not segments; the segment→holder mapping lives in the
//!   lifecycle registry).
//! - [`announce_segment_remap`] — after the owner compacts `S → S'`, tell
//!   the holders of `S` so they re-point their OWN object rows (the
//!   owner's compaction rewrites only its RocksDB). Carries the
//!   chunk-remap table because the repacked layout is not
//!   offset-preserving.
//!
//! Both are best-effort fast paths with bounded retries (3 × 500 ms,
//! mirroring the hint delivery retry); the periodic reconciliation loop
//! (g4) is the mandatory failsafe that runs regardless of whether any
//! announcement arrived.

use std::{sync::Arc, time::Duration};

use oceanfs_core::{Counter, LabelSet, MetricRegistrar, NodeId, RemappedChunk, SegmentId};
use oceanfs_durability::healing_rpc::{
    healing_rpc_client::HealingRpcClient, LossAnnouncement, RemappedChunk as ProtoRemappedChunk,
    SegmentRemap,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;

/// Announcement transmit counters (ADR-0029 §D4 observability).
///
/// Incremented by [`announce_pool_loss`] / [`announce_segment_remap`]
/// per delivery attempt. Registered with the node's metric registrar.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar};
/// use oceanfs_node::announce::AnnounceMetrics;
///
/// struct Registrar;
/// impl MetricRegistrar for Registrar {
///     fn register_counter(&self, _c: Counter) {}
///     fn register_gauge(&self, _g: Gauge) {}
///     fn register_histogram(&self, _h: std::sync::Arc<oceanfs_core::Histogram>) {}
/// }
///
/// let metrics = AnnounceMetrics::new();
/// metrics.register_metrics(&Registrar);
/// metrics.record_delivery();
/// ```
#[derive(Debug, Clone)]
pub struct AnnounceMetrics {
    tx_total: Counter,
}

impl AnnounceMetrics {
    /// Creates unregistered counters.
    pub fn new() -> Self {
        Self {
            tx_total: Counter::new(
                "oceanfs_announcements_tx_total".into(),
                "Loss/remap announcements delivered".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers the counters with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.tx_total.clone());
    }

    /// Records one successful announcement delivery.
    pub fn record_delivery(&self) {
        self.tx_total.inc();
    }
}

impl Default for AnnounceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The default announcement retry policy (ADR-0029 §D4 fast path).
pub const DEFAULT_ANNOUNCE_ATTEMPTS: usize = 3;
/// The default spacing between retries.
pub const DEFAULT_ANNOUNCE_RETRY_DELAY: Duration = Duration::from_millis(500);
/// Per-RPC timeout for an announcement delivery.
pub const DEFAULT_ANNOUNCE_TIMEOUT_MS: u64 = 2_000;

/// Computes the fan-out target list for an affected segment set: the
/// UNION of every segment's `storage_locations`, minus self — the pinned
/// ADR-0029 §D4 fan-out (NOT the whole cluster, NOT `ring.lookup`).
///
/// The input is a slice of `(segment_id, storage_locations)` pairs (the
/// caller reads them from the lifecycle registry). Duplicates and self
/// are removed; order follows first-seen.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeId;
/// use oceanfs_node::announce::derive_fan_out_targets;
///
/// let self_id = NodeId::new("a");
/// let b = NodeId::new("b");
/// let c = NodeId::new("c");
/// let segments = vec![
///     (oceanfs_core::SegmentId::new(), vec![self_id.clone(), b.clone(), c.clone()]),
///     (oceanfs_core::SegmentId::new(), vec![b.clone(), c.clone()]),
/// ];
/// let targets = derive_fan_out_targets(&segments, &self_id);
/// assert_eq!(targets, vec![b, c]);
/// ```
pub fn derive_fan_out_targets(
    segments: &[(oceanfs_core::SegmentId, Vec<NodeId>)],
    self_id: &NodeId,
) -> Vec<NodeId> {
    let mut targets: Vec<NodeId> = Vec::new();
    for (_segment_id, locations) in segments {
        for loc in locations {
            if loc != self_id && !targets.contains(loc) {
                targets.push(loc.clone());
            }
        }
    }
    targets
}

/// Delivers one unary RPC to `target` with bounded retries.
///
/// Returns `Ok(())` when the target acked (or explicitly declined —
/// a declined RPC is a successful delivery; the g4 failsafe covers
/// whatever was genuinely missed); `Err` after all retries are
/// exhausted. The caller (the announcement builder) decides whether to
/// drop or escalate — g4's reconciliation loop is the failsafe.
async fn deliver_with_retry(
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    target: &NodeId,
    attempts: usize,
    retry_delay: Duration,
    send: impl Fn(HealingRpcClient<tonic::transport::Channel>) -> SendRpcFuture,
) -> Result<(), String> {
    for attempt in 0..attempts {
        let addr = match membership.address_of(target) {
            Some(a) => a,
            None => return Err(format!("node {target} not found in membership (announce)")),
        };
        let pooled = match pool.get_channel(addr).await {
            Ok(p) => p,
            Err(e) => {
                if attempt + 1 < attempts {
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
                return Err(format!("connection pool error for {target}: {e}"));
            }
        };
        let channel = pooled.channel().clone();
        drop(pooled);
        let client = HealingRpcClient::new(channel);
        match tokio::time::timeout(Duration::from_millis(DEFAULT_ANNOUNCE_TIMEOUT_MS), send(client))
            .await
        {
            Ok(Ok(true)) => return Ok(()),
            Ok(Ok(false)) => {
                // The receiver processed the RPC but did not accept (e.g.
                // it does not hold the segment). Not a transport failure;
                // retrying is pointless — treat as done (the g4 failsafe
                // covers whatever was genuinely missed).
                return Ok(());
            }
            Ok(Err(e)) => {
                if attempt + 1 < attempts {
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
                return Err(e);
            }
            Err(_elapsed) => {
                if attempt + 1 < attempts {
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
                return Err(format!("announce to {target} timed out"));
            }
        }
    }
    Err("announce retries exhausted".to_string())
}

/// A boxed future returned by the per-RPC send closures.
type SendRpcFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + Send + 'static>>;

/// Announces a data pool's confirmed loss to the replica holders of the
/// affected segments (ADR-0029 §D4 fast path).
///
/// `targets` is the caller-computed union of
/// `union(storage_locations(segment) − self)` over the affected set (the
/// feature's pinned fan-out — peers are NOT cluster-wide and NOT ring
/// lookups). Each target is attempted `attempts` times (default 3) at
/// `retry_delay` spacing; exhausted targets are dropped (g4 failsafe).
///
/// # Errors
///
/// Returns `Err` when at least one target could not be reached after all
/// retries (membership address missing, connection failure, timeout, or
/// RPC error). The announcement is best-effort: the g4 reconciliation
/// loop is the mandatory failsafe.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
/// use oceanfs_membership::Membership;
/// use oceanfs_network::ConnectionPool;
/// use oceanfs_node::announce::announce_pool_loss;
/// use oceanfs_routing::{Ring, RingCache};
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("n1"), "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(), GossipConfig::default(), ring,
/// ));
/// let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
/// let targets = vec![NodeId::new("n2")];
/// let segments = vec![oceanfs_core::SegmentId::new()];
/// let rt = tokio::runtime::Runtime::new().expect("runtime");
/// let _ = rt.block_on(announce_pool_loss(
///     &NodeId::new("n1"), 3, &segments, &targets, &pool, &membership,
///     None, None, None,
/// ));
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn announce_pool_loss(
    origin: &NodeId,
    pool_id: u32,
    segments: &[SegmentId],
    targets: &[NodeId],
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    attempts: Option<usize>,
    retry_delay: Option<Duration>,
    metrics: Option<&AnnounceMetrics>,
) -> Result<(), String> {
    if segments.is_empty() || targets.is_empty() {
        return Ok(());
    }
    let attempts = attempts.unwrap_or(DEFAULT_ANNOUNCE_ATTEMPTS);
    let retry_delay = retry_delay.unwrap_or(DEFAULT_ANNOUNCE_RETRY_DELAY);

    let proto_segments: Vec<oceanfs_core::proto::common::SegmentId> =
        segments.iter().map(|s| (*s).into()).collect();
    let proto_origin: oceanfs_core::proto::common::NodeId = origin.clone().into();

    let mut failures = Vec::new();
    for target in targets {
        let request = LossAnnouncement {
            origin: Some(proto_origin.clone()),
            pool_id,
            segments: proto_segments.clone(),
        };
        let result =
            deliver_with_retry(pool, membership, target, attempts, retry_delay, |mut client| {
                let req = tonic::Request::new(request.clone());
                Box::pin(async move {
                    let resp = client.announce_loss(req).await.map_err(|e| e.to_string())?;
                    // `accepted` may be 0 (receiver holds nothing) — the
                    // RPC still succeeded.
                    let _ = resp.into_inner();
                    Ok(true)
                })
            })
            .await;
        if let Some(m) = metrics {
            m.record_delivery();
        }
        if let Err(e) = result {
            tracing::warn!(
                origin = %origin,
                target = %target,
                pool_id,
                segments = segments.len(),
                error = %e,
                "loss announcement delivery failed after retries (g4 failsafe)"
            );
            failures.push(target.clone());
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("loss announcement not delivered to {} target(s)", failures.len()))
    }
}

/// Announces a compaction segment-remap to the holders of the old
/// segment (g3 Option A).
///
/// `targets` is the caller-computed `storage_locations(old) − self`. The
/// announcement carries the chunk-remap table so receivers translate
/// their object rows (`(old_offset, length) → new_offset`).
///
/// # Errors
///
/// Returns `Err` when at least one target could not be reached after all
/// retries. Best-effort — the g4 reconciliation loop is the failsafe.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use oceanfs_core::{GossipConfig, NodeId, RemappedChunk, RingConfig};
/// use oceanfs_membership::Membership;
/// use oceanfs_network::ConnectionPool;
/// use oceanfs_node::announce::announce_segment_remap;
/// use oceanfs_routing::{Ring, RingCache};
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("n1"), "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(), GossipConfig::default(), ring,
/// ));
/// let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
/// let targets = vec![NodeId::new("n2")];
/// let old = oceanfs_core::SegmentId::new();
/// let new = oceanfs_core::SegmentId::new();
/// let rt = tokio::runtime::Runtime::new().expect("runtime");
/// let _ = rt.block_on(announce_segment_remap(
///     &NodeId::new("n1"), old, new, &[], &targets, &pool, &membership,
///     None, None, None,
/// ));
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn announce_segment_remap(
    origin: &NodeId,
    old_segment_id: SegmentId,
    new_segment_id: SegmentId,
    chunks: &[RemappedChunk],
    targets: &[NodeId],
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    attempts: Option<usize>,
    retry_delay: Option<Duration>,
    metrics: Option<&AnnounceMetrics>,
) -> Result<(), String> {
    if targets.is_empty() {
        return Ok(());
    }
    let attempts = attempts.unwrap_or(DEFAULT_ANNOUNCE_ATTEMPTS);
    let retry_delay = retry_delay.unwrap_or(DEFAULT_ANNOUNCE_RETRY_DELAY);

    let proto_chunks: Vec<ProtoRemappedChunk> = chunks
        .iter()
        .map(|c| ProtoRemappedChunk {
            old_offset: c.old_offset,
            length: c.length,
            new_offset: c.new_offset,
        })
        .collect();
    let proto_origin: oceanfs_core::proto::common::NodeId = origin.clone().into();
    let proto_old: oceanfs_core::proto::common::SegmentId = old_segment_id.into();
    let proto_new: oceanfs_core::proto::common::SegmentId = new_segment_id.into();

    let mut failures = Vec::new();
    for target in targets {
        let request = SegmentRemap {
            origin: Some(proto_origin.clone()),
            old_segment_id: Some(proto_old.clone()),
            new_segment_id: Some(proto_new.clone()),
            chunks: proto_chunks.clone(),
        };
        let result =
            deliver_with_retry(pool, membership, target, attempts, retry_delay, |mut client| {
                let req = tonic::Request::new(request.clone());
                Box::pin(async move {
                    let resp = client.announce_remap(req).await.map_err(|e| e.to_string())?;
                    Ok(resp.into_inner().applied)
                })
            })
            .await;
        if let Some(m) = metrics {
            m.record_delivery();
        }
        if let Err(e) = result {
            tracing::warn!(
                origin = %origin,
                target = %target,
                old_segment_id = %old_segment_id,
                new_segment_id = %new_segment_id,
                error = %e,
                "segment-remap delivery failed after retries (g4 failsafe)"
            );
            failures.push(target.clone());
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("segment remap not delivered to {} target(s)", failures.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fan_out_targets_union_locations_minus_self() {
        let self_id = NodeId::new("a");
        let b = NodeId::new("b");
        let c = NodeId::new("c");
        let d = NodeId::new("d");
        let segments = vec![
            (SegmentId::new(), vec![self_id.clone(), b.clone(), c.clone()]),
            (SegmentId::new(), vec![c.clone(), d.clone()]),
            (SegmentId::new(), vec![self_id.clone()]), // self-only → contributes nothing
        ];
        let targets = derive_fan_out_targets(&segments, &self_id);
        assert_eq!(targets, vec![b, c, d]);
    }

    #[test]
    fn fan_out_targets_deduplicates_and_orders_first_seen() {
        let self_id = NodeId::new("a");
        let b = NodeId::new("b");
        let c = NodeId::new("c");
        let segments = vec![
            (SegmentId::new(), vec![c.clone(), b.clone()]),
            (SegmentId::new(), vec![b.clone(), c.clone()]),
        ];
        let targets = derive_fan_out_targets(&segments, &self_id);
        // First-seen order, no duplicates.
        assert_eq!(targets, vec![c, b]);
    }

    #[test]
    fn fan_out_targets_empty_when_all_self() {
        let self_id = NodeId::new("a");
        let segments = vec![(SegmentId::new(), vec![self_id.clone()]), (SegmentId::new(), vec![])];
        assert!(derive_fan_out_targets(&segments, &self_id).is_empty());
    }

    #[test]
    fn fan_out_targets_empty_for_no_segments() {
        let self_id = NodeId::new("a");
        assert!(derive_fan_out_targets(&[], &self_id).is_empty());
    }
}
