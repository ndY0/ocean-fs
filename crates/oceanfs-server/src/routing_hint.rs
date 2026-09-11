//! Peer-side cached routing state (ADR-0029 §D5) — a hint, never a
//! dependency.
//!
//! The read path (replica selection for a GET) and the write path
//! (replica target selection for a PUT) consult the node's cached
//! storage-pool manifests to **order** candidates: `Preferred` first
//! (Healthy data pool, not `write_degraded`), then `Fallback`
//! (reachable but Degraded). Only `Excluded` candidates are skipped
//! (node unavailable, `write_degraded`, or no usable data pool at all).
//! Degraded is a preference, never an exclusion — the f5 fix restoring
//! ADR-0029 §D3/D5: "the cache optimizes; the error path guarantees".
//!
//! The trait is defined in the CONSUMING crate (`oceanfs-server`),
//! per architecture §2.1: the implementation lives in the composition
//! root's `ManifestCache` (`oceanfs-node::routing_cache`), which owns
//! the manifest data, the classification policy, and the routing
//! metrics. The coordinators hold `Option<Arc<dyn RoutingHint>>` —
//! `None` disables the hint entirely (every candidate is preferred).
//!
//! Failover semantics: the cache optimizes; the error path guarantees.
//! An I/O error on a candidate replica falls through to the next
//! candidate regardless of what the cache said — `on_failover` only
//! records the event.

use oceanfs_core::NodeId;

/// The routing preference class of a candidate replica (ADR-0029 §D5).
///
/// Candidates are attempted `Preferred` first, then `Fallback`; only
/// `Excluded` is skipped. A Degraded data pool is a `Fallback`, not an
/// exclusion: reads still serve from it and writes still replicate to it
/// when no Healthy alternative exists.
///
/// # Examples
///
/// ```
/// use oceanfs_server::routing_hint::CandidateClass;
///
/// assert_ne!(CandidateClass::Preferred, CandidateClass::Fallback);
/// assert_ne!(CandidateClass::Fallback, CandidateClass::Excluded);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateClass {
    /// A Healthy data pool is available (and the candidate is not
    /// hard-degraded).
    Preferred,
    /// Reachable and usable, but no Healthy data pool remains (Degraded
    /// or draining data pools only). Attempted after every `Preferred`
    /// candidate.
    Fallback,
    /// Must not be attempted: node unavailable, `write_degraded` (write
    /// path only), or no usable data pool at all (all Dead/absent).
    Excluded,
}

/// Which coordinator consulted the fallback tier (metric label).
///
/// # Examples
///
/// ```
/// use oceanfs_server::routing_hint::FallbackPath;
///
/// assert_ne!(FallbackPath::Read, FallbackPath::Write);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPath {
    /// The read path ordered a fallback read candidate.
    Read,
    /// The write path ordered a fallback replica target.
    Write,
}

/// The routing-hint source consulted by the read/write coordinators.
///
/// Implementations must be cheap and lock-free on the hot path (perf
/// 2.4: `ArcSwap`-backed, wholesale-replaced map — no lock in the
/// read/write path, perf 7.2).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_core::NodeId;
/// use oceanfs_server::routing_hint::{CandidateClass, RoutingHint};
///
/// /// A test hint: never excludes, counts failovers.
/// struct NoopHint(std::sync::atomic::AtomicU64);
///
/// impl RoutingHint for NoopHint {
///     fn exclude_read_candidate(&self, _node_id: &NodeId) -> bool { false }
///     fn exclude_write_target(&self, _node_id: &NodeId) -> bool { false }
///     fn on_failover(&self) {
///         self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
///     }
/// }
///
/// let hint: Arc<dyn RoutingHint> = Arc::new(NoopHint(Default::default()));
/// assert!(!hint.exclude_read_candidate(&NodeId::new("peer")));
/// // Implementors that do not override the tier methods stay
/// // preferred/not-preferred (backwards compatible).
/// assert_eq!(
///     hint.read_candidate_class(&NodeId::new("peer")),
///     CandidateClass::Preferred
/// );
/// ```
pub trait RoutingHint: Send + Sync {
    /// Whether `node_id` must be **hard-excluded** as a read candidate:
    /// node unavailable, or no usable data pool at all.
    ///
    /// Returns `false` when the cache has no entry (unknown peer = no
    /// pool info): the caller proceeds and relies on the error-driven
    /// fallback to the next replica — the cache is a hint, the error is
    /// the truth.
    fn exclude_read_candidate(&self, node_id: &NodeId) -> bool;

    /// Whether `node_id` must be **hard-excluded** as a write target:
    /// node unavailable, `write_degraded` (role consequence, ADR-0029
    /// §D3), or no writable data pool at all.
    ///
    /// Returns `false` when the cache has no entry (unknown peer stays
    /// eligible; write failures become hinted-handoff debt).
    fn exclude_write_target(&self, node_id: &NodeId) -> bool;

    /// Records an error-driven failover: a candidate replica failed at
    /// I/O time (timeout, connection error, disk error) and the caller
    /// is falling through to the next replica. Backs the
    /// `oceanfs_routing_failover_total` metric.
    fn on_failover(&self);

    /// Tier-aware read classification (ADR-0029 §D5).
    ///
    /// The default implementation keeps older hints binary
    /// (excluded / preferred); the `ManifestCache` overrides it to
    /// return `Fallback` for Degraded-but-usable candidates.
    fn read_candidate_class(&self, node_id: &NodeId) -> CandidateClass {
        if self.exclude_read_candidate(node_id) {
            CandidateClass::Excluded
        } else {
            CandidateClass::Preferred
        }
    }

    /// Tier-aware write classification (ADR-0029 §D5).
    ///
    /// The default implementation keeps older hints binary
    /// (excluded / preferred); the `ManifestCache` overrides it to
    /// return `Fallback` for Degraded-but-writable candidates.
    fn write_target_class(&self, node_id: &NodeId) -> CandidateClass {
        if self.exclude_write_target(node_id) {
            CandidateClass::Excluded
        } else {
            CandidateClass::Preferred
        }
    }

    /// Records that the fallback tier was consulted: a Degraded
    /// candidate was ordered for an attempt (the coordinator will only
    /// reach it if every preferred candidate fails). Backs the
    /// `oceanfs_routing_degraded_fallbacks_total{path}` metric.
    fn on_degraded_fallback(&self, _path: FallbackPath) {}
}

/// Orders read candidates `Preferred` first, then `Fallback`; `Excluded`
/// candidates are dropped. The relative order inside each tier is the
/// input order (replica-set order).
///
/// Calls [`RoutingHint::on_degraded_fallback`] with
/// [`FallbackPath::Read`] once when at least one fallback candidate was
/// ordered — the metric counts "a Degraded replica was consulted", not
/// "the fallback candidate was eventually attempted".
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeId;
/// use oceanfs_server::routing_hint::{order_read_candidates, CandidateClass, RoutingHint};
///
/// struct Classify;
/// impl RoutingHint for Classify {
///     fn exclude_read_candidate(&self, _: &NodeId) -> bool { false }
///     fn exclude_write_target(&self, _: &NodeId) -> bool { false }
///     fn on_failover(&self) {}
///     fn read_candidate_class(&self, node_id: &NodeId) -> CandidateClass {
///         match node_id.as_str() {
///             "degraded" => CandidateClass::Fallback,
///             "dead" => CandidateClass::Excluded,
///             _ => CandidateClass::Preferred,
///         }
///     }
/// }
///
/// let hint = Classify;
/// let ordered = order_read_candidates(
///     vec![NodeId::new("degraded"), NodeId::new("dead"), NodeId::new("healthy")],
///     Some(&hint),
/// );
/// let ids: Vec<&str> = ordered.iter().map(|n| n.as_str()).collect();
/// assert_eq!(ids, vec!["healthy", "degraded"]);
/// ```
pub fn order_read_candidates(
    candidates: impl IntoIterator<Item = NodeId>,
    hint: Option<&dyn RoutingHint>,
) -> Vec<NodeId> {
    let Some(hint) = hint else {
        return candidates.into_iter().collect();
    };
    let mut preferred = Vec::new();
    let mut fallback = Vec::new();
    for node in candidates {
        match hint.read_candidate_class(&node) {
            CandidateClass::Preferred => preferred.push(node),
            CandidateClass::Fallback => fallback.push(node),
            CandidateClass::Excluded => {}
        }
    }
    if !fallback.is_empty() {
        hint.on_degraded_fallback(FallbackPath::Read);
        preferred.append(&mut fallback);
    }
    preferred
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct Classify;

    impl RoutingHint for Classify {
        fn exclude_read_candidate(&self, _: &NodeId) -> bool {
            false
        }
        fn exclude_write_target(&self, _: &NodeId) -> bool {
            false
        }
        fn on_failover(&self) {}
        fn read_candidate_class(&self, node_id: &NodeId) -> CandidateClass {
            match node_id.as_str() {
                "dead" | "unavailable" => CandidateClass::Excluded,
                s if s.starts_with("degraded") => CandidateClass::Fallback,
                _ => CandidateClass::Preferred,
            }
        }
    }

    #[test]
    fn order_read_candidates_puts_preferred_first_and_drops_excluded() {
        let ordered = order_read_candidates(
            vec![
                NodeId::new("degraded-a"),
                NodeId::new("dead"),
                NodeId::new("healthy-a"),
                NodeId::new("degraded-b"),
                NodeId::new("healthy-b"),
            ],
            Some(&Classify),
        );
        let ids: Vec<&str> = ordered.iter().map(|n| n.as_str()).collect();
        assert_eq!(ids, vec!["healthy-a", "healthy-b", "degraded-a", "degraded-b"]);
    }

    #[test]
    fn order_read_candidates_without_hint_is_passthrough() {
        let ordered = order_read_candidates(vec![NodeId::new("b"), NodeId::new("a")], None);
        let ids: Vec<&str> = ordered.iter().map(|n| n.as_str()).collect();
        assert_eq!(ids, vec!["b", "a"]);
    }

    #[test]
    fn default_tier_methods_follow_the_exclusion_predicates() {
        struct Binary;
        impl RoutingHint for Binary {
            fn exclude_read_candidate(&self, node_id: &NodeId) -> bool {
                node_id.as_str() == "no"
            }
            fn exclude_write_target(&self, node_id: &NodeId) -> bool {
                node_id.as_str() == "no"
            }
            fn on_failover(&self) {}
        }
        assert_eq!(Binary.read_candidate_class(&NodeId::new("no")), CandidateClass::Excluded);
        assert_eq!(Binary.read_candidate_class(&NodeId::new("yes")), CandidateClass::Preferred);
        assert_eq!(Binary.write_target_class(&NodeId::new("no")), CandidateClass::Excluded);
        assert_eq!(Binary.write_target_class(&NodeId::new("yes")), CandidateClass::Preferred);
    }
}
