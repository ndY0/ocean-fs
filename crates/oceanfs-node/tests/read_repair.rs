//! Read repair integration tests.
//!
//! Verifies that conflict resolution correctly identifies stale replicas
//! using the configured ConflictResolver.

#![allow(clippy::unwrap_used)]

use oceanfs_core::{ConflictResolver, Hlc, LwwResolver, NodeId};

fn node(name: &str) -> NodeId {
    NodeId::new(name)
}

#[test]
fn lww_resolver_prefers_newer_wall_time() {
    let resolver = LwwResolver;
    let old = Hlc::new(1000, 0);
    let new = Hlc::new(2000, 0);

    // When remote is newer: remote accepted.
    let result = resolver.resolve(&old, &new, &node("n1"), &node("n2"));
    assert!(result.is_remote_accepted());

    // When local is newer: remote rejected.
    let result = resolver.resolve(&new, &old, &node("n1"), &node("n2"));
    assert!(!result.is_remote_accepted());
}

#[test]
fn lww_resolver_prefers_higher_logical_clock_when_wall_time_equal() {
    let resolver = LwwResolver;
    let lower = Hlc::new(1000, 1);
    let higher = Hlc::new(1000, 2);

    // Remote with higher logical clock wins.
    let result = resolver.resolve(&lower, &higher, &node("n1"), &node("n2"));
    assert!(result.is_remote_accepted());

    // Local with higher logical clock: remote rejected.
    let result = resolver.resolve(&higher, &lower, &node("n1"), &node("n2"));
    assert!(!result.is_remote_accepted());
}

#[test]
fn lww_resolver_tie_break_uses_node_id() {
    let resolver = LwwResolver;
    let same = Hlc::new(1000, 5);
    // Equal HLCs: the lexicographically greater node id wins (G7).
    let result = resolver.resolve(&same, &same, &node("node-a"), &node("node-z"));
    assert!(result.is_remote_accepted(), "greater remote node id must win");
    let result = resolver.resolve(&same, &same, &node("node-z"), &node("node-a"));
    assert!(!result.is_remote_accepted(), "lesser remote node id must lose");
}
