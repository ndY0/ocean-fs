//! Read repair integration tests.
//!
//! Verifies that conflict resolution correctly identifies stale replicas
//! using the configured ConflictResolver.

#![allow(clippy::unwrap_used)]

use oceanfs_core::{ConflictResolver, Hlc, LwwResolver};

#[test]
fn lww_resolver_prefers_newer_wall_time() {
    let resolver = LwwResolver;
    let old = Hlc::new(1000, 0);
    let new = Hlc::new(2000, 0);

    // When remote is newer: remote accepted.
    let result = resolver.resolve(&old, &new);
    assert!(result.is_remote_accepted());

    // When local is newer: remote rejected.
    let result = resolver.resolve(&new, &old);
    assert!(!result.is_remote_accepted());
}

#[test]
fn lww_resolver_prefers_higher_logical_clock_when_wall_time_equal() {
    let resolver = LwwResolver;
    let lower = Hlc::new(1000, 1);
    let higher = Hlc::new(1000, 2);

    // Remote with higher logical clock wins.
    let result = resolver.resolve(&lower, &higher);
    assert!(result.is_remote_accepted());

    // Local with higher logical clock: remote rejected.
    let result = resolver.resolve(&higher, &lower);
    assert!(!result.is_remote_accepted());
}

#[test]
fn lww_resolver_tie_break_keeps_local() {
    let resolver = LwwResolver;
    let same = Hlc::new(1000, 5);
    // When HLCs are exactly equal, remote is NOT accepted (local kept).
    let result = resolver.resolve(&same, &same);
    assert!(!result.is_remote_accepted());
}
