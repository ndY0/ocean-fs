//! Integration test: HLC ordering across nodes.
//!
//! Simulates multi-node HLC behavior: node A writes, node B writes
//! concurrently, verify HLC ordering yields deterministic LWW outcome.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{ConflictResolver, Hlc, HlcClock, LwwResolver, NodeId, Resolution};

#[test]
fn hlc_monotonic_across_multiple_clocks() {
    let clock_a = HlcClock::new();
    let clock_b = HlcClock::new();

    let mut prev_a = clock_a.now();
    let mut prev_b = clock_b.now();
    for _ in 0..50 {
        let curr_a = clock_a.now();
        let curr_b = clock_b.now();
        assert!(curr_a > prev_a, "clock A must be monotonic");
        assert!(curr_b > prev_b, "clock B must be monotonic");
        prev_a = curr_a;
        prev_b = curr_b;
    }
}

#[test]
fn hlc_receive_merge_yields_causal_ordering() {
    // Node A generates a timestamp, sends it to B.
    let clock_a = HlcClock::new();
    let clock_b = HlcClock::new();

    let a_ts = clock_a.now();
    let b_merged = clock_b.update(a_ts);

    // After merging, B's clock must be ahead of A's original timestamp.
    assert!(b_merged > a_ts, "merged HLC must be > received HLC");
}

#[test]
fn lww_resolver_deterministic() {
    let resolver = LwwResolver;
    let ts1 = Hlc::new(1000, 0);
    let ts2 = Hlc::new(1000, 1);
    let n1 = NodeId::new("node-1");
    let n2 = NodeId::new("node-2");

    let r1 = resolver.resolve(&ts1, &ts2, &n1, &n2);
    let r2 = resolver.resolve(&ts2, &ts1, &n2, &n1);

    assert!(r1.is_remote_accepted(), "newer remote should win");
    assert!(r2.is_local_accepted(), "older remote should lose");
}

#[test]
fn lww_resolver_equal_hlc_is_deterministic() {
    let resolver = LwwResolver;
    let ts = Hlc::new(500, 3);
    // Equal HLCs: tie-break by node id — the greater remote id wins (G7).
    assert_eq!(
        resolver.resolve(&ts, &ts, &NodeId::new("node-a"), &NodeId::new("node-z")),
        Resolution::AcceptRemote,
    );
    assert_eq!(
        resolver.resolve(&ts, &ts, &NodeId::new("node-z"), &NodeId::new("node-a")),
        Resolution::AcceptLocal,
    );
}

#[test]
fn hlc_clock_cache_line_alignment() {
    // Verify alignment to prevent false sharing (perf rule 6.1).
    assert_eq!(std::mem::align_of::<HlcClock>(), 64);
}
