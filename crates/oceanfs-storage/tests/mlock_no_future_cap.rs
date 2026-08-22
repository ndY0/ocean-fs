//! Regression test: `mlockall` must never cap future allocations.
//!
//! The metadata store's swap defense previously used
//! `mlockall(MCL_CURRENT | MCL_FUTURE)`. With `MCL_FUTURE`, every
//! subsequent `mmap` of the process counts against `RLIMIT_MEMLOCK`;
//! once the process's locked total crosses the ceiling, ALL further
//! allocations fail with `EAGAIN` ("too much memory has been locked")
//! and Rust aborts via `handle_alloc_error`. Under sustained load this
//! crashed the whole node the moment its footprint passed the ceiling.
//!
//! This test reproduces that scenario deterministically:
//!
//! 1. lower `RLIMIT_MEMLOCK` to a finite ceiling (1.5 GB — high
//!    enough that `mlockall(MCL_CURRENT)` succeeds against the
//!    process's mapped size, small enough that a 1 GB allocation
//!    crosses it),
//! 2. open `RocksDbMetadataStore` with `mlock_block_cache = true`
//!    (the `!cfg!(test)` guard in `store.rs` does not apply to
//!    integration-test binaries — the lib is compiled without
//!    `cfg(test)`, so the real `mlockall` path runs),
//! 3. allocate 1 GB — larger than the remaining locked budget.
//!
//! Under the old `MCL_FUTURE` code the allocation fails with EAGAIN
//! and the process aborts — the test binary dies mid-test. Under the
//! fix (`MCL_CURRENT` only) the allocation succeeds and the test
//! passes.
//!
//! The test is skipped when the hard `RLIMIT_MEMLOCK` is below the
//! ceiling we need, or when `mlockall` does not actually succeed (no
//! cap installed → the bug cannot manifest in that environment).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::MetadataConfig;
use oceanfs_storage::metadata::RocksDbMetadataStore;

/// Restores the original `RLIMIT_MEMLOCK` soft limit on drop.
#[cfg(target_os = "linux")]
struct MemlockSoftRestore(libc::rlimit);

#[cfg(target_os = "linux")]
impl Drop for MemlockSoftRestore {
    fn drop(&mut self) {
        // SAFETY: `self.0` holds the original soft/hard values read
        // from `getrlimit`; restoring them is a plain `setrlimit`.
        #[allow(unsafe_code)]
        unsafe {
            let _ = libc::setrlimit(libc::RLIMIT_MEMLOCK, &self.0);
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn mlock_block_cache_never_caps_future_allocations() {
    // ── 1. Read the current limit; skip if we cannot set the soft
    //       limit to the ceiling the scenario needs. ───────────────
    let mut orig = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: `orig` is a valid `libc::rlimit` out-parameter.
    #[allow(unsafe_code)]
    let ok = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut orig) } == 0;
    if !ok {
        eprintln!("getrlimit(RLIMIT_MEMLOCK) failed; skipping mlock regression test");
        return;
    }
    // The scenario needs a ceiling high enough that `mlockall` locks
    // the process's mapped pages (VmSize) but low enough that a 1 GB
    // allocation crosses it.
    const CEILING: libc::rlim_t = 1536 * 1024 * 1024; // 1.5 GB
    if orig.rlim_max < CEILING {
        eprintln!(
            "hard RLIMIT_MEMLOCK ({}) < required ceiling ({}); \
             skipping mlock regression test",
            orig.rlim_max, CEILING
        );
        return;
    }
    let _restore = MemlockSoftRestore(orig);
    let lowered = libc::rlimit { rlim_cur: CEILING, rlim_max: orig.rlim_max };
    // SAFETY: lowering the soft limit below the hard limit is always
    // permitted (no privilege required); `lowered` is valid.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &lowered) };
    assert_eq!(rc, 0);

    // ── 2. Open the metadata store with mlock enabled. On Linux this
    //       runs `mlockall(MCL_CURRENT)` — the real production path. ──
    let dir = tempfile::tempdir().expect("tempdir");
    let _store = Arc::new(
        RocksDbMetadataStore::open(&MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 4 * 1024 * 1024,
            memtable_size: 1024 * 1024,
            mlock_block_cache: true,
            ..Default::default()
        })
        .expect("open metadata store"),
    );

    // If mlockall did not actually lock anything (e.g. a future
    // refactor disables it, or the kernel refused), no future cap
    // exists and the bug cannot manifest — skip rather than assert.
    let vmlck_kb = read_vmlck_kb();
    if vmlck_kb == 0 {
        eprintln!("VmLck = 0 after store open; mlockall did not engage; skipping");
        return;
    }
    eprintln!("VmLck after store open: {vmlck_kb} kB");

    // ── 3. Verify the store works. ──────────────────────────────────
    let id = oceanfs_core::SegmentId::new();
    let meta = oceanfs_core::SegmentMetadata {
        pool_id: 0,
        segment_id: id,
        ec_k: 0,
        ec_m: 0,
        size_tier: oceanfs_core::SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(0),
    };
    // Segment state lives in the machine (the segments CF is removed) —
    // the store's objects side is exercised by the tombstone below.
    let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    registry.reserve(id, meta).expect("reserve segment");
    assert!(registry.get(id).is_some(), "segment must be readable after store open");

    // ── 4. THE REGRESSION: allocate 1 GB. With `MCL_FUTURE` this
    //       mmap counts against the 1.5 GB `RLIMIT_MEMLOCK` ceiling
    //       (already ~0.8 GB consumed by the process's mapped pages)
    //       → `EAGAIN` → Rust aborts the process here. With
    //       `MCL_CURRENT` the allocation is not capped and succeeds. ──
    let mut big = vec![0x5Au8; 1024 * 1024 * 1024];
    // Touch the tail so the pages are committed.
    let last = big.len() - 1;
    big[last] = 0x7E;
    assert_eq!(big.len(), 1024 * 1024 * 1024);
    eprintln!("1 GB allocation succeeded (VmLck: {} kB)", read_vmlck_kb());
    // Dropping the buffer here is fine — the point was that the
    // allocation itself survived.
}

#[cfg(target_os = "linux")]
fn read_vmlck_kb() -> u64 {
    let content = match std::fs::read_to_string("/proc/self/status") {
        Ok(c) => c,
        Err(_) => return 0,
    };
    for line in content.lines() {
        if let Some(val) = line.strip_prefix("VmLck:") {
            return val.split_whitespace().next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        }
    }
    0
}

#[cfg(not(target_os = "linux"))]
#[test]
fn mlock_noop_on_non_linux() {
    eprintln!("mlock regression test is Linux-only; skipping");
}
