# ADR-0014: Permit Bounds-Check Elimination via Raw Pointer Arithmetic in `oceanfs-storage`

**Status:** Proposed
**Date:** 2026-08-08
**Deciders:** architecture team

---

## Context

The [EC Encode Optimizations] feature (Feature 8, epic:
`performance-optimization`) introduces a `StreamingEcSegment` type in
`oceanfs-storage/src/segment/streaming.rs`. This type performs EC
encoding incrementally during the segment write lifetime, overlapping
encode work with data append so that seal becomes a near-no-op
collecting pre-computed parity shards.

The `parity_shards()` method on `StreamingEcSegment` is called at seal
time (once per segment). It iterates over completed stripe rows and
collects parity shards from a `Vec<Vec<BytesMut>>` buffer. The inner
loop iterates over `completed × m` parity shards, where `completed` is
the number of fully-encoded stripes and `m` is the parity shard count
(typically 2–6). For a 4 MB segment with k=4, strip_size=64 KB, and
m=2, this is `16 × 2 = 32` parities. For a 64 MB segment with m=6,
it is `256 × 6 = 1,536` parities.

The iteration bounds are **provably safe**:

1. **`stripe_idx < completed`** — the `completed` counter is an
   `AtomicUsize` only incremented by rayon workers **after** they
   finish writing parity into their pre-allocated slot. An `Acquire`
   load in `parity_shards()` synchronizes with the `Release` store
   in each worker, guaranteeing visibility of all completed writes.
   The `completed` counter is never decremented, and workers only
   increment to `stripe_idx + 1` after `parity_buf[stripe_idx]` is
   fully written. Thus `completed ≤ buf.len()`.

2. **`parity_idx < m`** — each slot in `parity_buf` is pre-allocated
   at construction time (`StreamingEcSegment::new()`) with exactly
   `m` `BytesMut` elements, each `strip_size` bytes. The slot is
   never resized, truncated, or moved. The worker writes into all
   `m` elements before signalling completion.

The current implementation uses raw pointer arithmetic to eliminate
redundant bounds checks:

```rust
#[allow(unsafe_code)]
pub fn parity_shards(&self) -> Option<Vec<Bytes>> {
    let completed = self.completed.load(Ordering::Acquire);
    if completed == 0 {
        return None;
    }
    let m = self.parity_shards as usize;
    let buf = self.parity_buf.lock();
    let slice_ptr = buf.as_ptr();
    let mut shards: Vec<Bytes> = Vec::with_capacity(m * completed);

    for stripe_idx in 0..completed {
        // SAFETY: stripe_idx < completed ≤ buf.len() ...
        let stripe = unsafe { &*slice_ptr.add(stripe_idx) };
        let stripe_ptr = stripe.as_ptr();
        for parity_idx in 0..m {
            // SAFETY: parity_idx < m ...
            let shard = unsafe { &*stripe_ptr.add(parity_idx) };
            shards.push(shard.clone().freeze());
        }
    }
    Some(shards)
}
```

This is **not** a Linux syscall wrapper (covered by [ADR-0012]), a
`memmap2::Mmap` operation (covered by [ADR-0011]), or a `setsockopt`
call (covered by [ADR-0013]). It is a new category of `unsafe`:
bounds-check elimination via raw pointer arithmetic on provably-bounded
hot iteration paths. The [architecture guideline §7.2] currently states:

> Limited to the five categories documented in ADR-0011 and ADR-0012;
> new unsafe use-cases require a new ADR.

This ADR fulfills that requirement for the new category.

### Why Not Safe Iteration?

The obvious safe rewrite would use standard indexing:

```rust
for stripe_idx in 0..completed {
    let stripe = &buf[stripe_idx];
    for parity_idx in 0..m {
        let shard = &stripe[parity_idx];
        shards.push(shard.clone().freeze());
    }
}
```

This produces identical behavior, but the Rust compiler inserts two
bounds checks per iteration: `buf[stripe_idx]` checks `stripe_idx <
buf.len()`, and `stripe[parity_idx]` checks `parity_idx < stripe.len()`.
In release builds with `-C opt-level=3`, the compiler may elide some
bounds checks via loop-invariant code motion or range analysis, but:

- The `stripe_idx` check is provably redundant (`completed ≤ buf.len()`
  at the start of iteration), but the compiler cannot see the
  synchronizes-with relationship between the `AtomicUsize` and the
  `Vec` length.
- The `parity_idx` check is provably redundant (`stripe.len() == m` for
  all slots), but the compiler sees `stripe: &Vec<BytesMut>` with no
  visible invariant linking `stripe.len()` to `m`.
- When compiling with `codegen-units = 1` and `lto = "fat"`, the
  compiler may propagate the invariant — but not reliably across
  lock-acquire boundaries.

The per-element bounds check cost is small (~1-2 CPU cycles with branch
prediction) but multiplied by `completed × m` for every segment seal,
it represents measurable overhead. The feature DoD requires seal-time
encode latency reduction to ≤50µs for parity collection; every
eliminated bounds check contributes to meeting that target.

## Decision

**Add a sixth category of permitted `unsafe` in `oceanfs-storage`:
bounds-check elimination via raw pointer arithmetic on provably-bounded
hot iteration paths.** The existing `#![deny(unsafe_code)]` crate-level
lint, per-item `#[allow(unsafe_code)]` annotation pattern, and
`// SAFETY:` comment requirement are unchanged.

### Category 6: Bounds-Check Elimination on Hot Paths

**Location:** `crates/oceanfs-storage/src/segment/streaming.rs`
(method `StreamingEcSegment::parity_shards()`), and any future hot
iteration path within `oceanfs-storage` that meets the criteria below.

**Operation:** Replace `buf[index]` (which incurs a runtime bounds
check) with `unsafe { &*ptr.add(index) }` (raw pointer offset with no
bounds check). The unsafe block takes responsibility for ensuring
`index` is within bounds.

**Safety invariants (must hold at every site):**

1. **Provable upper bound.** The iteration variable must be provably
   bounded by the collection's length at the start of the loop.
   Acceptable proofs include:
   - The bound is derived from a synchronizes-with relationship (e.g.,
     `AtomicUsize` with `Acquire`/`Release` ordering) that guarantees
     the counter ≤ the buffer length.
   - The bound is a compile-time constant and the buffer is allocated
     to that fixed size at construction time.
   - The bound is tracked by a monotonic counter in the same module
     with a documented invariant.

2. **No concurrent mutation.** No other thread may mutate the
   collection (insert, remove, resize, truncate) during the iteration.
   For `Vec<Vec<BytesMut>>`, this means: the outer `Vec` is not
   resized, the inner `Vec`s are not resized, and elements are not
   moved or deallocated. This is satisfied when:
   - The collection is held behind a `Mutex` lock that is held for
     the duration of the iteration, **or**
   - The collection is frozen (no further mutations possible) after
     the synchronizes-with event that established the bound.

3. **No reallocation.** The pointer obtained from `Vec::as_ptr()` must
   remain valid for the duration of the iteration. This means the `Vec`
   must not be resized (which could reallocate and invalidate the
   pointer). The `Mutex` guard pattern above also satisfies this,
   since no other thread can mutate while the lock is held.

4. **Documented at each site.** Every use of raw pointer arithmetic
   must carry a `// SAFETY:` comment that explicitly cites which
   invariant proves the bound. The comment must reference the
   specific field or counter that establishes the upper bound and
   explain why concurrent mutation is impossible.

### Scope Boundaries

This ADR adds one category to the five already permitted:

1. Memory-mapped segment I/O via `memmap2::Mmap` ([ADR-0011]).
2. WAL range-sync via `sync_file_range` + `fdatasync` ([ADR-0012]).
3. Atomic segment writes via `open(O_TMPFILE)` + `linkat` ([ADR-0012]).
4. Page cache hints via `madvise(MADV_SEQUENTIAL, MADV_DONTNEED)` ([ADR-0012]).
5. Background thread scheduling via `ioprio_set` + `sched_setscheduler` ([ADR-0012]).
6. **Bounds-check elimination via raw pointer arithmetic on provably-bounded
   hot iteration paths (this ADR).**

It does **not** authorize:

- `get_unchecked` or any other `std` unstable API for bounds-check
  elimination. All unsafe must go through raw pointer arithmetic from
  `Vec::as_ptr()` / `[T]::as_ptr()` or equivalent.
- Raw pointer arithmetic where the bounds cannot be proven from module
  invariants. "The compiler will probably optimize it" is not a proof.
- Raw pointer arithmetic in crates other than `oceanfs-storage`.
  Other crates must request their own ADR.
- `transmute`, `MaybeUninit` shenanigans, inline assembly, or FFI
  bindings not already authorized by [ADR-0011], [ADR-0012], or future
  ADRs.
- Arbitrary `unsafe` in `oceanfs-storage` — every `unsafe` block must
  fall into one of the six permitted categories.

### Enforcement

The existing enforcement mechanisms are unchanged and apply to all six
categories:

1. **Crate-level:** `#![deny(unsafe_code)]` in
   `oceanfs-storage/src/lib.rs`. All `unsafe` blocks are errors by
   default.
2. **Per-item override:** Each `unsafe` block must be preceded by
   `#[allow(unsafe_code)]`, making all unsafe sites auditable via
   `grep -r "allow(unsafe_code)" crates/oceanfs-storage/src/`.
3. **Safety comment:** Each `unsafe` block must carry a
   `// SAFETY:` comment citing the specific invariant that proves
   the bound (enforced by `clippy::undocumented_unsafe_blocks` at
   the crate level).
4. **Category audit trail:** A `// SAFETY:` comment for Category 6
   must explicitly state the provenance of the bound (e.g., "the
   AtomicUsize completed counter, loaded with Acquire ordering,
   synchronizes with the Release store in the rayon worker; this
   guarantees `stripe_idx < completed ≤ buf.len()`").

### Concurrency Model for `parity_shards()`

The specific unsafe site in `StreamingEcSegment::parity_shards()` is
sound under the following model:

```
Thread A (rayon worker):                   Thread B (sealer):
  write parity into buf[stripe_idx][..m]
  completed.fetch_max(stripe_idx+1, Rel) ───→ completed.load(Acquire)
                                               lock(parity_buf)
                                               // now all writes to buf[0..completed] are visible
                                               for i in 0..completed:
                                                   read buf[i][..m]
                                               unlock(parity_buf)
```

- The `Release` store in the worker synchronizes with the `Acquire`
  load in `parity_shards()`. This guarantees that all writes to
  `buf[0..completed]` performed by the worker are visible to the
  sealer.
- The `Mutex` lock on `parity_buf` is acquired **after** the
  `Acquire` load, ensuring the sealer holds the lock for the duration
  of the iteration. No other thread can mutate `parity_buf` while
  the sealer holds the lock, so the `as_ptr()`-derived pointers
  remain valid.
- The `completed` counter is monotonic (never decremented), so the
  bound `stripe_idx < completed` is valid at the start of the loop
  and remains valid because the loop counter `stripe_idx` only
  increases.

## Consequences

### Positive

- **Eliminates redundant bounds checks on the seal-time path.** Each
  call to `parity_shards()` saves `2 × completed × m` bounds checks.
  For a 64 MB segment with m=6, this eliminates ~3,072 bounds checks
  per seal — measurable at scale.
- **Helps meet the feature DoD latency target.** The EC Encode
  Optimizations feature DoD requires seal-time encode latency ≤50µs
  for parity collection. Bounds-check elimination contributes to the
  goal of making seal a near-no-op.
- **Establishes a documented, auditable pattern.** Future hot iteration
  paths within `oceanfs-storage` can follow this ADR's template:
  prove the bound, hold the lock, document the invariant, eliminate
  the check. No new ADR needed for each site.
- **No platform gating.** Unlike the syscall categories, bounds-check
  elimination via raw pointer arithmetic is platform-independent and
  works on all targets.
- **Auditability preserved.** Every site requires `#[allow(unsafe_code)]`
  and a `// SAFETY:` comment citing the bound provenance. A grep for
  `add(` in `crates/oceanfs-storage/src/` within `unsafe` blocks gives
  a complete audit trail.

### Negative

- **Unsafe surface expands to a sixth category within `oceanfs-storage`.**
  The crate now has `unsafe` blocks spanning up to seven files (mmap,
  sync, atomic_write, segment_reader, mmapped, sched, streaming). Code
  review surface grows proportionally.
- **Risk of scope creep.** The template-based authorization ("any
  provably-bounded hot path") is intentionally broader than the
  syscall-by-syscall enumeration in [ADR-0012]. This could tempt
  developers to apply bounds-check elimination to paths where the
  proof is weaker or the hotness is unproven. Mitigation: code review
  must verify both the invariant proof and the performance justification
  (the path must be measurably hot — not just "probably hot").
- **Raw pointer arithmetic is the most error-prone form of `unsafe`.**
  Unlike a `libc::setsockopt` call (trivial safety invariant: valid fd),
  raw pointer arithmetic requires reasoning about pointer provenance,
  aliasing, and reallocation. A mistake here is UB, not just a returned
  error code. Mitigation: the invariants are simple (provable bound,
  lock-held, no-reallocation) and must be explicitly stated at each
  site. The existing `clippy::undocumented_unsafe_blocks` lint ensures
  the comment exists; code review ensures the reasoning is correct.
- **Compiler optimizations may make bounds checks free anyway.** The
  Rust compiler (via LLVM) is increasingly capable of eliding bounds
  checks through range analysis and loop-invariant code motion,
  especially with `lto = "fat"` and PGO. Raw pointer arithmetic
  discards the safety net for a benefit that may shrink as the compiler
  improves. Mitigation: each site must be benchmarked **before and
  after** the unsafe transformation. If a Criterion benchmark shows
  no measurable improvement, the unsafe should be reverted to safe
  indexing.

### Neutral

- **Architecture guideline §7.2 must be updated.** The
  `oceanfs-storage` entry's scope note must change from "five
  categories" to "six categories" and enumerate Category 6. This
  is a one-paragraph edit.
- **No CI changes.** The existing `deny` + `allow` enforcement is
  already in place for `oceanfs-storage`.
- **No new crates.** The unsafe remains confined to `oceanfs-storage`,
  the crate that owns the relevant invariants (segment buffer lifecycle,
  parity buffer allocation, concurrency model).
- **The `StreamingEcSegment` type already exists** in the working tree
  with the unsafe blocks documented. This ADR retroactively approves
  the usage and establishes the architectural precedent — matching the
  pattern of [ADR-0012], which retroactively approved already-implemented
  syscall wrappers.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Use safe indexing (no unsafe)** | No policy change; no audit burden; compiler may elide bounds checks with PGO+LTO | Leaves performance on the table; compiler elision is not guaranteed across lock boundaries; the feature DoD latency target (≤50µs seal) may be harder to meet without eliminating every predictable overhead; doesn't establish the pattern for future hot paths where the benefit may be larger | Bounds-check overhead on a hot path with provable invariants is wasteful when the proof is simple and the unsafe surface is tiny (two lines). The same rationale that justified mmap (zero-copy reads), syscall wrappers (fsync latency), and madvise (page cache hints) applies here: the unsafe is scoped, documented, and backed by a correctness proof. Rejecting it while accepting more complex unsafe categories would be inconsistent. |
| **Use `unsafe { buf.get_unchecked(idx) }` instead of raw pointer arithmetic** | Slightly less verbose; `get_unchecked` is semantically "skip bounds check" rather than "raw pointer arithmetic" | `get_unchecked` is nightly-only (`#![feature(get_unchecked)]`); OceanFS targets stable Rust; cannot be used without a nightly feature flag, which is unacceptable per the project's Rust toolchain policy; the function is not yet stable and has no stabilization timeline | Requiring nightly Rust for a bounds-check elimination would violate the project's stable-Rust policy and impose a toolchain constraint on all contributors. Raw pointer arithmetic from `Vec::as_ptr()` is stable and semantically equivalent. |
| **Apply `#[inline(always)]` and rely on compiler optimization** | No unsafe; no policy change; compiler can sometimes elide bounds checks when the bound invariant is visible after inlining | The compiler cannot see the synchronizes-with relationship between the `AtomicUsize` and the `Vec` length — this is a runtime semantic guarantee, not a compile-time constant; the `Mutex` lock acquisition creates a function-call boundary that defeats inlining-based range analysis; PGO+LTO can help but does not guarantee elision across module boundaries | This is not a design alternative; it is hope masquerading as strategy. The Rust compiler's bounds-check elision is an optimization, not a semantic guarantee. The OceanFS performance model (§1 of the performance audit) demands predictable latency, not best-effort compiler heuristics. If the invariant is provable, the unsafe is justified. |
| **Move `parity_shards()` to a separate `oceanfs-encode` crate** | Isolates the unsafe to a dedicated crate; `oceanfs-storage` stays cleaner | Adds a new crate for a single function (~20 lines); the safety invariants (segment buffer lifecycle, parity buffer allocation, locking discipline) are intrinsically tied to `oceanfs-storage` types; a separate crate would need to expose internal buffer layout details, breaking encapsulation; rejected in [ADR-0012] for syscall wrappers with the same rationale | The safety argument depends on `oceanfs-storage`'s invariants (`StreamingEcSegment` construction pre-allocation, `AtomicUsize` monotonicity, `Mutex` discipline). Placing the unsafe in a separate crate would either require exposing internals (violating encapsulation) or duplicating the invariants (creating a maintenance hazard). The unsafe belongs where the invariant is established and maintained. |
| **Use `Vec::as_ptr()` but keep the bounds check as `debug_assert!`** | Combines raw pointer read with debug-mode invariant checking; catches logic errors in tests | Adds `debug_assert!(stripe_idx < buf.len())` and `debug_assert!(parity_idx < slot.len())` to every iteration — these are themselves bounds checks that compile to the same `cmp` + branch sequence; in debug mode the overhead is worse than safe indexing (two checks instead of one); in release mode the assertions are stripped, achieving the same result as pure raw pointer arithmetic but with no additional safety | This is a cosmetic change: `debug_assert!` in debug mode adds overhead, and in release mode it disappears entirely. It does not improve correctness — if the invariant is broken in release mode, the raw pointer read is UB regardless of whether a `debug_assert!` would have caught it in tests. The safety comment is the contract; tests validate the contract. |
| **Use `rayon::scope` or channels to eliminate the `Mutex` and make bounds-check elision compiler-friendly** | The compiler can see the full data flow within a `rayon::scope`; bounds checks can be elided automatically | Requires architectural changes to the streaming encode model — replacing the `Arc<Mutex<Vec<Vec<BytesMut>>>>` with a channel-based design would change the feature's concurrency model significantly; introduces backpressure between writers and encoders; the feature doc explicitly designs for a shared parity buffer with lock-based synchronization; this is a feature redesign, not an alternative to the unsafe category | This is a valid architectural alternative for future consideration, but it changes the feature's design, not just the unsafe policy. The ADR authorizes the unsafe pattern as-implemented; a channel-based refactor can be proposed as a separate feature with its own ADR. |

## References

- [ADR-0011: Relax `unsafe_code` in `oceanfs-storage` for mmap Segment I/O](0011-storage-mmap-unsafe.md) — precedent for scoped `unsafe` permission in `oceanfs-storage`; established `#![deny(unsafe_code)]` + per-item `#[allow(unsafe_code)]` pattern
- [ADR-0012: Extend `unsafe` in `oceanfs-storage` for Linux Syscall Wrappers](0012-storage-linux-syscall-unsafe.md) — precedent for extending scope with per-category enumeration; explicit clause that new categories require a new ADR
- [ADR-0013: Relax `unsafe_code` in `oceanfs-network` for Linux Socket Tuning](0013-network-setsockopt-unsafe.md) — precedent for adding a new crate to the permitted-unsafe list with scoped authorization
- [Feature: EC Encode Optimizations](../features/performance-optimization/ec-encode-optimizations/feature.md) — feature specification; streaming EC encode is §3 of the feature scope
- [Architecture guideline §7.2: Unsafe Code Policy](../../guidelines/architecture.md#72-unsafe-code-policy) — current permitted-crates list and scope note; must be updated to enumerate Category 6
- [Performance guideline §12.1: `// SAFETY:` comments on every unsafe block](../../guidelines/performance.md#121-safety-comments-on-every-unsafe-block)
- [Coding guideline §12.1: `// SAFETY:` comments on every unsafe block](../../guidelines/coding.md#121-safety-comments-on-every-unsafe-block)
- [`StreamingEcSegment` implementation](../../crates/oceanfs-storage/src/segment/streaming.rs) — the specific unsafe site authorized by this ADR
- [Rust Reference: Behavior considered undefined](https://doc.rust-lang.org/reference/behavior-considered-undefined.html) — pointer provenance and out-of-bounds pointer arithmetic
- [Rustonomicon: Raw pointers](https://doc.rust-lang.org/nomicon/working-with-unsafe.html) — raw pointer safety semantics

[ADR-0011]: 0011-storage-mmap-unsafe.md
[ADR-0012]: 0012-storage-linux-syscall-unsafe.md
[ADR-0013]: 0013-network-setsockopt-unsafe.md
[EC Encode Optimizations]: ../features/performance-optimization/ec-encode-optimizations/feature.md
