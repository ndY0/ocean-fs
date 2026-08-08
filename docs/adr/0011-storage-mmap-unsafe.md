# ADR-0011: Relax `unsafe_code` in `oceanfs-storage` for mmap Segment I/O

**Status:** Proposed
**Date:** 2026-08-08
**Deciders:** architecture team

---

## Context

The Platform I/O Optimizations feature requires zero-copy segment reads
via memory-mapped I/O (performance guideline §3.3:
`mmap` for hot segment reads). The implementation uses
`memmap2::Mmap::map()`, which returns a `&[u8]` reference backed by an
OS-managed memory region.

`memmap2::Mmap::map()` (v0.9.x) is marked `unsafe`. The Rust standard
library's `unsafe` contract for this function states: the file backing
the mmap region may be modified concurrently, either by another process
or by writes to the same file descriptor — and Rust cannot guarantee
that the bytes underlying the returned `&[u8]` reference are truly
immutable. The caller must uphold the invariant that the backing file
is not mutated for the lifetime of the mapping.

In OceanFS, **this invariant holds**:

- Segment shard files are **immutable after sealing**.
  `SegmentSealer::seal_from_data()` is the sole code path that writes
  segment data to disk. Once a segment is sealed, no code path opens a
  writable handle to the segment file.
- The segment read path opens files read-only. There is no concurrent
  writer — no other process or thread can mutate the bytes.
- The OS cannot modify the file bytes for the mapping's lifetime because
  no write file descriptor exists and the filesystem grants no other
  process write access.

Under these invariants, calling `Mmap::map()` on a sealed segment file
is sound: the `&[u8]` reference truly points to immutable bytes.

Currently, `oceanfs-storage/src/lib.rs` declares
`#![forbid(unsafe_code)]` (line 16). This makes it *impossible* to call
`Mmap::map()` — even in a carefully-scoped function with a `// SAFETY:`
justification. The `forbid` lint cannot be overridden by
`#[allow(unsafe_code)]` on individual items; it is absolute.

Architecture guideline §7.2 currently restricts `unsafe` to three
crates: `oceanfs-accel` (GPU FFI, SIMD), `oceanfs-hash` (BLAKE3
implementation), and `oceanfs-ec` (SIMD GF arithmetic). All other
crates must be `#![forbid(unsafe_code)]`.

## Decision

**Relax `#![forbid(unsafe_code)]` to `#![deny(unsafe_code)]` in
`oceanfs-storage`**, and amend architecture guideline §7.2 to add
`oceanfs-storage` to the list of crates where targeted `unsafe` is
permitted.

Concretely:

1. **In `oceanfs-storage/src/lib.rs`:** Change line 16 from
   `#![forbid(unsafe_code)]` to `#![deny(unsafe_code)]`.

2. **On specific functions that perform mmap:** Add
   `#[allow(unsafe_code)]` and a `// SAFETY:` comment documenting the
   segment-immutability invariant. The deny-by-default lint ensures that
   all other code in the crate cannot use `unsafe` — the
   `#[allow(unsafe_code)]` annotation is required on each item, making
   unsafe usage auditable at a glance.

   Example usage in `oceanfs-storage/src/io/mmap.rs`:

   ```rust
   /// Map a sealed segment file for zero-copy reads.
   ///
   /// # Safety
   ///
   /// The segment file must have been sealed. No writable handle to the
   /// file may exist for the lifetime of the returned `Mmap`. This
   /// function enforces these invariants at the type level by accepting
   /// only a sealed `SegmentHandle`.
   #[allow(unsafe_code)]
   pub(crate) fn map_segment(file: &File, handle: &SegmentHandle) -> Result<Mmap> {
       // SAFETY: The segment is sealed (guaranteed by SegmentHandle).
       // No writable file descriptor exists for this segment. The OS
       // cannot modify the bytes because no other process holds a write
       // handle. The &[u8] reference produced by Mmap::map() therefore
       // points to truly immutable bytes for the mapping's lifetime.
       let mmap = unsafe { Mmap::map(file)? };
       Ok(mmap)
   }
   ```

3. **Amend architecture guideline §7.2** to add `oceanfs-storage` to
   the permitted-crates list, with the scope note: "mmap segment I/O;
   unsafe blocks must be preceded by `// SAFETY:` comments documenting
   the segment-immutability invariant."

   Updated §7.2:

   > Unsafe code is permitted only in the following crates:
   > - `oceanfs-accel` (GPU FFI, SIMD intrinsics)
   > - `oceanfs-hash` (BLAKE3 implementation if not using the upstream crate)
   > - `oceanfs-ec` (SIMD-accelerated GF arithmetic)
   > - `oceanfs-storage` (memory-mapped segment I/O via `memmap2::Mmap`;
   >   unsafe blocks must document the segment-immutability invariant)

   The enforcement clause remains: CI checks each crate's `lib.rs` for
   `#![forbid(unsafe_code)]` (or `#![deny(unsafe_code)]` for the four
   permitted crates).

4. **Scope of permitted `unsafe` in `oceanfs-storage`:** This decision
   authorizes `unsafe` **only** for `memmap2::Mmap` operations
   (including `Mmap::map`, `MmapOptions`, and any related mapping
   operations). It does not authorize FFI, raw pointer manipulation,
   `transmute`, or any other form of `unsafe`. If a future requirement
   needs additional `unsafe` in `oceanfs-storage`, a new ADR is
   required.

## Consequences

### Positive

- **Zero-copy segment reads become possible.** The Platform I/O
  Optimizations feature can implement §3.3 (mmap), unblocking a
  significant read-path performance improvement (~2× faster random
  reads vs. `tokio::fs::read` for cached segments).
- **Auditability preserved.** `deny(unsafe_code)` + per-item
  `#[allow(unsafe_code)]` retains the "audit-at-a-glance" property: a
  grep for `allow(unsafe_code)` in `oceanfs-storage/src/` shows every
  site, and each must carry a `// SAFETY:` justification per guideline
  §12.1.
- **No new crate.** The unsafe surface is confined to a known,
  invariants-backed operation. Creating a separate crate (e.g.,
  `oceanfs-mmap-util`) would add a new node to the dependency graph
  with negligible benefit — the invariants are specific to
  `oceanfs-storage`'s segment lifecycle, not a reusable utility.
- **CI enforcement remains strong.** The existing CI check (verify
  `lib.rs` declares `forbid` or `deny` as appropriate) is trivially
  updated to accept `deny` for the fourth crate.

### Negative

- **Unsafe surface expands by one crate.** The list of crates with
  permitted `unsafe` grows from 3 to 4. This increases the review
  burden: every PR touching `oceanfs-storage` must be scrutinized for
  unauthorized `unsafe` blocks.
- **Risk of scope creep.** Developers adding unrelated features to
  `oceanfs-storage` may be tempted to add `#[allow(unsafe_code)]` for
  purposes not covered by this ADR. Mitigation: code review must reject
  any `unsafe` block that is not mmap-related. The ADR explicitly
  scopes the permission.
- **The `deny` + `allow` pattern is less absolute than `forbid`.**
  `forbid` is a hard compiler-level guarantee; `deny` is a
  lint-level default that can be overridden. In a large team, an
  unauthorized `#[allow(unsafe_code)]` could slip through. Mitigation:
  CI lint audit that diffs the set of `#[allow(unsafe_code)]` sites
  and flags any not in the `io/mmap.rs` module.

### Neutral

- **Architecture guideline §7.2 must be updated.** One-line addition to
  the permitted-crates list. Straightforward but requires a docs PR.
- **CI enforcement script must be updated.** The check that verifies
  each crate's `lib.rs` attribute must accept `deny(unsafe_code)` for
  `oceanfs-storage` in addition to the three existing permitted crates.
- **The clippy lint `clippy::undocumented_unsafe_blocks` is already
  denied** at the crate level (line 21 of `lib.rs`), so all `unsafe`
  blocks will require `// SAFETY:` comments — unchanged behavior.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Keep `forbid` + use a separate `oceanfs-io` crate for mmap** | No change to `oceanfs-storage`'s unsafe policy; mmap concerns isolated in a dedicated crate | Adds a 14th crate to the workspace; the unsafe invariant (segment immutability) is intrinsically tied to `oceanfs-storage`'s `SegmentHandle` type, making the crate boundary artificial; increases compilation graph size for no architectural benefit; all other I/O paths (`O_DIRECT`, io_uring, sendfile) are tightly coupled to segment lifecycle, so splitting one path into a separate crate would fragment the I/O module | The `oceanfs-io` crate would inevitably depend on `oceanfs-storage` types (or force those types into `core`), creating either a circular dependency or an abstraction inversion. The marginal benefit of a new crate does not justify the complexity. |
| **Keep `forbid` + implement segment reads with `tokio::fs::read` only (no mmap)** | No unsafe at all; simplest implementation; no policy change | Violates performance guideline §3.3 (`mmap` for hot segment reads); ~2× read throughput penalty for cached segments; doubles memory usage (kernel page cache + userspace buffer); the feature doc DoD explicitly requires mmap as a deliverable | Performance guideline §3.3 was adopted specifically because mmap eliminates the userspace copy on segment reads. The Platform I/O Optimizations feature is a high-priority deliverable; removing mmap from scope would require a separate ADR to amend §3.3, which is unjustified given the soundness of mmap under segment immutability. |
| **Remove the unsafe lint entirely (no `forbid` or `deny` in `oceanfs-storage`)** | Maximum flexibility; no per-item annotations needed | No guardrails; unsafe could proliferate across the crate without any compiler-enforced audit trail; violates the project's unsafe code policy (§7.2); inconsistent with every other crate in the workspace | The whole point of the unsafe policy is auditable, constrained unsafe. Removing the lint removes the audit trail. The `deny` + per-item `allow` pattern gives exactly the flexibility needed (mmap) without removing the guardrails. |
| **Use `unsafe` blocks without `#[allow(unsafe_code)]` (i.e., keep `forbid` and rely on Rust's `unsafe` keyword being sufficient documentation)** | No policy change needed | `forbid(unsafe_code)` prevents `unsafe` blocks entirely — the compiler rejects them, not just warns. This alternative is technically impossible. | This is not a design choice; it is a misunderstanding of what `forbid(unsafe_code)` does. `forbid` makes the lint an error at all scopes with no override. |
| **Vendor a safe wrapper around `mmap` that moves the unsafe into a dependency** | No `unsafe` in OceanFS source; the wrapper crate handles the unsafe | Shifts the problem, does not solve it: someone must audit the wrapper's invariants, and the invariants are the same segment-immutability argument; adds a dependency for a ~20-line function; wrapper cannot enforce OceanFS-specific invariants (segment sealing) at the type level — it would accept raw `&File`, making it strictly less safe than a `oceanfs-storage`-internal function that accepts `&SegmentHandle` | The safety argument depends on OceanFS's segment lifecycle, which only `oceanfs-storage` owns. A generic wrapper crate cannot know about segment sealing and therefore cannot document or enforce the invariant. Placing the unsafe in `oceanfs-storage` where the invariant lives is the correct granularity. |

## References

- [Performance guideline §3.3: `mmap` for hot segment reads](../../guidelines/performance.md#33-mmap-for-hot-segment-reads)
- [Architecture guideline §7.2: Unsafe Code Policy](../../guidelines/architecture.md#72-unsafe-code-policy)
- [Performance guideline §12.1: `// SAFETY:` comments on every unsafe block](../../guidelines/performance.md#121-safety-comments-on-every-unsafe-block)
- [Feature: Platform I/O Optimizations](../../docs/features/performance-optimization/platform-io-optimizations/feature.md)
- [ADR-0006: Hardware Acceleration Tier Model](0006-hardware-acceleration-tier-model.md) — precedent for scoped `unsafe` permission in a specific crate
- [memmap2 crate documentation](https://docs.rs/memmap2/0.9.x/memmap2/struct.Mmap.html#method.map) — `Mmap::map()` safety documentation
- [oceanfs-storage/src/lib.rs](../../crates/oceanfs-storage/src/lib.rs) — current `#![forbid(unsafe_code)]` declaration (line 16)
