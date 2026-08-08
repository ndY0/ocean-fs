# ADR-0013: Relax `unsafe_code` in `oceanfs-network` for Linux Socket Tuning

**Status:** Proposed
**Date:** 2026-08-08
**Deciders:** architecture team

---

## Context

The [Network Socket Tunings] feature requires applying three Linux
`socketopt`-level optimizations to gRPC sockets in `oceanfs-network`:

1. **`TCP_QUICKACK`** — disables delayed ACKs, saving up to 40ms per RPC
   round-trip for independent request-response patterns (quorum writes,
   SWIM pings, shard fetches).
2. **`SO_REUSEPORT`** — binds multiple sockets to the same port, using
   kernel 4-tuple-hash distribution to eliminate accept-queue contention
   on multi-core machines.
3. **`SO_BUSY_POLL`** — enables low-latency busy-wait polling (default
   50µs) instead of interrupt-driven wakeups, eliminating ~5-10µs
   wakeup latency for short RPCs.

Options 1 (`TCP_QUICKACK`) and 2 (`SO_REUSEPORT`) are available as
**safe** wrappers via the `socket2` crate (v0.5.x). Option 3
(`SO_BUSY_POLL`) is **not** exposed by `socket2` and requires a raw
`libc::setsockopt` call, which is `unsafe`.

### The Crate Placement Problem

The feature's socket-option functions belong in `oceanfs-network` — the
crate responsible for `ConnectionPool`, `RpcClient`, and all gRPC socket
lifecycle per the [architecture guideline §1.2]. Placing them in
`oceanfs-storage` (which already permits `unsafe` under [ADR-0011] and
[ADR-0012]) would create a circular dependency:

```
network → storage → membership → network
```

`oceanfs-network` currently has `#![forbid(unsafe_code)]`, which is
correct under the [architecture guideline §7.2]. The guideline lists four
crates permitted to use `unsafe`: `oceanfs-accel`, `oceanfs-hash`,
`oceanfs-ec`, and `oceanfs-storage`. All other crates are
`#![forbid(unsafe_code)]`.

### What the Unsafe Block Does

```rust
// SAFETY: `fd` is a valid socket file descriptor provided by the caller
// (obtained from `socket2::Socket` or `std::net::TcpStream`).
// `SO_BUSY_POLL` is an advisory hint — the kernel may ignore it.
// It cannot cause undefined behavior: the syscall either succeeds (sets
// the poll timeout) or returns an error (EINVAL for invalid fd, ENOPROTOOPT
// for unsupported kernels). No shared mutable state, no pointer aliasing,
// no lifetime violations, no FFI into complex libraries.
#[allow(unsafe_code)]
let ret = unsafe {
    libc::setsockopt(
        fd,
        libc::SOL_SOCKET,
        libc::SO_BUSY_POLL,
        &val as *const _ as *const libc::c_void,
        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
    )
};
```

This is a well-known, trivially safe syscall wrapper — a single
`setsockopt` with an advisory hint flag. It is **simpler and safer**
than the `memmap2::Mmap` operations already permitted in
`oceanfs-storage` under [ADR-0011]:

| Property | `SO_BUSY_POLL` | `memmap2::Mmap` |
|---|---|---|
| Memory aliasing | None | `&[u8]` reference could alias mutable bytes |
| Invariant required | Valid `fd` (caller-guaranteed) | File immutability for mapping lifetime (segment sealing invariant) |
| Failure mode | Returns `EINVAL`/`ENOPROTOOPT` — safe error | Could produce `&[u8]` to mutated bytes if invariant broken |
| Kernel action | Advisory hint; may be ignored | Changes effective contents of a Rust reference |
| Line count | ~6 lines of unsafe | ~1 line of unsafe but complex invariant reasoning |

If `oceanfs-storage`'s use of `unsafe` (which involves memory aliasing
and file immutability invariants) is architecturally acceptable under
[ADR-0011], then `oceanfs-network`'s use of `unsafe` for a trivially
safe advisory syscall should be acceptable under a narrower scope.

## Decision

**Relax `#![forbid(unsafe_code)]` to `#![deny(unsafe_code)]` in
`oceanfs-network`**, and amend architecture guideline §7.2 to add
`oceanfs-network` as a fifth crate where targeted `unsafe` is permitted.

### Scope

This ADR permits `unsafe` in `oceanfs-network` for the following
purpose **only**:

- **`libc::setsockopt` for Linux socket tuning** — specifically
  `SO_BUSY_POLL` on gRPC server listening sockets and client sockets.
  The scope is limited to `libc::setsockopt` calls, not general FFI,
  raw pointer manipulation, `transmute`, or inline assembly.

It does **not** authorize:

- Any `unsafe` not directly related to `libc::setsockopt` for socket
  tuning.
- Any FFI bindings to libraries other than `libc` for `setsockopt`.
- Any other syscall (e.g., `sendmsg`, `recvmsg`, `ioctl` on sockets).
- `unsafe` in any other `oceanfs-*` crate (they remain
  `#![forbid(unsafe_code)]` unless authorized by their own ADR).

If a future requirement needs additional `unsafe` in `oceanfs-network`,
a new ADR is required — following the precedent set by [ADR-0011] →
[ADR-0012].

### Concrete Changes

1. **In `oceanfs-network/src/lib.rs`:** Change
   `#![forbid(unsafe_code)]` to `#![deny(unsafe_code)]`.

2. **In `oceanfs-network/src/socket_opts.rs`:** Add
   `#[allow(unsafe_code)]` on the `set_busy_poll()` function with a
   `// SAFETY:` comment documenting the `setsockopt` invariants.

3. **Amend architecture guideline §7.2** to add `oceanfs-network` to
   the permitted-crates list with the scope note: "Linux `setsockopt`
   wrappers for gRPC socket tuning (`SO_BUSY_POLL`); unsafe blocks must
   be preceded by `// SAFETY:` comments."

4. **CI enforcement:** The check that verifies each crate's `lib.rs`
   lint attribute must accept `deny(unsafe_code)` for
   `oceanfs-network` in addition to the four existing permitted crates.

### Enforcement (Unchanged from Existing Policy)

1. **Crate-level:** `#![deny(unsafe_code)]` — all `unsafe` blocks are
   errors by default.
2. **Per-item override:** Each `unsafe` block must be preceded by
   `#[allow(unsafe_code)]`, making all unsafe sites auditable via
   `grep -rn "allow(unsafe_code)" crates/oceanfs-network/src/`.
3. **Safety comment:** Each `unsafe` block must carry a
   `// SAFETY:` comment (enforced by `clippy::undocumented_unsafe_blocks`
   at the crate level per [coding guideline §12.1]).
4. **Platform gating:** The `unsafe` block is `#[cfg(target_os = "linux")]`
   -gated with a no-op fallback on non-Linux platforms. Non-Linux builds
   are completely unaffected — zero cost, zero risk.

## Consequences

### Positive

- **Low-latency polling unblocked.** `SO_BUSY_POLL` eliminates kernel
  interrupt wakeup latency (~5-10µs) for short RPCs — critical for the
  inter-node quorum write ack pattern where median payloads are ~100
  bytes. Combined with `TCP_QUICKACK`, expected median latency reduction
  of ≥30% for small RPCs under 1KB (feature DoD).
- **Trivial safety invariant.** Unlike `memmap2::Mmap` (memory aliasing)
  or `madvise` (page state manipulation), `SO_BUSY_POLL` is purely
  advisory. A valid `fd` is the only invariant, which is trivially
  guaranteed by the caller (a `socket2::Socket` or `TcpStream` that is
  alive at the call site). The kernel may silently ignore the hint or
  return a safe error (`EINVAL`/`ENOPROTOOPT`).
- **No new crate.** The unsafe surface is confined to `oceanfs-network`
  — the crate that owns the gRPC socket lifecycle. Creating a separate
  crate (e.g., `oceanfs-syscall`) was already rejected in [ADR-0012]
  for the same reasons: the safety invariant depends on the calling
  crate's types and invariants, and a generic wrapper crate cannot
  enforce them at the type level.
- **Auditability preserved.** `deny(unsafe_code)` + per-item
  `#[allow(unsafe_code)]` retains the "audit-at-a-glance" property. A
  grep for `allow(unsafe_code)` in `oceanfs-network/src/` shows every
  site — currently one function.

### Negative

- **Unsafe surface expands by one crate.** The list of crates with
  permitted `unsafe` grows from 4 to 5. This increases the review
  burden: every PR touching `oceanfs-network` must be scrutinized for
  unauthorized `unsafe` blocks.
- **Risk of scope creep.** Developers adding unrelated features to
  `oceanfs-network` may be tempted to add `#[allow(unsafe_code)]` for
  purposes not covered by this ADR (e.g., `sendmsg` scatter-gather,
  `ioctl` on tun devices). Mitigation: code review must reject any
  `unsafe` block that is not a `libc::setsockopt` call. The ADR
  explicitly scopes the permission.
- **The `deny` + `allow` pattern is less absolute than `forbid`.**
  `forbid` is a hard compiler-level guarantee; `deny` is a lint-level
  default that can be overridden. An unauthorized
  `#[allow(unsafe_code)]` could slip through review. Mitigation: CI lint
  audit that diffs the set of `#[allow(unsafe_code)]` sites in
  `oceanfs-network` and flags any not in `socket_opts.rs`.

### Neutral

- **Architecture guideline §7.2 must be updated.** One-entry addition to
  the permitted-crates list. Straightforward but requires a docs update.
- **CI enforcement script must be updated.** The check that verifies
  each crate's `lib.rs` attribute must accept `deny(unsafe_code)` for
  `oceanfs-network` in addition to the four existing permitted crates.
- **`oceanfs-network` gains a dependency on `libc`.** This is a
  well-established, audited crate already used by `oceanfs-storage`
  under [ADR-0012]. No novel supply-chain risk. The `libc` dependency
  is `#[cfg(target_os = "linux")]`-gated to avoid pulling it on
  non-Linux targets (though `socket2` already depends on `libc` on all
  platforms, so this is a no-op in practice).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Place socket opts in `oceanfs-storage` (already permits unsafe)** | No new crate with `unsafe`; no policy change needed | Creates circular dependency: `network → storage → membership → network`; violates architecture guideline §1.3 (DAG constraint); socket options are not storage I/O — they are network configuration, fundamentally outside `oceanfs-storage`'s responsibility (§1.2); the `SO_BUSY_POLL` call would need to live in a crate that knows nothing about gRPC connection lifecycle | Circular dependencies are a CI-failing hard constraint (§1.3). Even if the cycle were somehow broken (e.g., by introducing a trait), socket tuning is not storage's concern. Putting network configuration in a storage crate is an architectural violation worse than a minor policy amendment. |
| **Create a new `oceanfs-syscall` crate for all unsafe syscall wrappers** | Isolates all unsafe FFI to one crate; `oceanfs-network` stays `#![forbid(unsafe_code)]` | Adds a 15th crate to the workspace; the safety invariant (valid socket `fd`) is intrinsically tied to `oceanfs-network`'s socket lifecycle — a generic syscall crate cannot enforce this at the type level; a one-function crate (`set_busy_poll`) provides no architectural benefit; this alternative was already rejected in [ADR-0012] for storage syscalls with the same rationale | The safety argument depends on the caller guaranteeing a valid, live socket `fd`. A generic syscall crate would accept a raw `RawFd` with no lifetime or ownership tracking — strictly less safe than a `oceanfs-network`-internal function that can witness the `Socket`/`TcpStream` alive at the call site. Keeping the unsafe where the invariant lives is the correct granularity. |
| **Skip `SO_BUSY_POLL`; implement only safe socket options** | No policy change; no unsafe at all; simpler implementation | Violates the feature spec's DoD, which explicitly requires `SO_BUSY_POLL` as a deliverable; loses the low-latency polling benefit (5-10µs wakeup elimination) that is the primary motivation for socket tuning on the gRPC data path; the feature's expected tail-latency improvement (p99 reduction ≥20%) depends substantially on `SO_BUSY_POLL` | The [Network Socket Tunings] feature is a medium-priority deliverable in the performance-optimization epic. Removing the primary latency-reducing optimization from scope would require a separate ADR to amend the feature spec, which is unjustified given the trivial safety of the `setsockopt` call. |
| **Use `socket2::SockRef::setsockopt` with a raw option value** | Keeps the unsafe call in a dependency, not in OceanFS source | `socket2::SockRef::setsockopt` for unknown options is still marked `unsafe` — it delegates to `libc::setsockopt` with the same invariants; shifting the `unsafe` keyword to a dependency doesn't eliminate the safety reasoning burden — someone must still audit that `SO_BUSY_POLL` is used correctly; adds an indirection layer for a single syscall with no additional safety | The `unsafe` is semantically identical whether it appears in `oceanfs-network` or behind `socket2`'s API. Placing it directly in `oceanfs-network` makes it explicit and auditable — a grep for `allow(unsafe_code)` finds it immediately, rather than hiding it behind a dependency's unsafe block. Additionally, `socket2`'s raw option API requires manually constructing the option value as `&[u8]`, which is less type-safe than the `&val as *const _` cast used in the direct `libc` call. |

## References

- [ADR-0011: Relax `unsafe_code` in `oceanfs-storage` for mmap Segment I/O](0011-storage-mmap-unsafe.md) — precedent for scoped `unsafe` permission in a specific crate
- [ADR-0012: Extend `unsafe` in `oceanfs-storage` for Linux Syscall Wrappers](0012-storage-linux-syscall-unsafe.md) — precedent for syscall-specific unsafe scope with per-category enumeration; explicit clause that other crates need their own ADR
- [Feature: Network Socket Tunings](../features/performance-optimization/network-socket-tunings/feature.md) — feature specification for all three socket options
- [Architecture guideline §1.2: Crate Responsibilities](../../guidelines/architecture.md#12-crate-responsibilities) — `oceanfs-network` owns `ConnectionPool`, `RpcClient`, `RpcConfig`
- [Architecture guideline §1.3: Dependency Enforcement](../../guidelines/architecture.md#13-dependency-enforcement) — DAG constraint; circular dependencies forbidden
- [Architecture guideline §7.2: Unsafe Code Policy](../../guidelines/architecture.md#72-unsafe-code-policy) — current permitted-crates list (4 crates)
- [Performance guideline §10.6: Conditional platform-specific code paths](../../guidelines/performance.md#106-conditional-platform-specific-code-paths) — `#[cfg(target_os = "linux")]` gating
- [Coding guideline §12.1: `// SAFETY:` comments on every unsafe block](../../guidelines/coding.md#121-safety-comments-on-every-unsafe-block)
- `setsockopt(2)` — Linux man page (`SO_BUSY_POLL`, kernel 3.11+)

[ADR-0011]: 0011-storage-mmap-unsafe.md
[ADR-0012]: 0012-storage-linux-syscall-unsafe.md
[Network Socket Tunings]: ../features/performance-optimization/network-socket-tunings/feature.md
