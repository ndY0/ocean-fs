---
feature: "Membership Plane: Proto + Config + Pool"
epic: "membership-plane"
status: implemented
priority: high
owner: ""
dependencies: []
adr: [0028]
perf: [1.3, 4.1, 4.5, 7.1]
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Proto + Config + Pool

## Summary

The wire and configuration foundation for ADR-0028: extend the gossip/
membership protos with the fields the full protocol needs (per-entry
`version` + `origin`, version-vector pull requests, ack-carried deltas, the
`Probe` RPC), add `NodeConfig::membership_listen_addr`, and build the
membership plane's dedicated `ConnectionPool` with probe-appropriate
timeouts. No behavior changes — the new fields are ignored by the current
merge until the later features use them.

## Scope

### In Scope

- `proto/oceanfs/gossip.proto`:
  - `MembershipEntry` (in `membership.proto`) gains `uint64 version`,
    `string origin`, keeps `last_seen`.
  - `GossipAck` gains `repeated MembershipEntry delta` and
    `map<string, uint64> version_vector` (the push response carries the
    peer's pull — D4).
  - `GossipPullRequest.last_known_version` replaced by
    `map<string, uint64> version_vector`.
  - `GossipMessage.ring_version` and `hlc` removed (dead fields).
  - New service `ProbeRpc` with `rpc Probe(ProbeRequest) returns
    (ProbeResponse)` (spec §12.3; `ProbeRequest`/`ProbeResponse` already
    exist in `membership.proto`).
- `oceanfs-core` `NodeConfig`: `membership_listen_addr` (default
  `0.0.0.0:9002`), config round-trip tests.
- New `oceanfs-membership::plane` module: the membership-plane
  `ConnectionPool` (per-peer size 2, connect timeout derived from
  `ping_timeout_ms`) + the announced-address derivation helper
  (`0.0.0.0 → advertise IP` substitution shared with the gRPC address).
- Generated code regeneration (`oceanfs-network`/`oceanfs-membership`
  proto modules).

### Out of Scope

- The dedicated server/listener (f2).
- Detector probe logic (f3).
- Merge-rule changes (f4).
- Dissemination changes (f5).

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/gossip.proto`, `membership.proto` | Fields above |
| `oceanfs-network` | Generated gossip/probe client+server traits |
| `oceanfs-core` | `NodeConfig::membership_listen_addr`; `GossipConfig` unchanged |
| `oceanfs-membership` | New `plane` module (pool + address derivation) |

## Interface (Public API)

- `NodeConfig::membership_listen_addr: String` — bind address of the
  membership plane (default `0.0.0.0:9002`).
- `oceanfs_membership::plane::membership_pool(ping_timeout_ms: u64, tls_cert_path: Option<PathBuf>) -> Arc<ConnectionPool>` —
  the membership plane's dedicated pool (per-peer 2, probe-derived
  timeouts).
- `oceanfs_membership::plane::membership_address(listen: &str, advertise_ip: Option<&str>) -> SocketAddr` —
  the address announced to peers (D1).

## Data Flow

```
config → membership_listen_addr → plane pool + announced address
proto  → version/origin/vector fields → carried but unused until f4/f5
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in all affected crates
- [x] **Tests:** config round-trip (listen addr default + explicit);
      address-derivation unit tests (0.0.0.0 substitution, explicit IP);
      pool construction with probe timeouts
- [x] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes
- [x] **ADR:** ADR-0028 D1 (config + pool), D2 (Probe RPC on the wire),
      D4 (proto fields) satisfied
- [x] **Perf:** 1.3 (pre-sized vectors for deltas), 4.1 (pool per peer),
      4.5 (probe-derived timeouts), 7.1 (no lock held across pool ops)
- [x] **Integration:** a generated-code smoke test round-trips a
      `MembershipEntry{version, origin}` and a `Probe` call over the
      existing test server

## Deviations (accepted)

- **Probe transport: timeout-bounded pooled channel, not a fresh
  channel.** ADR-0028 D1 specified that probes use "a fresh channel with
  a hard per-call deadline rather than waiting on the pool semaphore".
  The implementation instead acquires a channel from the membership pool
  under a hard deadline — `make_client(pool, addr, ping_timeout_ms)`
  (`failure_detector/ping.rs:259`) — so a probe never waits unbounded,
  and the hard `ping_timeout_ms` bound D1 was designed to guarantee is
  preserved. The pool shape (per-peer 2, connect timeout derived from
  `ping_timeout_ms`) is unchanged; only the per-probe acquisition is
  bounded rather than creating a fresh channel per probe. Same deviation
  recorded in f2/f3.
