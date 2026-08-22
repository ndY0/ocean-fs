---
feature: "Storage Pools: NodeManifest Gossip Attribute"
epic: "disk-resilience"
status: done
priority: high
owner: ""
dependencies: ["pool-runtime"]
adr: [0029]
perf: [1.3, 2.4, 4.1]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: NodeManifest Gossip Attribute

## Summary

ADR-0029 §D2's wire-level presence: each node's membership state carries a
compact, versioned `NodeManifest` (one `PoolManifest` per pool) so every
peer knows the node's pools, their roles, status, free capacity, and
weights. Built from the `PoolRegistry` at node start and on every pool
change, attached to the node's own membership entry, and propagated by the
existing gossip plane (ADR-0028) — no new channels. Peers consume it in f7;
Phase B's health monitor drives the status fields.

## Scope

### In Scope

- Wire (proto):
  - `membership.proto`/`gossip.proto`: new message
    `message NodeManifest { uint64 incarnation = 1; repeated PoolManifest
    pools = 2; }` and `message PoolManifest { uint32 id = 1; string role =
    2; string status = 3; bool write_degraded = 4; uint64 capacity_free_bytes
    = 5; uint32 weight = 6; }` (role/status as strings for forward
    compatibility; f2's enum values encode/decode here).
  - `MembershipEntry` gains `optional NodeManifest manifest = N` (schema
    addition — older nodes absent-field-safe; gossip merge keeps it as an
    attributed field, no authority-class change).
- `oceanfs-membership`:
  - `NodeManifest::from_pools(incarnation, &[Arc<StoragePool>])` — built by
    the node, carried opaquely by membership (the membership crate holds the
    manifest as an attached field, not as merge logic);
  - **Version semantics (pinned):** a manifest change calls
    `set_self_manifest`, which increments the node's own **entry version**
    (the per-node version-vector counter from ADR-0028) but **NOT the
    incarnation** — a pool change is not a restart. Peers see a version
    bump and re-apply the entry; the authority-class merge is untouched
    (the manifest is carried along, never interpreted);
  - propagation uses the existing push-pull deltas unchanged — a bumped
    entry version is all the dissemination layer needs to forward the new
    manifest.
- `oceanfs-node`:
  - build the manifest at boot from the `PoolRegistry` (after f2) with the
    announce incarnation;
  - `register_membership_manifest(manifest)` on every pool set change
    (Phase A: boot only — f8 adds runtime changes).
- Tests:
  - unit (membership): manifest field attaches to an entry, rides a delta,
    survives push-pull round-trip; absent manifest is merge-neutral;
  - unit (node): manifest built from a 4-pool registry has 4
    PoolManifests with correct role/status/weight/free;
  - integration: 3-node local cluster — each node's manifest visible on
    every peer's membership view within gossip convergence.

### Out of Scope

- Peer-side consumption / caching (f7).
- Status transitions (Degraded/Dead) and `write_degraded` — Phase B; the
  wire fields exist and encode f2's `Healthy` constant.
- Loss announcements / affected-range sets — Phase B (ADR-0029 §D4).

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/membership.proto`, `gossip.proto` | NodeManifest/PoolManifest messages; entry field |
| `oceanfs-membership` | manifest attach + propagation (opaque field) |
| `oceanfs-node` | manifest builder + registration at boot |
| `oceanfs-network` | regenerated stubs |

## Interface (Public API)

- `oceanfs_membership::manifest::{NodeManifest, PoolManifest}` — wire types
  (encode/decode helpers over the proto).
- `Membership::set_self_manifest(manifest: NodeManifest)` — replaces the
  node's own manifest attribute **and bumps the self entry version**
  (incarnation unchanged).
- `Membership::manifest_of(node_id) -> Option<NodeManifest>` — read access
  for f7's cache.

## Data Flow

```
PoolRegistry ──▶ NodeManifest::from_pools(incarnation) ──▶ Membership::set_self_manifest
                                                              └─ entry version bump
peer ──▶ gossip push/pull delta ──▶ manifest attached to entry
   └─ Membership::manifest_of(node_id) ──▶ f7 routing cache
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-membership`,
      `oceanfs-node`, `oceanfs-network` (verified: builds clean, incl.
      `oceanfs-core` for the regenerated proto stubs)
- [x] **Tests:** unit (attach, round-trip, merge-neutral) + node build +
      3-node convergence of manifests (verified: 114 membership — 98 lib +
      16 integration — + 121 node + 227 core + 17 network tests green;
      server's two pre-existing failures are unrelated to this feature)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean (verified:
      `RUSTDOCFLAGS="-D warnings" cargo doc` clean on membership, node,
      network, core)
- [x] **ADR:** ADR-0029 §D2 (schema'd versioned manifest, O(pools) cost)
      satisfied (verified: proto schema, opaque attach, version-not-
      incarnation bump, preserve-on-None per D5)
- [x] **Perf:** 1.3 (pre-sized manifest vec), 2.4 (manifest built once per
      change, shared via Arc), 4.1 (rides the existing membership pool —
      no new channels) — all verified in code
- [x] **Integration:** the 3-node local cluster test asserts each peer's
      view contains all 3 manifests with matching pool counts (verified:
      `manifests_converge_on_all_three_peers` passes, asserts 3-node view
      + per-peer manifest equality + pool counts)

## Deviations (accepted)

- **Manifest is an opaque attached field, not merge-rule input.** The
  authority-class merge (ADR-0028 §D3) does not interpret manifest contents
  — the manifest follows the entry's version, and a peer's cached copy is
  replaced wholesale on version bump. Phase B's loss announcements may
  change this, but the decision keeps the authority table untouched in
  Phase A.
- **`NodeManifest::from_pools(incarnation, &[PoolManifest])`, not
  `&[Arc<StoragePool>]`.** The doc's pinned signature would force a
  membership → storage dependency; architecture §1.2 keeps membership on
  core/network/routing only. The `StoragePool → PoolManifest` mapping
  lives in the composition root (`oceanfs_node::pool_manifest::build_node_manifest`),
  which passes wire values into membership (architecture §4.1).
- **Preserve-on-None (merge-neutral absence).** An incoming entry without
  a manifest (older peer, failure-detector path) never erases the cached
  copy — this is the ADR-0029 D5 "stale-but-present beats absent" rule
  applied at the merge, and it makes pre-manifest nodes merge-neutral.
