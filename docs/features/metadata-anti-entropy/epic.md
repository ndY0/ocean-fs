---
epic: "metadata-anti-entropy"
status: proposed
priority: high
created: 2026-09-12
updated: 2026-09-12
---

# Metadata Anti-Entropy (Index-Plane Convergence) — Epic Plan

Epic: `metadata-anti-entropy`

Design (ratified by the user 2026-09-12):
[ADR-0038 — Metadata Convergence: Co-Durable Change Journal with Pull-Based
Catch-Up](../../adr/0038-metadata-change-journal.md). The ADR file's status
remains `proposed`; the user aligned on all of its open decisions on
2026-09-12, so the feature specs may proceed, and the status flip is an
upstream documentation edit outside this feature set. The superseded
[ADR-0037 rotating scan](../../adr/0037-metadata-anti-entropy-detection.md) is
**not** resurrected; [pr3-metadata-anti-entropy](pr3-metadata-anti-entropy.md)
is `superseded` and kept as the O3 historical record.

Constraint ADRs: [ADR-0034](../../adr/0034-bounded-metadata-accounting.md)
(bounded metadata accounting — the constraint),
[ADR-0023](../../adr/0023-metadata-store-native-replacement-path.md)
(native-store path — no RocksDB coupling),
[ADR-0027](../../adr/0027-hinted-handoff-ownership-model.md) (hint
delivery/ownership; §D5),
[ADR-0028](../../adr/0028-membership-plane-full-swim-gossip.md) (retained-Dead
topology, origin-attributed state),
[ADR-0029](../../adr/0029-storage-pools-disk-resilience.md) §D3/§D7 (pools;
metadata loss recovery),
[ADR-0030](../../adr/0030-re-replication-target-pull.md) (target-pull),
[ADR-0033](../../adr/0033-manifest-aware-peer-selection.md) (manifest-aware
peer selection),
[ADR-0017](../../adr/0017-durability-task-abstraction.md) (Tier-1, 2026-09-06
amendment). Segment-plane
[ADR-0015](../../adr/0015-anti-entropy-merkle-protocol.md) is untouched.

Spec mapping: `docs/spec.md` §15 Phase 7 (anti-entropy / production-grade
durability) and §7.4 — this epic is the **index-plane row** complement of the
spec's segment-plane Merkle exchange.

Design capture: [design-draft.md](design-draft.md) (deferred gap record; the
epic is its promotion path). Superseded spec:
[pr3-metadata-anti-entropy.md](pr3-metadata-anti-entropy.md) (banner points
here). Related: [fleet-degradation](../fleet-degradation/epic.md) (paused —
owns f0/f4), [f0 hints-durability-gate](../fleet-degradation/f0-hints-durability-gate.md),
[f4 pool-degradation-under-load](../fleet-degradation/f4-pool-degradation-under-load.md).

## Ratified design decisions (2026-09-12 — encoded, not reopened)

1. **J1 fail-closed.** A metadata write is rejected if its journal entry
   cannot be recorded; the entry is written in the same durability class as
   the row (no extra per-write fsync; group commit; a gap marker only for
   power-loss windows). Mirrors f0's admission honesty.
2. **Enable-time bootstrap required.** On first enable, a triggered,
   range-bounded, key-ordered transfer of owned ranges heals pre-enable
   divergence — f4 P4 Pass A deliberately creates it before Pass B enables
   sync. Lands with S3 (ownership edge).
3. **Bootstrap pacing.** Tier-1 paced for catch-up; node-gating only for the
   empty-store boot rebuild (today's g8 semantics) — never a recurring scan.
4. **ADR-0027 D5 clarification.** Foreign chunk refs are normal; there is
   **no local-segment-presence precondition anywhere** in the catch-up path.
   Recorded as a doc follow-up on ADR-0027 once ADR-0038 is accepted (doc-only
   ownership: ADR owner).
5. **O1 fingerprint CF** stays the documented, **unwired** fallback — adopted
   only if journal capture completeness proves intractable.
6. **Spot-check key provisioning** is deferred to S4.
7. **Journal location (pinned).** `<metadata_pool_root>/metadata-journal/` —
   same pool/device as the metadata store; segmented append-only files + a
   small manifest (epoch/generation, base seq, active segment); group-committed;
   trimmed by whole-segment deletion below the safe watermark; bounded
   (256 MiB / 7 d cap); counted against pool capacity; replaced with the pool
   (fresh epoch; peers invalidate stale watermarks → bootstrap). Explicitly
   **not** on the hints pool (would couple acks to hints health), **not** a
   RocksDB CF/WAL.

## Goal

Close the index plane's eventual-repair gap — once a row is missing and its
hint is gone, nothing re-derives it — **with zero recurring full-metadata
scans**. Performance is the center of the design: detection cost is ∝ changes
applied, never ∝ rows stored; recovery reads are triggered and each read is
range-bounded; complexity and failure modes are the accepted currency. The
four boundaries:

| Boundary | What it owns | Mechanism | Stage |
|---|---|---|---|
| **ack** (admission) | no acked write without durable debt | f0 hints gate; S2's J1 mirrors the discipline for the journal | f0 (done) / S2 |
| **change** (steady state) | detection ∝ changes | co-durable journal + durable per-peer watermarks + Tier-1 pull worker | S2 |
| **ownership** (completeness) | pre-enable / gap / new-owner / pool-loss divergence | triggered, range-bounded, key-ordered bootstrap; replaces g8's full-CF filtered scan | S3 |
| **user** (read edge) | local row absent while the node is an owner | owner-only read-miss point fetch, single-flighted, negatively cached | S3 |

The locked architecture remains **segment reconcile (bytes) + f0 ack gate +
metadata convergence (this epic)**. Read-path push repair and debt-replay
repair stay dropped. Every state the journal cannot cover has a triggered
bootstrap; no scheduled phase anywhere re-reads the keyspace.

## Staged features

| Stage | Feature | Status | Priority | Depends on | Scope |
|---|---|---|---|---|---|
| S1 | [ae1 — Hint-Debt Hardening & Segment Unrecoverable Signal](ae1-debt-hardening-loss-signal.md) | done (2026-09-12, review PASS) | high | — (independent; f5 is the escalation substrate) | Membership-driven hint retention (not blind TTL); TTL expiry escalates through the f5 D3 repair-intent sink; `hinted_handoff_pending_debt{target}` gauge; hint mirroring stays rejected. Segment plane: a repair with no live recorded holder becomes terminal after N sweeps — bounded dedupe marker + `oceanfs_repair_unrecoverable_total{reason}` + stop re-enqueue; a returning holder clears and resumes. No journal dependency. |
| S2 | [ae2 — Metadata Change Journal & Pull-Based Catch-Up](ae2-metadata-change-journal.md) | proposed (next; unblocked) | critical | ae1 (soft, ordering only — done 2026-09-12); f0 (complement) | The change edge: co-durable journal at the store mutation choke point (pinned location, format, capture points, epoch, trim), per-peer durable watermarks + exchange, Tier-1 `metadata_sync` worker, ownership filter, point fetch of current row state, HLC-LWW idempotent apply, J1 fail-closed, exact `[metadata_sync]` kill-switch, metrics, ADR-0034 accounting statement. Bootstrap/read-trigger/spot-check are S3/S4 follow-ups, not S2 DoD. |
| S3 | ae3 — Range Iterator + Bootstrap + Read-Trigger (slug indicative; spec not written) | not specified | high | S2 (epoch/watermarks/gap); f0 | Backend-neutral `visit_rows_range` on the metadata store; triggered range-bounded, key-ordered bootstrap with persisted cursors (new-owner / gap / pool-loss / **enable-time** triggers); g8 lister replacement with golden equivalence; owner-only read-miss point fetch with single-flight + short negative cache. **Acceptance gate: the mandatory N>RF matrix below + the f4 P3 re-run.** |
| S4 | ae4 — Spot-Check + Hardening (slug indicative; spec not written) | not specified | medium | S2/S3 | Bounded HMAC spot-check verifier (recent-touch reservoir, tag exchange, mismatch escalates to point fetch + apply) for probabilistic capture-gap detection; mixed-mesh/epoch/fault-injection hardening; spot-check key provisioning decided here (ratified deferral); O1 remains the unwired fallback. |

## Sequencing

**S1 quick wins → S2 core → S3 removes the last full scan → S4 polish.**
S1 [ae1](ae1-debt-hardening-loss-signal.md) landed 2026-09-12 with review
PASS. **S2 is next and unblocked** — the ae1 dependency was soft ordering
only; S2 carries the dominant risk (co-durability/capture/trim) and the
"minimum viable convergence"; S3 supplies completeness (bootstrap) and the
user edge, and is the point where the g8 full-CF scan disappears; S4 adds
probabilistic detection and hardening.

**f4 mapping (f4 is paused; fleet at 0 resources; resume only on the user's
go-ahead — PIPELINE §7):**

- **Pass A (AE off):** P4 unchanged; records the post-f0 residual cold-key
  divergence — the baseline.
- **Pass B (same fleet session; enable `metadata_sync` + restart):**
  validates **S2** on divergence created **after** enablement (ADR-0038 S2:
  the change edge does not heal the pre-enable residual). Assertions:
  mismatches detected, rows pulled/applied, RF coverage restored, the missed
  DELETE converges to the tombstone, per-cycle budgets respected, no bounded-
  discipline regression.
- **P3 re-run (metadata-pool loss):** validates **S3**'s windowed g8
  replacement end to end (objects + deletions rebuild). When S3 lands, its
  **enable-time bootstrap** heals the Pass A pre-enable residual on the same
  fixture, so S2's "pre-enable residual stays unhealed" behavior is closed
  without a second provisioning cycle.
- S3's partial-ownership behavior is **not** covered by f4 (N=3, RF=3); the
  N>RF matrix is its gate. S4 is validated locally (f4 has no silent-loss
  injection).

## Dependency graph

```
f0 hints-durability-gate   done 2026-09-11 ── ack boundary (complement)
S1 ae1 debt-hardening + unrecoverable signal   done 2026-09-12 (review PASS)
        │  (soft ordering only)
        ▼
S2 ae2 journal + sync (change edge)  ◄── ADR-0038 ratified 2026-09-12   (next; unblocked)
        │
        ▼
S3 ae3 bootstrap + range iterator + read-trigger (ownership/user edge)
        │
        ▼
S4 ae4 spot-check + hardening

f4 (paused): Pass A baseline ──► Pass B validates S2 ──► P3 re-run validates S3
```

## Mandatory N>RF test matrix (S3 acceptance gate)

f4 is N=3 with RF=3 full replication: it **cannot** validate any
partial-ownership behavior. These tests are the hard acceptance gate for S3
and require a fleet with N ≥ 4 (RF = 3) or an equivalent in-process
weighted-ring harness:

| # | Test | Assertion |
|---|---|---|
| 1 | New owner joins (N→N+1) | Owned ranges bootstrapped via triggered windowed reads; node serves reads for those keys; no scheduled full scan appears |
| 2 | Mutation filtering under partial ownership | Only co-owned keys are consumed/applied (`keys_skipped_total{not_owner}` > 0); non-owned keys untouched |
| 3 | Peer down within retention | Watermark catch-up replays exactly the missed change records; no bootstrap; mutation ordering preserved |
| 4 | Peer down beyond cap/age | `gap_detected_total` fires; owned ranges bootstrapped; no resurrection; bounded per-window reads only |
| 5 | Trim safety with a stuck peer | Trim frontier stalls at the retained-Dead peer; journal ≤ hard cap; after cap → gap → bootstrap |
| 6 | Read-trigger with foreign refs | Row absent on one owner, bytes elsewhere; read served via point fetch + LWW apply; subsequent reads hit locally |
| 7 | Tombstone safety under N>RF | Missed DELETE converges to the tombstone; no stale live row survives any bootstrap path |
| 8 | Mixed kill-switch mesh | Enabled nodes converge among themselves; disabled nodes skipped; re-enable triggers gap/catch-up; no storm |
| 9 | Crash matrix | Kill between journal fsync and row commit across every capture point; recovery is consistent; no lost acked write |
| 10 | Bootstrap/resume under ring churn | A ring change mid-bootstrap invalidates and recomputes windows; no false repair, no stuck cursor |

## Acceptance bar (epic DoD)

- [ ] **ADR:** ADR-0038 is the ratified basis (user aligned 2026-09-12); the
      ADR file's status flip and the ADR-0027 D5 clarification are recorded as
      upstream doc follow-ups; ADR-0037 is not resurrected; ADR-0034's
      accounting statement for the journal is recorded in S2.
- [x] **S1:** ae1 landed 2026-09-12 with review PASS — membership-driven
      retention, TTL escalation through the f5 D3 sink, pending-debt gauge,
      no hint mirroring; terminal no-live-holder signal with bounded dedupe,
      metric, and resume-on-return; existing suites show no regression (the
      one remaining failure is pre-existing and baseline-reproducing).
- [ ] **S2:** ae2 lands with review PASS under the kill-switch default
      (`enabled = false`), structurally inert (no journal I/O, no worker, no
      RPC, no metrics), J1 fail-closed with group commit, and passes f4
      Pass B once the fleet resumes (post-enable divergence assertions).
- [ ] **S3:** ae3 spec + implementation land with review PASS — g8 golden
      equivalence, enable-time bootstrap, gap/new-owner/pool-loss triggers,
      read-trigger single-flight; the **N>RF matrix is green** and the f4 P3
      re-run validates the windowed g8 path.
- [ ] **S4:** ae4 spec + implementation land with review PASS — injected
      silent loss (bypassing the journal) is detected within N cycles and
      repaired; mixed enabled/disabled mesh terminates in bounded catch-up,
      never an infinite gap loop.
- [ ] **No recurring full scans:** every recovery read is triggered and
      range-bounded; no scheduled phase has cost ∝ rows; O1 remains unwired.
- [ ] **Cost:** every fleet run is user-approved; no load/perf suite on the
      dev machine (PIPELINE §6).

## Cost & process guardrails

- **Fleet runs only on the user's explicit go-ahead.** The observed 3-node +
  volumes fleet cost is close to **€1/h** (user-verified 2026-09-12;
  PIPELINE §7); servers bill while they exist, and every resource bills a
  1-hour minimum. The fleet is at **0 servers / 0 volumes** now; nothing may
  be provisioned "to check" S1–S4 — local unit and in-process multi-node
  tests do the verification until a cloud pass is approved. Destroy at the
  end of every approved session.
- **No load/perf suite on the dev machine** (PIPELINE §6). Fleet passes are
  correctness runs, not performance measurements (fleet-degradation
  non-comparability rule).
- **No test-only hooks.** All work is product code + config; the f4 harness
  changes only by flipping `[metadata_sync] enabled` and restarting.

## Non-goals (explicit, recorded)

- Segment-plane reconciliation / holder-set repair / EC reconstruction —
  bytes stay f5/reconcile/ADR-0030's plane (S1's unrecoverable signal is a
  row/segment-plane dead-end classification only, not a byte-repair path).
- Read-path **push** repair and debt-replay repair (dropped 2026-09-11).
- Any recurring audit/sampling scan of the keyspace; "rare" does not excuse
  ∝ rows.
- New RocksDB CFs or WAL coupling; O1 is the unwired fallback.
- C2b proactive row migration (may later reuse the range API).
- Repair of extra (non-owner) stale rows — GC/ownership handles those.
- Changing the hint delivery contract (ADR-0027 as amended) or the f0 gate
  beyond S1's D7 hardening.
- A fleet session before the user approves the f4 resume.

## Cross-links

| Consumer | What this epic provides |
|---|---|
| [f4](../fleet-degradation/f4-pool-degradation-under-load.md) / fleet-degradation | Pass A baseline → Pass B validates S2 → P3 re-run validates S3. The work lands inert before any resumed fleet session; no second provisioning cycle is needed for the enable-time bootstrap. |
| [design draft](design-draft.md) | The verified gap record; this epic is its promotion path under ADR-0038. |
| [pr3 (superseded)](pr3-metadata-anti-entropy.md) | Historical O3 record; its scan is explicitly rejected. |
| [f0](../fleet-degradation/f0-hints-durability-gate.md) | Admission honesty; S2's J1 mirrors it, S1 hardens its debt ledger without changing the delivery contract. |
| [f5](../fleet-degradation/f5-degraded-pool-semantics.md) | S1 routes TTL-expired hint debt through f5 D3's `HintDropSink` so debt escalates instead of dying silently. |
| [disk-resilience-capacity](../disk-resilience-scale/epic.md) | S3's range iterator unblocks the inert `keyspace_fraction` sharding and can later serve C2b. |
| ADR-0027 D5 | Clarification follow-up (pull of version-guarded current state; refs are not identity; no local-segment-presence precondition) recorded for the ADR owner once ADR-0038 is accepted. |

## Open Questions (epic-level)

| # | Question | Owner | Blocks |
|---|---|---|---|
| a | **f4 resume timing** (user go-ahead). No provisioning before that; Pass A data precedes any final bound assertion. | user | f4 S2/S3 acceptance |
| b | **S3/S4 spec timing and the N>RF harness choice** — N ≥ 4 fleet vs an equivalent in-process weighted-ring harness; f4 alone cannot validate partial ownership. Decide at S3 spec time. | user + implementer | S3 spec |
| c | **ADR-0027 D5 clarification** — doc-only follow-up once ADR-0038 is accepted; wording recorded in S2's apply section. | ADR owner | doc hygiene |
| d | **O1 fallback trigger** — adopt only on evidence that journal capture completeness is intractable; no work otherwise. | ADR owner + implementer | post-S2 amendment |
| e | **Spot-check key provisioning** (cluster secret vs `metadata_sync` config) — ratified deferral; decided at S4 spec time. | implementer | S4 spec |

## Deviations (accepted)

_None yet — filled at epic close-out; per-feature deviations stay in the
feature docs._
