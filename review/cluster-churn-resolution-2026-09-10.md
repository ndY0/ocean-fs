# Cluster-churn debug session — resolution trace (2026-09-10)

**Goal (from the 2026-09-09 handoff):** make `e2e/tests/load_cluster_churn.rs`
pass cleanly on the phase-3 fleet.

**Outcome:** ✅ **PASS** — fleet quick run `3_load_cluster_churn_20260910T215424.json`:
all 10 assertions green, `result: pass`, read-quorum **0/106**, cache-invalidation
**0/20**, handoff `stored=41 delivered=41 dropped=0 pending=0`, and **0** "single
durable copy" traces on any node.

**Primary root cause (the one that mattered): writes ran with `write_quorum = 1`.**
`NodeConfig` had a `replication_factor` field but **no `write_quorum`/`read_quorum`
fields**, so the `write_quorum = 2` / `read_quorum = 2` keys in
`config_cluster_churn` and `scrub-deploy.sh` were silently dropped by serde.
`BucketConfigStore::new()` is empty, and the PUT/DELETE handlers fell back to
`unwrap_or(1)`. Every write acked on the local copy
(`write_quorum=1 quorum=1 acks=1 remote_targets=0`); replication was best-effort
rather than required and the honest-quorum guards never engaged. Most keys still
reached 2 copies via best-effort replication, which is why read-quorum only failed
sporadically (3–18 of ~108) instead of wholesale.

## Action trace

| Step | Action | Finding |
|---|---|---|
| 1 | Reload handoff; probe VMs | VMs gone (TTL); re-provisioned phase-3 fleet (`vm-up`, 3×CX33 + CX43). `code-graph` re-indexed. |
| 2 | Trace write coordinator quorum path | Guards present (`quorum_requires_ring`, readiness gate, rollback) but `req.write_quorum` was the deciding input. |
| 3 | Explore single-copy candidates | Ranked: quorum-rollback erasing prior rows; hint-apply counted as delivered; readiness gate opening at ring≥2; no object-row backfill. |
| 4 | **Fix C1** — quorum-failure rollback deleted the local metadata row unconditionally | For segment-tier writes the coordinator never wrote that row → a failed overwrite erased a prior committed version. Regression test added. Commit `7556647`. |
| 5 | **Fix class‑2** — PUT with unknown outcome recorded nothing | Added probe-and-record in the load harness (HEAD the body's ETag; record if it exists). Commit `7556647`. |
| 6 | **Fix hint honesty + L3 + readiness** | Hint appliers return success (accept only on durable apply); `CacheGrpcService` clears L3; readiness gate waits for roster stability (`cluster_stability_rounds × gossip interval`). Commit `7556647`. |
| 7 | Fleet run #1 | 9/10 assertions; read-quorum 9/107; handoff livelocked (`stored=44 delivered=7`). Root cause: all-or-nothing batch re-enqueue + Multi-tier hinted applies unsupported. |
| 8 | **Fix hint livelock** — per-hint partial acceptance + bounded give-up + Multi-tier apply | `HintedHandoffResponse.retry_indices`, durable `HintRecord.attempts`, `hint_max_delivery_attempts`, `hints_dropped_total`. Commits `fa96082`. |
| 9 | Fleet run #2 | 9/10; read-quorum 5; cache 0/20; handoff pending=0. Then **added instrumentation**: `x-oceanfs-hlc` / `x-oceanfs-tombstone-hlc` headers + manifest mutation provenance (`MutationEvent`). Commit `6ad1e92`. |
| 10 | Fleet run #3 (instrumented) | 9/10; read-quorum 8 → 5 genuine single-copy + 3 verifier false positives. Evidence showed **live segments being unlinked** (`segment unavailable on every holder`). |
| 11 | **Fix orphan reaper** — summed capture records raw, double-counting the same physical byte range | A segment hit `dead >= total` while a live row still referenced it → `.dat` unlinked. Count each `(segment, offset, length)` once. Removed the live-reference veto band-aid. Commit `4c0d216`. |
| 12 | Fleet run #4 | 9/10; read-quorum 3. Two were stale-L3 (404 with `tombstone_hlc=None`, later 200). |
| 13 | **Fix L3 on non-S3 writes** — `append_segment` / hint apply persisted rows without clearing the local L3 negative cache | Cleared L3 on those paths; also added the single-copy WARN trace. Commits `3183a86`. |
| 14 | Fleet run #5 | Still 10/107. The new trace exposed `write_quorum=1` — the missing config fields. |
| 15 | **Fix quorum config** — add `write_quorum`/`read_quorum` to `NodeConfig`; `BucketConfigStore` carries a node-level default policy; handlers use `get_or_default` | W=2 enforced. Commit `269aa4a`. |
| 16 | Fleet run #6 | ✅ **PASS — 10/10**, read-quorum 0/106, 0 single-copy traces. |

## Commits (all on `main`)

- `7556647` — quorum-rollback data loss, hint honesty, L3 invalidation, readiness stability, probe-and-record.
- `fa96082` — Multi-tier hinted applies, per-hint partial acceptance, bounded give-up.
- `6ad1e92` — per-key triage traces (HLC headers + mutation provenance).
- `4c0d216` — orphan reaper dead-chunk dedupe (stop reaping live segments) + verifier re-check.
- `3183a86` — clear L3 on non-S3 row writes + single-copy ack trace.
- `269aa4a` — honor configured `write_quorum` (root cause).
- `docs/adr/0027-hinted-handoff-ownership-model.md` amended (per-hint partial acceptance + bounded give-up).
- Grafana "Hinted Handoff" / "Per-Node Hinted Handoff" panels add the `dropped` series.

## Verification

- Fleet quick run `3_load_cluster_churn_20260910T215424.json` — `result: pass`, 10/10.
- Unit/lib: core 236, server 250, node 116, durability 285, cache 58, e2e lib 129.
- Functional cluster suites: write_path 6, hinted_handoff 3, lifecycle 4, cache_invalidation 2.
- `cargo fmt --check` clean; `cargo clippy --lib -D warnings` clean.

## Open items / notes for tomorrow

1. **Disk:** sut-0/1/2 at 34/28/28 G of 75 G after the W=2 run. Monitor; the
   earlier 71 G event was the wedged-hint loop. The orphan-reaper dedupe should
   keep reclamation correct.
2. **Dead config keys now live:** `write_quorum`/`read_quorum` are real
   `NodeConfig` fields now. Audit other `sut-deploy.sh` / `config_*` keys that may
   still be silently dropped.
3. **Verifier timing:** the read-quorum re-check (3×5 s) is retained; the fleet run
   passed without relying on it, but node-settling false positives are possible.
4. **Residual product items** (not blocking, now latent): CDELETE quorum-failure has
   no rollback; anti-entropy does not backfill missing object *rows*; the
   metadata-only replication append is HLC-blind (`put_object_in_bucket`).
5. **Environment:** phase-3 fleet still up; 4 h TTL from ~20:48 → auto-poweroff
   ~00:48. `vm-status` / `hcloud server poweron` to resume.
6. **Docs:** this trace + the phase-3 feature doc DoD/status updated. ADR-0027
   amended in-place.
