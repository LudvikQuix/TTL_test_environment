# RocksDB Rebalance Livelock Test — Runbook

Operator guide for the A/B rig. Spec: `./spec.md` · Architecture: `./architecture.md`.

**Topology.** Both filters run `replicas: 2` + `state.enabled` → one shared state
volume. Input `rebalance-raw-events` has 8 partitions (range-assigned 4/4). Feature
pin `eb10421c` (fix + hardening); stable pin `5e248ef3` (pre-fix). `max.poll.interval.ms
= 60000`. Churn/stop/scale and logs go through the Quix Portal API
(`scripts/rebalance_churn.py` / `scripts/rebalance_report.py`, built on
`scripts/portal_api.py`).

**Confirmed Portal endpoints (Q2).** scale = `PATCH /deployments/{id} {"replicas":N}`
(→ `--mode scale`, PRIMARY); stop/start = `PUT /deployments/{id}/stop|/start`
(→ `--mode restart` fallback); logs = `GET /deployments/{id}/logs/history/filter`
(`start`/`end` = epoch **seconds**) + `/logs/current`. List = `GET /deployments?workspaceId=…`.

## 0. Prerequisites

- `../.env` (repo root) has `Quix__Pat__Token`, `Quix__Workspace__Id`
  (`quixdev-ttltestenvironment-ttlgenerator`), `Quix__Portal__Api`
  (`https://portal-api.dev.quix.io`), `Quix__Sdk__Token`. (All present.)
- Local **Python 3.12** with `requests`, `python-dotenv`, `confluent-kafka`,
  `quixstreams` (the churn driver's 1 Hz watermark sampler needs the last two).
- `.tmp/rebalance/` is created automatically by the scripts (gitignored).

## 1. Sync the pipeline (push first — Quix builds from the pushed repo)

```bash
git push                                                  # push the branch first
quix cloud environments sync quixdev-ttltestenvironment-ttlgenerator
```

Then in the Portal, **before running**:
1. Confirm `rebalance-raw-events` was created with **8 partitions** (Topics view).
   Everything depends on multi-partition state.
2. **Confirm both `Rebalance Filter (Feature)` and `(Stable)` show `replicas = 2`**
   and a state volume. quix.yaml uses the canonical flat `resources: { cpu, memory,
   replicas }`. **If sync rejected the flat form** (Q3), the blocks carry a comment
   with the nested-`limits` fallback — apply it and then **set replicas = 2 in the
   Portal UI** (the nested form does not carry replicas). A deployment silently
   running at 1 replica invalidates the whole test.

## 2. Start the generator and warm the state

1. Start **Rebalance Generator**.
2. Start **Rebalance Filter (Feature)** (and/or **(Stable)** — separate topics /
   consumer groups / state namespaces, so they can run together if capacity allows).
3. Watch filter logs for `[STATE-SIZE-REBAL] … bytes=…`. **Wait until `bytes ≥ 300 MB`**
   (≈ 37 MB/partition) before churning — typically 5–15 min at `SLEEP_SECONDS=0.0001`.
   The churn driver can gate this automatically with `--warmup-bytes 314572800`.

## 3. S1 — steady-load churn (both builds)

Run feature first, then stable (or in parallel — outputs are keyed by deployment).
Each churn run takes ~40 min (10 cycles × 2 transitions × 120 s dwell); the driver
samples the output watermark at 1 Hz and snapshots logs per cycle throughout.

```bash
# Feature
python scripts/rebalance_churn.py --deployment "Rebalance Filter (Feature)" \
    --output-topic rebalance-feature-out --cycles 10 --dwell 120
python scripts/rebalance_report.py --deployment "Rebalance Filter (Feature)" \
    --output-topic rebalance-feature-out
#   -> .tmp/rebalance/report.csv (+ prints the table)

# Stable  (use a distinct --out so it does not overwrite the feature CSV)
python scripts/rebalance_churn.py --deployment "Rebalance Filter (Stable)" \
    --output-topic rebalance-stable-out --cycles 10 --dwell 120
python scripts/rebalance_report.py --deployment "Rebalance Filter (Stable)" \
    --output-topic rebalance-stable-out --out .tmp/rebalance/report-stable.csv
```

Leave the generator running throughout. `--dry-run` on the churn driver prints the
API calls without executing (rehearsal).

**Read the CSV against spec §8:**
- **Feature PASS:** `evictions=0`, `max_lock_attempt ≤ 2`, `fast_revoke_seen=True`
  (≥ 1 cycle), `longest_output_stall_s < 60 s` (target < 30 s), no unplanned restarts,
  `open_budget_hit=False`, `revoke_flush_timeout=False`.
- **Stable:** expect the degraded signature in ≥ 1 cycle — an eviction + lock-retry
  storm (`lock_retry_count` large, `max_lock_attempt` → 10), or a crash-loop, or
  `longest_output_stall_s ≥ 60 s`; and `fast_revoke_seen=False` in all cycles.
- If the stable build shows **none** of the signature → **INCONCLUSIVE** rig (spec §8
  escalation ladder), not a pass.

## 4. S2 — graceful stop while lock-waiting (feature)

Automated (induces a `2→1` handover, waits `--stop-after` for retries to begin, then
stops the whole deployment):

```bash
python scripts/rebalance_churn.py --deployment "Rebalance Filter (Feature)" \
    --output-topic rebalance-feature-out --mode stop --cycles 1 --stop-after 8 --dwell 60
python scripts/rebalance_report.py --deployment "Rebalance Filter (Feature)" \
    --output-topic rebalance-feature-out --out .tmp/rebalance/report-s2.csv
```

Or manual: run `--mode scale --cycles 1`, watch the survivor's logs for
`cannot acquire a lock` beginning to stream, then stop the deployment in the Portal.

**Feature PASS (crisp):** deployment reaches **Stopped, no error status, no crash-loop
restart**; process exits within ≈ one `open_retry_backoff` (~3 s) of the stop
(`RocksDBOpenAborted` aborts the backoff); **no** further `cannot acquire a lock` lines
after the stop timestamp; the pre-stop checkpoint is preserved (next start, no data
loss). Stable contrast: retries continue up to the remaining backoff (~30 s) before exit.

## 5. S3 — orphaned recovery-pause resume (observational)

In any churn run where a rebalance lands mid-recovery, the report's
`orphan_resume_seen` column flags `Resuming data partitions paused for recovery`.
Presence is the evidence; **absence is not a failure** (hard to force deterministically).

## 6. Teardown

1. Stop all three deployments (resting `desiredStatus: Stopped`).
2. Optionally bump `CG_VERSION` (both filters) to reset consumer groups/state for a
   clean re-run. Note: this also mints fresh empty changelog topics.
3. Evidence remains under `.tmp/rebalance/` (`report*.csv`, `*_watermarks.csv`,
   `*_cycles.json`, `logs/`), all gitignored.

## Notes (resolved)

- **DEBUG required:** filters run `QS_LOGLEVEL=DEBUG` so `fast_revoke_seen` (feature,
  DEBUG-level) and stable `max_lock_attempt` (DEBUG `attempt=N`) are captured. Lowering
  to INFO loses those signals. No per-message DEBUG in the hot path — it does not flood.
- **Log availability during a crash-loop (Q4/Q5):** the report uses server-side
  `history/filter` (retained) and falls back to the driver's live `logs/current`
  snapshots (`.tmp/rebalance/logs/`). Watermark sampling is broker-side, crash-robust.
- **Must set manually in the Portal (only if flat resources were rejected at sync):**
  `replicas = 2` on both filter deployments (Q3 fallback). Otherwise nothing manual.
```
