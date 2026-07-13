# Architecture — RocksDB Rebalance Livelock Test Rig

**Feature:** rebalance-livelock-test · **Owner:** ArchDev · **Spec:** `./spec.md` · **Runbook:** `./runbook.md`
**Under test:** quix-streams `fix/1131` (feature `eb10421c` = full fix + hardening; stable `5e248ef3` = pre-fix)

## 1. What this builds

A deployable Quix Cloud A/B rig that reproduces the RocksDB shared-volume rebalance
livelock on demand and turns each churn cycle into one row of machine-readable
evidence. Two byte-identical stateful filter deployments run with `replicas: 2` +
`state.enabled` (one shared state volume, 8 input partitions split 4/4). A local
Portal-API driver churns the replica count `2→1→2` to force cross-process RocksDB
lock handovers; a local scraper correlates deployment logs and the output topic's
high-watermark into a per-cycle CSV. The two deployments differ ONLY in the pinned
`quixstreams` commit, so any divergence is attributable to the fix.

## 2. Why this topology reproduces the bug

The livelock needs two processes contending for one RocksDB directory's OS file
lock. `state.enabled: true` + `replicas: 2` mounts one shared volume across both
replicas; Kafka's `range` assignment strategy (forced by quix-streams via
`consumer_extra_config_overrides`) splits the 8-partition input 4/4. A `2→1` churn
reassigns the terminated replica's 4 partition directories to the survivor while the
terminating replica is still closing them → the cross-process handover. Small RocksDB
buffers (1 MiB write buffer, 2 MiB SST target) + 2 KB value padding + no TTL maximize
background flush/compaction debt, so on the pre-fix build `db.close()` blocks past
`max.poll.interval.ms` (set low, 60 s) and the survivor spins on `cannot acquire a
lock`. On the feature build, `cancel_all_background(True)` releases the lock in ms,
fast-revoke skips the local flush, and `OpenDeadline` bounds the total open wait.

## 3. Components

| Component | Path | Responsibility |
|---|---|---|
| Feature filter | `rebalance-filter-feature/` | Stateful write-heavy filter pinned to `eb10421c`. |
| Stable filter | `rebalance-filter-stable/` | Same code, pinned to `5e248ef3`. |
| Generator | `quix.yaml` deployment `Rebalance Generator` | Reuses the existing `duplicate-key-generator` app; produces write-heavy keyed load to the new 8-partition `rebalance-raw-events`. **No new generator code.** |
| Portal helper | `scripts/portal_api.py` | Low-level `call()` + deployment helpers (resolve/scale/start/stop/replicas/logs). |
| Shared lib | `scripts/rebalance_common.py` | Paths, timeline I/O, marker extraction, cycle windows, Kafka watermark sampler. |
| Churn driver | `scripts/rebalance_churn.py` | Drives the churn, samples watermarks live, snapshots logs, writes the timeline. |
| Report | `scripts/rebalance_report.py` | Correlates timeline + watermarks + logs → the per-cycle CSV. |

The two filter folders share a **byte-identical** `main.py` and `build/dockerfile`
(verified in CI-lite: `diff -q`). The only differences are `requirements.txt` (the
pin) and the app.yaml / quix.yaml env defaults (`output`, `CG_PREFIX`).

## 4. Data flow

```
duplicate-key-generator ──(key={order_id}-{status}, JSON)──▶ rebalance-raw-events (8 partitions)
                                                                      │
                          ┌───────────────────────────────────────────┴───────────┐
                          ▼ (range assignment: 4 partitions each)                  ▼
              Rebalance Filter replica A                             Rebalance Filter replica B
              per msg: state.set("entry", {status, pad})            (shared /app/state volume)
              emit every msg ─────────────────────────┐            changelog__… (recovery delta)
                                                        ▼
                                        rebalance-<feature|stable>-out
                                                        ▲
                          1 Hz get_watermark_offsets ───┘  (rebalance_churn.py sampler thread)

Portal API  ◀── PATCH replicas / stop / start ── rebalance_churn.py ──▶ .tmp/rebalance/<dep>_cycles.json
            ◀── logs/history/filter, logs/current ─ rebalance_report.py           _watermarks.csv
                                                             │                     logs/*.log
                                                             ▼
                                                   .tmp/rebalance/report.csv (1 row / cycle)
```

**Live vs. offline split (important):** the 1 Hz output high-watermark sampling runs
**inside `rebalance_churn.py`** (a background thread), not in the report. A broker
high-watermark is a point-in-time value with no historical series API, so it MUST be
sampled while the churn happens. The report is a pure offline correlator over the
captured series + logs. (The spec assigned sampling to the report; see §7.)

## 5. Log-marker reference (ground truth from the committed builds)

Matched on invariant substrings because the two builds emit **different** text. The
filters run at `QS_LOGLEVEL=DEBUG` so the DEBUG-level markers are visible.

| Marker | Match | Level | Build | Source |
|---|---|---|---|---|
| Lock retry (count) | `cannot acquire a lock` | WARNING | both | `partition.py._init_rocksdb` |
| — feature text | `... on "<path>", cannot acquire a lock (attempt N/M). Retrying in Xsec.` | WARNING | feature | `eb10421c` L1031 |
| — stable text | `Failed to open rocksdb partition , cannot acquire a lock. Retrying in Xsec.` (path-less, **no** `(attempt N/M)`) | WARNING | stable | `5e248ef3` L1539 |
| Open attempt depth | `Opening rocksdb partition on "<path>" attempt=N` | DEBUG | both | `_init_rocksdb` top |
| Fast revoke | `Fast revoke: skipping local state flush` | **DEBUG** | feature only | `checkpoint.py` L338 |
| Orphan resume | `Resuming data partitions paused for recovery` | INFO | feature | `recovery.py` L491 |
| Open budget exhausted | `Open budget exhausted for rocksdb partition on ` / `would be exceeded by the next retry` | WARNING | feature | `partition.py` L1021/L1045 |
| Revoke flush timeout | `Revoke:` + `flush timed out` (sink or producer) | WARNING | feature | `checkpoint.py` L284/L382 |
| Eviction | `MAXPOLL` / `maximum poll interval` / member-left-group | — | librdkafka | consumer logs |

`max_lock_attempt`: parsed from the feature `(attempt N/M)` form (authoritative,
clean run → 0). The stable WARNING carries no attempt number, so when there ARE lock
retries the value falls back to the DEBUG `attempt=N` depth (→ approaches 10 in a
storm). If DEBUG is off on the stable build, this reads 0 during a storm — read
`lock_retry_count` instead.

## 6. Resolved open questions

- **Q1 (moot):** new 8-partition topic `rebalance-raw-events` (spec choice).
- **Q2 — Portal endpoints (confirmed against `…/swagger/v2/swagger.json`):**
  - Scale = `PATCH /deployments/{id}` body `{"replicas": N}` (`DeploymentPatchRequestV2.replicas`). **Replica scaling IS exposed → `--mode scale` is the PRIMARY mechanism; `--mode restart` (stop+start) is the fallback.**
  - Stop/Start = `PUT /deployments/{id}/stop` | `/start`.
  - Logs = `GET /deployments/{id}/logs/history/filter?start=<epoch_s>&end=<epoch_s>&limit=N` (windowed; **`start`/`end` are epoch SECONDS**, `entries[].timestamp` is **epoch NANOSECONDS**) and `GET /deployments/{id}/logs/current` (full plain text). Replicas = `GET /deployments/{id}/replicas` (`["0","1",…]`).
  - List = `GET /deployments?workspaceId=<ws>` (400 without the param).
- **Q3 — descriptor placement (confirmed against the Quix pipeline-descriptor reference + the live API model):**
  - Resources use the **canonical flat form** `resources: { cpu, memory, replicas }`. The spec's "replicas as a sibling of `limits`" was rejected: `replicas` is documented as a flat sibling of `cpu`/`memory`, so the nested placement risks being silently ignored (which would break the entire multi-replica topology). Each new block carries a comment with the nested-`limits` fallback (then set replicas in the Portal UI).
  - Topic partition count = `configuration: { partitions: 8 }` (matches `TopicConfiguration` and every existing topic).

## 7. Design decisions & deviations from the spec (with rationale)

1. **Emit every message, not on status change** (spec §6.1 said emit-on-change). The
   reused generator bakes the status into the record key (`{order_id}-{status}`), so
   per-key status is constant and emit-on-change goes silent after warm-up —
   defeating the stall metric whose stated intent is a "steady output signal". Emit-
   every-message fulfils that intent and makes output HWM a faithful processing-
   progress signal that stalls precisely when a partition's handover blocks.
2. **`QS_LOGLEVEL=DEBUG` (new env var, not in the spec's verbatim block).** The
   `Fast revoke: skipping local state flush` and benign-close markers are logged at
   DEBUG in the committed feature build; at the default INFO the spec's own PASS
   criterion `fast_revoke_seen=True` is unobservable. DEBUG also surfaces the
   `attempt=N` open line used for `max_lock_attempt` on the stable build. Verified:
   there is **no per-message DEBUG logging** in the SDF hot path, so DEBUG does not
   flood or slow processing.
3. **`[STATE-SIZE-REBAL]` marker** (spec §6.1 wrote `[STATE-SIZE-FEATURE]`/
   `[STATE-SIZE-STABLE]`). A single literal keeps `main.py` byte-identical across the
   two folders (spec §5.6); the `cg=` field disambiguates the builds.
4. **Watermark sampling lives in the churn driver, not the report** (spec §5.7/§7.6).
   Correctness fix — a historical 1 Hz high-watermark series can only be captured
   live. The report consumes the captured `_watermarks.csv`.
5. **No `rebalance-generator/` folder.** The spec §6.2 + the verbatim §7.4 block reuse
   the existing `duplicate-key-generator` app via a new deployment block (the brief's
   summary line said create a folder; its own instruction to append §7.4 verbatim
   reuses the app). Reuse avoids code duplication and touches no existing folder.
6. **`STATE_DIR` not declared** in app.yaml/quix.yaml (platform-managed; `main.py`
   default `"state"` resolves to the `/app/state` mount, matching `state.path`).
   Matches the spec's own §7.4 block, which omits it, and avoids the state-path
   mismatch warning.

## 8. CSV schema & metric computation (`.tmp/rebalance/report.csv`)

`cycle, deployment, handover_s, lock_retry_count, max_lock_attempt, evictions,
fast_revoke_seen, orphan_resume_seen, longest_output_stall_s, open_budget_hit,
revoke_flush_timeout` — one row per cycle.

- **Cycle window** = `[first action ts, next cycle first action ts)` (last cycle → end
  of the watermark series). `ref` = the cycle's first action (the `2→1` scale-down).
- **handover_s** = `ref` → first watermark sample after `ref` whose total HWM exceeds
  the baseline at `ref`. Healthy build: ~1 s; stalled build: tens of seconds.
- **longest_output_stall_s** = longest zero-advance interval of total HWM within the
  window (1 Hz sampling).
- Marker columns via `rebalance_common.extract_markers` over the window's log lines
  (Portal `history/filter` primary; per-cycle log snapshots as crash-proof fallback).

## 9. Integration with the existing pipeline

Append-only: 3 deployments + 3 topics added to `quix.yaml`; no existing deployment,
topic, or service folder modified. New topics `rebalance-raw-events` (8 partitions),
`rebalance-feature-out`, `rebalance-stable-out` are isolated from `raw-events` /
`deduped-events*` / `recovery-*` / `ttl-verify-*`. Changelog + repartition topics are
auto-created by quix-streams (not declared). The scripts extend the existing
`scripts/portal_api.py` and follow `scripts/changelog_stats.py`'s consumer pattern.
`scripts/` and `.tmp/` are gitignored (local tooling + evidence).

## 10. Operational caveats

- **DEBUG logging is required** for `fast_revoke_seen` and stable `max_lock_attempt`.
  Do not lower `QS_LOGLEVEL` to INFO unless you accept losing those signals.
- **Warm-up gate:** churn should not start until `[STATE-SIZE-REBAL] bytes ≥ 300 MB`.
  The driver can enforce it (`--warmup-bytes 314572800`); default is off (operator-
  gated per the runbook step 2).
- **Crash-loop log availability (Q4/Q5):** the report prefers server-side
  `history/filter` (retained across crashes) and falls back to the churn driver's
  live per-cycle `logs/current` snapshots. Watermark sampling is broker-side and
  unaffected by replica crashes.
- **Inconclusive-rig guard (spec §8):** if the stable build shows none of the degraded
  signature, the run is INCONCLUSIVE, not a pass — follow the spec's escalation ladder.
- **Interpreter:** run the scripts with a Python 3.12 that has `requests`,
  `python-dotenv`, `confluent-kafka`, `quixstreams` (the report's watermark sampler is
  in the churn driver; the report itself needs only `requests`+`dotenv`).
```
