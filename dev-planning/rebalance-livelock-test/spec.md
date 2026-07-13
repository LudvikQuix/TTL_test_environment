# RocksDB Rebalance Lock-Contention Livelock Test

**Status:** Draft
**Project:** TTL_test_environment
**Created:** 2026-07-13
**Planned with:** Buddy
**PR under test:** quix-streams `fix/1131` — "Fix RocksDB rebalance lock-contention livelock and harden state handover"

> **As-built pass — 2026-07-13.** ArchDev implemented this spec and reported six deviations
> + two factual corrections, all rooted in the spec layer. They are folded in below as dated
> `As-built (2026-07-13)` notes (originals preserved). Authoritative rationale + confirmed API
> details: `./architecture.md`. The runbook is finalized by ArchDev — do not edit it.
> Summary of as-built changes: (1) filters emit every message, not on-change; (2) filters run
> `QS_LOGLEVEL=DEBUG` (fast-revoke/benign-close markers are DEBUG-only); (3) single state-size
> marker `[STATE-SIZE-REBAL]` for both builds; (4) live output-watermark sampling moved into the
> churn driver; (5) no `rebalance-generator/` folder (generator app reused verbatim); (6)
> `STATE_DIR` not declared (platform-managed `/app/state`); (a) the stable build's lock-retry
> WARNING has no `(attempt N/M)` suffix; (b) `quix.yaml` uses the flat
> `resources: { cpu, memory, replicas }` form (Q3 resolved).

## 1. Summary

Add a deployable Quix Cloud A/B rig that reproduces and verifies the quix-streams
`fix/1131` fix **including the review hardening**. Two identical write-heavy stateful filter
services run with **2 replicas sharing one state volume** (`state.enabled: true`,
`replicas: 2`) — the exact #1131 topology. A local driver forces repeated rebalances by
churning the replica count (`2 → 1 → 2`), which moves RocksDB state partitions between
replicas on the shared volume. On the **feature** build (pinned to `origin/fix/1131` HEAD
`eb10421c`, the full fix + hardening), the outgoing replica cancels background
flush/compaction and skips the local state flush during revoke, releasing the OS file lock
in milliseconds; every blocking step is additionally bounded (revoke flushes at
`max.poll.interval.ms × 0.2`, total per-assign open time at `max.poll.interval.ms × 0.5`),
and a graceful SIGTERM aborts a contended open cleanly. On the **stable** build (pinned to
pre-fix `5e248ef3`), the outgoing replica blocks inside `db.close()` on background work,
holds the lock past `max.poll.interval.ms`, is evicted mid-revoke, and the incoming replica
spins on `cannot acquire a lock` — the livelock. A local scraper turns each churn cycle
into one CSV row of log-derived + broker-derived evidence. Verification is
manual/operator-driven via the Quix Portal API; no automated CI.

The two builds differ **only** in the pinned `quixstreams` commit; identical service code,
env, resources, and choreography. Any behavioral divergence is therefore attributable to the fix.

## 2. Goals

- Reproduce the shared-volume rebalance handover on demand in Quix Cloud, repeatedly, under
  write-heavy state (hundreds of MB across 8 partitions split over 2 replicas).
- Demonstrate the **feature** build handing partitions over cleanly under churn: no
  crash-loop, bounded lock retries, bounded output stall, fast-revoke marker present, and
  every blocking revoke/open step bounded by its `max.poll.interval.ms`-scaled budget.
- Demonstrate a **graceful stop during a contended open** exiting cleanly (S2) — a crisp,
  now-committed behavior on the feature build.
- Demonstrate the **stable** build degrading under identical choreography (retry storms,
  evictions, long stalls, possible crash-loop), and define what "reproduced the issue"
  means so a non-reproduction is flagged as an *inconclusive rig*, not a pass.
- Emit machine-readable evidence: one CSV row per churn cycle per deployment.
- Cover three scenarios: S1 steady-load churn (both builds), S2 graceful-stop-while-lock-waiting
  (feature clean exit), S3 orphaned recovery-pause resume (best-effort/observational).
- Ship an operator runbook (`runbook.md`).

## 3. Non-goals

- No automated tests, assertions, or CI. Phase 1 is build + operator-driven run + CSV read.
- No changes to any existing service folder, existing `quix.yaml` deployment entry, or the
  existing topics (`raw-events`, `deduped-events*`, `recovery-*`, `ttl-verify-*`).
- No modification of the quix-streams repo and **no push** to it — the pins reference commits
  already on `origin`.
- No new cloud deployments for the driver/scraper — those run locally against the Portal API +
  Kafka.
- No third (pre-hardening `0103ccb1`) variant — the matrix stays two builds (feature@`eb10421c`
  vs stable@`5e248ef3`) to keep it small.

## 4. Scenarios

| ID | Name | Builds | Mechanism | What it proves | Determinism |
|----|------|--------|-----------|----------------|-------------|
| **S1** | Steady-load churn | feature + stable | N cycles of replica churn `2→1→2` at fixed dwell, under warmed state + live generator | Feature: clean handover (no crash-loop, bounded retries, bounded stall, fast-revoke seen, budgets never exhausted). Stable: degraded (retry storm / eviction / long stall / crash-loop). | Deterministic |
| **S2** | Graceful stop while lock-waiting | feature (primary); stable (contrast) | During a `2→1` handover, while the survivor is mid open-retry, **stop the whole deployment** | Feature: SIGTERM during a contended open aborts via `RocksDBOpenAborted` → **clean exit, deployment reaches Stopped with no error status and no crash-loop restart**; the pre-stop checkpoint/commit is preserved. Stable: sleeps through remaining backoff before exiting. | Semi-deterministic (must catch the retry window) |
| **S3** | Orphaned recovery-pause resume | feature | Force a rebalance *during* recovery so a replica is reassigned data partitions while it brings no stateful partitions | `Resuming data partitions paused for recovery: [...]` marker fires; partitions do not stay paused. | **Best-effort / observational** (hard to force deterministically — non-appearance is not a failure) |

## 5. Proposed design

### 5.1 Topology and why it reproduces the bug

The livelock (see `C:\repos\quix-streams_revire\docs\rocksdb-lock-contention-analysis.md`,
which now also carries the committed log-reference table) requires **two processes contending
for one RocksDB directory's OS file lock**: an outgoing owner still inside a slow `db.close()`
while a *different* incoming owner tries to open the same directory. In Quix Cloud, a deployment
with `state.enabled: true` and `replicas > 1` mounts **one shared state volume** across replicas.
Kafka splits the input topic's partitions across the two replicas (same consumer group); each
replica opens the per-partition RocksDB subdirectories for the partitions it owns. A rebalance
that moves a partition from replica A to replica B is a cross-process lock handover on the shared
volume — exactly the bug's precondition.

> **As-built (2026-07-13).** quix-streams forces Kafka's `range` assignment strategy (via
> `consumer_extra_config_overrides`), so the 8-partition input splits deterministically 4/4 across
> the two replicas. Confirmed in `architecture.md` §2.

This **requires the input topic to have ≥ 2 partitions** so state is split across replicas.
The existing `raw-events` partition count cannot be assumed or modified, so the rig uses a
**new 8-partition input topic** `rebalance-raw-events` (2 replicas → 4 partitions each →
4 concurrent handovers per churn step). See §10 Q1 for the cheaper "reuse `raw-events`" path
if it is confirmed to already have ≥ 4 partitions.

### 5.2 Forcing rebalances — churn mechanism

**Chosen: replica-count churn `2 → 1 → 2`** via the Portal API, per cycle, with a dwell between
transitions.

- `2 → 1`: one replica is terminated (SIGTERM → graceful `Application.stop()` → revoke + `db.close()`).
  The survivor is reassigned the terminated replica's 4 partitions and must open those directories
  while the terminating replica closes them → the cross-process handover. On the stable build,
  slow closes make the survivor spin on lock retries.
- `1 → 2`: a new replica joins; 4 partitions are revoked from the survivor and opened by the new
  replica → a second handover.

This is preferred over a rolling **restart** because it deterministically produces the
cross-replica lock handover on the shared volume and gives a clean, scriptable knob
(replica count). A deployment **restart** (rolling) is the documented fallback if the Portal
API cannot set replica count cleanly (see §10 Q2).

> **As-built (2026-07-13).** Q2 resolved: `PATCH /deployments/{id}` `{"replicas": N}` IS exposed,
> so `--mode scale` is the primary mechanism and `--mode restart` (stop+start via
> `PUT /deployments/{id}/stop|start`) is the fallback. Endpoint details in §10 Q2 / `architecture.md` §6.

### 5.3 State pressure to make `close()` slow

Slow `close()` on the stable build comes from background flush/compaction debt. The rig maximizes
that debt with:

- **Small RocksDB buffers** via the existing env knobs (`ROCKSDB_WRITE_BUFFER_SIZE=1 MiB`,
  `ROCKSDB_TARGET_FILE_SIZE_BASE=2 MiB`, `ROCKSDB_MAX_WRITE_BUFFER_NUMBER=2`) → frequent flushes,
  many small SSTs, continuous compaction — the pressure `db.close()` blocks on.
- **Value padding** (`VALUE_PADDING_BYTES=2000`) and a large key space so state grows to hundreds
  of MB quickly (theoretical max ≈ `distinct_keys × VALUE_PADDING_BYTES`; with the reused generator
  at `KEY_SPACE=150000` and keys `{order_id}-{status}` (≈ 300k distinct) → ≈ 600 MB total, ≈ 75 MB
  per partition).
- **Warm-up gate**: churn does not start until the `[STATE-SIZE-*]` logger reports on-disk
  `bytes ≥ 300 MB` (≈ 37 MB per partition ≈ 18 SSTs at the 2 MiB target — enough compaction debt
  to block a plain close). Derivation: 300 MB ≈ 50% of the ≈ 600 MB theoretical max, typically
  reached in ~5–15 min at cloud throughput.

> **As-built (2026-07-13).** The single per-key state entry is written under a fixed logical key
> (`state.set("entry", {status, pad})`) per message; the state-size marker is `[STATE-SIZE-REBAL]`
> for both builds (not `[STATE-SIZE-FEATURE]`/`[STATE-SIZE-STABLE]`) — see §6.1 note. The driver can
> enforce the gate with `--warmup-bytes 314572800`; default is operator-gated per the runbook.

### 5.4 Changelog on (required for fast revoke)

Both filters keep `use_changelog_topics=True` (quix-streams default — do **not** disable). The
Stage-2 fast revoke (`Checkpoint.commit(revoking=True)` skipping the local flush) only fires for
changelog-backed stores; the changelog already carries the delta and the new owner replays it on
recovery. On the feature build this is the observable `Fast revoke: skipping local state flush`
marker; on the stable build no such skip exists.

> **As-built (2026-07-13).** That marker is emitted at **DEBUG** in the committed feature build, so
> the filters must run at `QS_LOGLEVEL=DEBUG` for the §8 `fast_revoke_seen=True` PASS criterion to be
> observable. See §6.1 / §7.1.

### 5.5 No `group_by`

The filters process `rebalance-raw-events` **directly** (`sdf` keyed by the incoming message key,
`stateful=True`) — no `group_by`. This keeps state partitions equal to the source partitions (8),
split cleanly across the 2 replicas, without introducing a repartition topic. Semantic dedup is
irrelevant to this test; the goal is heavy, partitioned stateful writes. TTL is **off** so state
grows monotonically (an expiring store would stay small and reduce close-time pressure).

> **As-built (2026-07-13).** The filters **emit every message** (not on status-change). The reused
> generator bakes the status into the record key (`{order_id}-{status}`), so per-key status is constant
> and an emit-on-change design goes silent after warm-up — defeating the output-stall metric. Emitting
> every message makes the output high-watermark a faithful processing-progress signal that stalls
> precisely when a partition's handover blocks. (Corrects §6.1's original "emit on change".)

### 5.6 Identical A/B builds, pin is the only difference

`rebalance-filter-feature` and `rebalance-filter-stable` are byte-identical service folders
(`main.py`, `app.yaml`, `build/dockerfile`) differing only in:
- `requirements.txt` pin (feature `eb10421c…`, stable `5e248ef3…`), and
- default env (`output` topic, `CG_PREFIX`).

> **As-built (2026-07-13).** Byte-identical `main.py` is preserved precisely because the state-size
> marker is the single literal `[STATE-SIZE-REBAL]` (disambiguated by its `cg=` field) rather than a
> per-build literal. `diff -q` on the two `main.py` / `build/dockerfile` is part of ArchDev's checks.

### 5.7 Local tooling

- **`scripts/rebalance_churn.py`** — Portal-API driver. Runs N cycles of `2→1→2` against a named
  deployment with configurable dwell; writes a machine-readable cycle timeline to
  `.tmp/rebalance/<deployment>_cycles.json` (cycle index, action, ISO timestamps) for correlation.
- **`scripts/rebalance_report.py`** — evidence scraper. For each cycle window from the timeline:
  (a) pulls deployment logs via the Portal API and extracts marker lines; (b) samples the output
  topic's high-watermark at 1 Hz (Kafka consumer, same `Quix__Sdk__Token` pattern as
  `scripts/changelog_stats.py`) to measure output progress/stall. Emits one CSV row per cycle per
  deployment.

> **As-built (2026-07-13).** The **1 Hz output-watermark sampling lives inside the churn driver**
> (background thread → `.tmp/rebalance/<deployment>_watermarks.csv`), NOT in the report. A broker
> high-watermark is a point-in-time value with no historical-series API, so it must be sampled live
> during churn. The report is a pure offline correlator over the captured watermark series + logs.
> A shared `scripts/rebalance_common.py` (paths, timeline I/O, marker extraction, cycle windows,
> watermark sampler) backs both scripts. (Corrects the split described here and in §7.6.)

Both build on the existing `scripts/portal_api.py` (`call(method, path, **kwargs)`, tokens from
`../.env`). Scripts run locally on Windows / Python 3.12; `scripts/` is gitignored (local tooling).

## 6. Sub-features / work breakdown

### 6.1 `rebalance-filter-feature/` and `rebalance-filter-stable/` — owner: ArchDev

Identical stateful filter code in both folders. Behavior:
- Consume `input` (default `rebalance-raw-events`), value JSON.
- Per message (`sdf.apply`/`update` with `stateful=True, metadata=True`): write a padded entry
  keyed by the record key — `state.set(key, {"status": value["status"], "pad": "x"*VALUE_PADDING_BYTES})`;
  emit to `output` when the stored status changes (steady output signal for stall detection). No TTL,
  no `group_by`.
- `Application(consumer_group=f"{CG_PREFIX}-{CG_VERSION}", state_dir=STATE_DIR,
  rocksdb_options=RocksDBOptions(write_buffer_size=…, target_file_size_base=…, max_write_buffer_number=…),
  consumer_extra_config={"max.poll.interval.ms": MAX_POLL_INTERVAL_MS})`. Do **not** pass
  `use_changelog_topics=False`.
- Periodic `[STATE-SIZE-FEATURE]` / `[STATE-SIZE-STABLE]` logger thread mirroring
  `dedup-filter-stable/main.py` lines 75–139, but **cheap metrics only** — `_dir_size_bytes` +
  `rocksdb.estimate-num-keys`. Do **not** run the O(keys) exact scan (it would itself read RocksDB
  during a handover and can contend with/slow the close). Interval `STATE_SIZE_LOG_INTERVAL=5`.
- Reference patterns: `dedup-filter-stable/main.py` (`load_dotenv()` before quixstreams import;
  `RocksDBOptions` env knobs; logger thread; `_iter_partitions`).

> **As-built (2026-07-13) — four corrections to this sub-feature:**
> 1. **Emit EVERY message**, not on status change (see §5.5 note). The record key bakes in the status,
>    so on-change goes silent after warm-up and breaks the stall metric.
> 2. **State-size marker is the single literal `[STATE-SIZE-REBAL]` for both builds** (not
>    `[STATE-SIZE-FEATURE]`/`[STATE-SIZE-STABLE]`), keeping `main.py` byte-identical (§5.6). The `cg=`
>    field (built from `CG_PREFIX`) disambiguates the two deployments in logs.
> 3. **New env `QS_LOGLEVEL=DEBUG`** (not in the original §7.1 table): the `Fast revoke: skipping local
>    state flush` and benign-close markers, plus the `Opening rocksdb partition … attempt=N` line used
>    for stable `max_lock_attempt`, are DEBUG-level in the committed builds. There is no per-message
>    DEBUG logging in the SDF hot path, so DEBUG does not flood or slow processing.
> 4. **`STATE_DIR` is not declared** as a platform variable; `main.py` keeps its default `"state"`,
>    which resolves to the platform-managed `/app/state` mount (matching `state.path`). Avoids a
>    state-path mismatch warning.

File tree per folder: `main.py`, `requirements.txt`, `app.yaml`, `.env.example`, `build/dockerfile`
(copy `dedup-filter-stable/build/Dockerfile`).

**Only difference** between the two folders: `requirements.txt` pin and the two default env values
(`output`, `CG_PREFIX`).

### 6.2 `Rebalance Generator` deployment (reuses `duplicate-key-generator` app) — owner: ArchDev

No new generator code. A new `quix.yaml` deployment block referencing `application: duplicate-key-generator`,
env `output=rebalance-raw-events`, `KEY_SPACE=150000`, `SLEEP_SECONDS=0.0001`, `MESSAGE_COUNT=0`,
`SEED=42`. Produces the write-heavy keyed load to the new 8-partition topic. See §10 Q1 for skipping
this if reusing `raw-events`.

> **As-built (2026-07-13).** Reused **verbatim** as a new `quix.yaml` deployment entry only — **no
> `rebalance-generator/` service folder was created** and no existing folder was touched.

### 6.3 `quix.yaml` additions — owner: ArchDev

Append the three deployment blocks and three topic entries in §7.4 verbatim. Do not modify existing
entries.

### 6.4 `scripts/rebalance_churn.py` — owner: ArchDev

Portal-API driver (§7.5). Depends on 6.3 (deployment names). Writes `.tmp/rebalance/<deployment>_cycles.json`.

> **As-built (2026-07-13).** Also owns the live 1 Hz output-watermark sampler thread →
> `.tmp/rebalance/<deployment>_watermarks.csv`, and snapshots per-cycle logs (crash-proof fallback).
> Shares `scripts/rebalance_common.py` with the report.

### 6.5 `scripts/rebalance_report.py` — owner: ArchDev

Log + watermark scraper (§7.6). Depends on 6.4 (timeline) and 6.3 (topic names). Emits the CSV in §7.3.

> **As-built (2026-07-13).** Offline correlator only — consumes the driver's captured
> `_watermarks.csv` + logs; it does **not** sample the broker itself.

### 6.6 `runbook.md` — owner: ArchDev (skeleton in this repo; finalized after implementation)

Operator steps: sync → start generator → warm state to target → run churn → run report → read CSV.
Skeleton delivered alongside this spec.

> **As-built (2026-07-13).** ArchDev has finalized the runbook; it is authoritative and must not be
> re-edited from the spec side.

## 7. Data & interface contracts

### 7.1 Env vars — `rebalance-filter-feature` / `rebalance-filter-stable` (identical)

| Env var | Default (feature / stable) | Maps to |
|---|---|---|
| `input` | `rebalance-raw-events` | input topic |
| `output` | `rebalance-feature-out` / `rebalance-stable-out` | output topic |
| `CG_PREFIX` | `rebalance-filter-feature` / `rebalance-filter-stable` | consumer-group prefix (state namespace); also the `cg=` field in `[STATE-SIZE-REBAL]` |
| `CG_VERSION` | `v1` | consumer-group suffix; bump for a fresh store |
| `QS_LOGLEVEL` | `DEBUG` | quix-streams log level. **DEBUG is required** — fast-revoke, benign-close, and the `attempt=N` open line are DEBUG-only. |
| `VALUE_PADDING_BYTES` | `2000` | bytes of `x` padding per stored value |
| `MAX_POLL_INTERVAL_MS` | `60000` | `consumer_extra_config["max.poll.interval.ms"]` |
| `ROCKSDB_WRITE_BUFFER_SIZE` | `1048576` | `RocksDBOptions.write_buffer_size` (1 MiB) |
| `ROCKSDB_TARGET_FILE_SIZE_BASE` | `2097152` | `RocksDBOptions.target_file_size_base` (2 MiB) |
| `ROCKSDB_MAX_WRITE_BUFFER_NUMBER` | `2` | `RocksDBOptions.max_write_buffer_number` |
| `STATE_SIZE_LOG_INTERVAL` | `5` | seconds between `[STATE-SIZE-REBAL]` lines |
| `LOGGER` | `on` | enable/disable the periodic logger thread |
| `Quix__Sdk__Token` | — (local via `.env`) | broker auth when run locally |

> **As-built (2026-07-13).** `QS_LOGLEVEL=DEBUG` added (correction #2). `STATE_DIR` **removed** from
> this table (correction #6) — platform-managed `/app/state`. State-size marker literal is
> `[STATE-SIZE-REBAL]` (correction #3).

### 7.2 Env vars — `Rebalance Generator` (reused `duplicate-key-generator`)

| Env var | Default | Maps to |
|---|---|---|
| `output` | `rebalance-raw-events` | output topic |
| `KEY_SPACE` | `150000` | random order-id upper bound |
| `MESSAGE_COUNT` | `0` | 0 = run forever (Service) |
| `SLEEP_SECONDS` | `0.0001` | inter-message sleep (~write-heavy) |
| `SEED` | `42` | deterministic sequence |

### 7.3 CSV schema — `scripts/rebalance_report.py`

One row per churn cycle per deployment. File: `.tmp/rebalance/report.csv`.

| Column | Type | Derivation |
|---|---|---|
| `cycle` | int | cycle index from the timeline (`_cycles.json`) |
| `deployment` | str | deployment name (feature or stable) |
| `handover_s` | float | wall-clock from the cycle's churn action (`2→1` scale-down = `ref`) to the first output high-watermark sample after `ref` whose total HWM exceeds the baseline at `ref` |
| `lock_retry_count` | int | count of `cannot acquire a lock` lines in the cycle window (summed over replicas/partitions) |
| `max_lock_attempt` | int | see as-built note below |
| `evictions` | int | count of eviction lines in the window: `MAXPOLL` / `maximum poll interval … exceeded` / member-left-group |
| `fast_revoke_seen` | bool | any `Fast revoke: skipping local state flush` (DEBUG) in the window |
| `orphan_resume_seen` | bool | any `Resuming data partitions paused for recovery` in the window |
| `longest_output_stall_s` | float | longest single interval with zero output high-watermark advance during the cycle window (1 Hz sampling by the driver) |
| `open_budget_hit` | bool | any `Open budget exhausted` / `would be exceeded by the next retry` (feature only) |
| `revoke_flush_timeout` | bool | any `Revoke: sink … flush timed out` / `Revoke: producer flush timed out` (feature only) |

> **As-built (2026-07-13).** `open_budget_hit` and `revoke_flush_timeout` are **standard columns**
> (the original spec listed them as optional). Correction (a): the **stable** build's lock-retry
> WARNING has **no `(attempt N/M)` suffix** — parsers match the `cannot acquire a lock` substring for
> `lock_retry_count`, and derive `max_lock_attempt` from the feature `(attempt N/M)` form (authoritative)
> or, on the stable build, backfill from the DEBUG `Opening rocksdb partition … attempt=N` depth (→ approaches
> 10 in a storm). If DEBUG is off on the stable build, `max_lock_attempt` reads 0 even during a storm —
> read `lock_retry_count` instead.

**Marker source.** All markers are derivable directly from the committed builds. The
**authoritative message list is the log-reference table in
`C:\repos\quix-streams_revire\docs\rocksdb-lock-contention-analysis.md`** and the confirmed table in
`./architecture.md` §5. Match on invariant substrings (below) for robustness.

| Marker | Invariant substring | Level | Build | Source |
|---|---|---|---|---|
| Lock retry | `cannot acquire a lock` | WARNING | both | `partition.py` `_init_rocksdb` |
| — feature text | `… on "<path>", cannot acquire a lock (attempt N/M). Retrying in Xsec.` | WARNING | feature (`eb10421c`) | L1031 |
| — stable text | `Failed to open rocksdb partition , cannot acquire a lock. Retrying in Xsec.` (path-less, **no** `(attempt N/M)`) | WARNING | stable (`5e248ef3`) | L1539 |
| Open attempt depth | `Opening rocksdb partition on "<path>" attempt=N` | DEBUG | both | `_init_rocksdb` top |
| Fast revoke | `Fast revoke: skipping local state flush` | **DEBUG** | feature only | `checkpoint.py` L338 |
| Orphan resume | `Resuming data partitions paused for recovery` | INFO | feature | `recovery.py` L491 |
| Open budget exhausted | `Open budget exhausted for rocksdb partition on ` / `would be exceeded by the next retry` | WARNING | feature | `partition.py` L1021/L1045 |
| Revoke flush timeout | `Revoke:` + `flush timed out` (sink or producer) | WARNING | feature | `checkpoint.py` L284/L382 |
| Benign close | `shutdown in progress` | DEBUG | feature | `partition.py` `close()` |
| Cancel failed | `Failed to cancel background work before closing rocksdb` | WARNING | feature | `partition.py` `close()` |
| Eviction (librdkafka) | `MAXPOLL` / `maximum poll interval` / member-left-group | — | both | consumer logs |

### 7.4 `quix.yaml` additions (verbatim)

> **As-built (2026-07-13) — correction (b).** Resources use the **canonical flat form**
> `resources: { cpu, memory, replicas }`. The original draft placed `replicas: 2` under `resources:`
> as a sibling of `limits:`; that is wrong — `replicas` is a flat sibling of `cpu`/`memory`, and the
> nested placement would be **silently ignored**, breaking the entire multi-replica topology (Q3 resolved).
> The blocks below are the as-built form. If a given workspace's schema rejects the flat form, fall back
> to `resources: { limits: { cpu, memory } }` and set replicas in the Portal UI. `QS_LOGLEVEL=DEBUG`
> added to both filter blocks; `STATE_DIR` intentionally absent.

Append to `deployments:`:

```yaml
  - name: Rebalance Generator
    application: duplicate-key-generator
    version: latest
    deploymentType: Service
    resources:
      cpu: 200
      memory: 200
      replicas: 1
    desiredStatus: Stopped
    variables:
      - name: output
        inputType: OutputTopic
        description: Write-heavy keyed load for the rebalance test (8-partition topic).
        required: true
        value: rebalance-raw-events
      - name: KEY_SPACE
        inputType: FreeText
        value: 150000
      - name: MESSAGE_COUNT
        inputType: FreeText
        value: 0
      - name: SLEEP_SECONDS
        inputType: FreeText
        value: 0.0001
      - name: SEED
        inputType: FreeText
        value: 42
  - name: Rebalance Filter (Feature)
    application: rebalance-filter-feature
    version: latest
    deploymentType: Service
    resources:
      cpu: 500
      memory: 2000
      replicas: 2
    state:
      enabled: true
      size: 2
      path: /app/state
    desiredStatus: Stopped
    variables:
      - name: input
        inputType: InputTopic
        required: true
        value: rebalance-raw-events
      - name: output
        inputType: OutputTopic
        required: true
        value: rebalance-feature-out
      - name: CG_PREFIX
        inputType: FreeText
        value: rebalance-filter-feature
      - name: CG_VERSION
        inputType: FreeText
        value: v1
      - name: QS_LOGLEVEL
        inputType: FreeText
        description: quix-streams log level. Keep DEBUG — fast-revoke / benign-close / attempt=N markers are DEBUG-only. No per-message hot-path DEBUG.
        value: DEBUG
      - name: VALUE_PADDING_BYTES
        inputType: FreeText
        value: 2000
      - name: MAX_POLL_INTERVAL_MS
        inputType: FreeText
        description: consumer_extra_config max.poll.interval.ms. Low (60000) so a slow revoke/close can exceed it and eviction is observable; also scales the feature build's bounded-flush (x0.2) and open-deadline (x0.5) budgets.
        value: 60000
      - name: ROCKSDB_WRITE_BUFFER_SIZE
        inputType: FreeText
        value: 1048576
      - name: ROCKSDB_TARGET_FILE_SIZE_BASE
        inputType: FreeText
        value: 2097152
      - name: ROCKSDB_MAX_WRITE_BUFFER_NUMBER
        inputType: FreeText
        value: 2
      - name: STATE_SIZE_LOG_INTERVAL
        inputType: FreeText
        value: 5
      - name: LOGGER
        inputType: FreeText
        value: on
  - name: Rebalance Filter (Stable)
    application: rebalance-filter-stable
    version: latest
    deploymentType: Service
    resources:
      cpu: 500
      memory: 2000
      replicas: 2
    state:
      enabled: true
      size: 2
      path: /app/state
    desiredStatus: Stopped
    variables:
      - name: input
        inputType: InputTopic
        required: true
        value: rebalance-raw-events
      - name: output
        inputType: OutputTopic
        required: true
        value: rebalance-stable-out
      - name: CG_PREFIX
        inputType: FreeText
        value: rebalance-filter-stable
      - name: CG_VERSION
        inputType: FreeText
        value: v1
      - name: QS_LOGLEVEL
        inputType: FreeText
        description: quix-streams log level. Keep DEBUG so stable max_lock_attempt can be backfilled from the DEBUG attempt=N line (the stable WARNING has no attempt number).
        value: DEBUG
      - name: VALUE_PADDING_BYTES
        inputType: FreeText
        value: 2000
      - name: MAX_POLL_INTERVAL_MS
        inputType: FreeText
        value: 60000
      - name: ROCKSDB_WRITE_BUFFER_SIZE
        inputType: FreeText
        value: 1048576
      - name: ROCKSDB_TARGET_FILE_SIZE_BASE
        inputType: FreeText
        value: 2097152
      - name: ROCKSDB_MAX_WRITE_BUFFER_NUMBER
        inputType: FreeText
        value: 2
      - name: STATE_SIZE_LOG_INTERVAL
        inputType: FreeText
        value: 5
      - name: LOGGER
        inputType: FreeText
        value: on
```

Append to `topics:`:

```yaml
  - name: rebalance-raw-events
    configuration:
      partitions: 8
  - name: rebalance-feature-out
  - name: rebalance-stable-out
```

Notes: `partitions: 8` is the crux (state split across 2 replicas). `replicationFactor` is omitted
so it defaults to the cluster value (setting RF > broker count fails on a single-broker dev cluster).
Changelog and repartition topics are auto-created by quix-streams — do not declare them. As-built:
`configuration: { partitions: 8 }` confirmed against `TopicConfiguration` (§10 Q3).

### 7.5 `scripts/rebalance_churn.py` — contract

```
Usage: python scripts/rebalance_churn.py --deployment "Rebalance Filter (Feature)" \
       [--cycles 10] [--dwell 120] [--down-replicas 1] [--up-replicas 2] \
       [--mode scale|restart] [--output-topic rebalance-feature-out] [--warmup-bytes N]
```

Per run:
1. Resolve `deploymentId` by name (`GET /deployments?workspaceId=<ws>`).
2. Start a background 1 Hz output-watermark sampler → `.tmp/rebalance/<deployment>_watermarks.csv`.
3. For each cycle: set replicas → `down-replicas` (default 1); record action + timestamp; sleep `dwell`;
   set replicas → `up-replicas` (default 2); record action + timestamp; sleep `dwell`. Snapshot
   `logs/current` per cycle (crash-proof fallback).
4. Append every action `{cycle, action, replicas, ts_iso}` to `.tmp/rebalance/<deployment>_cycles.json`.
5. Print a one-line-per-action heartbeat.

Defaults: `--cycles 10`, `--dwell 120` (dwell > `max.poll.interval.ms` of 60 s, long enough for one
full handover + changelog recovery to settle before the next transition). `--mode scale` primary
(`PATCH /deployments/{id}` `{"replicas": N}`); `--mode restart` fallback (`PUT …/stop` then `…/start`).

> **As-built (2026-07-13).** The 1 Hz watermark sampler was moved here from the report (§5.7 note):
> broker high-watermarks have no historical-series API and must be captured live.

### 7.6 `scripts/rebalance_report.py` — contract

```
Usage: python scripts/rebalance_report.py --deployment "Rebalance Filter (Feature)" \
       [--out .tmp/rebalance/report.csv]
```

1. Read `.tmp/rebalance/<deployment>_cycles.json` for cycle windows and
   `.tmp/rebalance/<deployment>_watermarks.csv` for the output-progress series.
2. For each cycle window: fetch deployment logs (Portal `logs/history/filter`, per-cycle
   `logs/current` snapshots as fallback) and extract the §7.3 markers; compute `lock_retry_count`,
   `max_lock_attempt`, `evictions`, `fast_revoke_seen`, `orphan_resume_seen`, `open_budget_hit`,
   `revoke_flush_timeout`.
3. From the captured watermark series compute `handover_s` and `longest_output_stall_s`.
4. Write one CSV row per cycle; print the table.

> **As-built (2026-07-13).** The report is a **pure offline correlator** — it consumes the driver's
> captured `_watermarks.csv`, it does **not** sample the broker itself (§5.7). Needs only
> `requests` + `python-dotenv`.

**Log-availability caveat (§10 Q4):** a crash-looping stable replica may make live logs unavailable.
Mitigation: prefer server-side `logs/history/filter` (retained across crashes) and fall back to the
driver's per-cycle `logs/current` snapshots. The watermark sampling is broker-side and unaffected by crashes.

## 8. Verification & evidence — pass/fail criteria

`max.poll.interval.ms = 60000 ms` is the reference budget. It is set **low** (default is 300000) so a
slow revoke/close on the stable build can plausibly exceed one poll interval given the rig's state size,
making eviction observable within the test window. Derived budgets on the **feature** build scale with it:

- **Bounded revoke flush** (sink + producer) = `60000 × 0.2 = 12 s` each.
- **Per-assign open deadline** (`OpenDeadline`) = `60000 × 0.5 = 30 s` total across all partitions opened
  in one `_on_assign`.
- Open-retry budget on both builds: `open_max_retries = 10`, `open_retry_backoff = 3.0 s` (defaults,
  `RocksDBOptions`) → up to 30 s of lock-waiting per partition open; on the feature build the
  `OpenDeadline` caps the *total* per-assign open wait at 30 s regardless of per-partition retries.

### Feature build — PASS if, across all S1 cycles:

| Criterion | Threshold | Derivation |
|---|---|---|
| No crash-loop | 0 unplanned replica restarts (beyond the deliberate `2→1→2` churn) | Fast shutdown (`cancel_all_background(True)`) releases the lock in ms; the incoming open succeeds without exhausting retries → no restart. |
| Bounded lock retries | `max_lock_attempt ≤ 2` (of 10) | Lock is free within ms; the incoming owner's first attempt (or its first 3 s backoff retry) finds it free → ≤ 2 attempts. |
| Fast revoke present | `fast_revoke_seen = True` in ≥ 1 cycle | Changelog is enabled → `Checkpoint.commit(revoking=True)` skips the local flush and logs the marker. **Requires `QS_LOGLEVEL=DEBUG`** (the marker is DEBUG-level). |
| Bounded output stall | `longest_output_stall_s < 60 s` (target < 30 s) | Handover + changelog replay must complete within one `max.poll.interval.ms` (60 s) or the consumer would be evicted; < 60 s == "no eviction". 30 s = 0.5× budget == the `OpenDeadline` cap == healthy target. |
| No eviction | `evictions = 0` | Consequence of the above. |
| Budgets not exhausted | `open_budget_hit = False` and `revoke_flush_timeout = False` in steady S1 | With fast shutdown + fast revoke, locks free in ms and flushes finish well under 12 s, so the 30 s open deadline and 12 s flush timeouts are never reached. Their *presence* means contention reached the bound — still safe (bounded, not a livelock) but investigate load/tuning. |

### Stable build — EXPECTED to reproduce the degraded behavior under identical choreography:

Expected signature (any one of these in ≥ 1 cycle counts as "reproduced"):
- an eviction (`evictions ≥ 1`) followed by a lock-retry storm on the reassigned partition
  (`lock_retry_count` large; `max_lock_attempt` approaching 10 **via the DEBUG `attempt=N` backfill** —
  the stable WARNING carries no attempt number, so gauge storm depth by `lock_retry_count` if DEBUG is
  unavailable); or
- a replica restart attributable to an open-lock failure (crash-loop: ≥ 2 restarts within a cycle); or
- `longest_output_stall_s ≥ 60 s` (≥ one poll interval).
- `fast_revoke_seen = False` in all cycles (pre-fix has no fast revoke — a sanity check the correct pin ran).

> **As-built (2026-07-13) — correction (a).** The stable lock-retry WARNING has no `(attempt N/M)`
> suffix; `max_lock_attempt` for the stable build is backfilled from the DEBUG `attempt=N` line, hence
> the DEBUG requirement. When DEBUG is off, use `lock_retry_count` as the storm gauge.

**Inconclusive-rig guard (no fudging):** if the stable build shows **none** of the above signature
across all cycles, the run is **INCONCLUSIVE**, not a pass. The choreography failed to create contention.
Escalate by (in order): confirm state actually reached the warm-up target; confirm partitions are split
across both replicas (not all on one); increase `VALUE_PADDING_BYTES` / `KEY_SPACE` for more compaction
debt; lower `MAX_POLL_INTERVAL_MS`; shorten `--dwell` to churn before compaction catches up.

### S2 (graceful stop while lock-waiting) — evidence

- Operator stops the whole deployment while the survivor is mid open-retry (lock-retry lines currently
  streaming).
- **Feature PASS (crisp):** the deployment reaches **Stopped with no error status and no crash-loop
  restart**; the process exits cleanly (`RocksDBOpenAborted` aborts the retry backoff via
  `stop_event.wait()`), within ≈ one `open_retry_backoff` (~3 s) of the stop signal; **no** further
  `cannot acquire a lock` lines after the stop timestamp; the pre-stop checkpoint/commit is preserved
  (no data loss on the next start).
- **Stable contrast:** continues emitting retry warnings and takes up to the remaining retry budget
  (up to ~30 s) to exit, and/or exits with an error.

### S3 (orphaned recovery-pause resume) — evidence (observational)

- `orphan_resume_seen = True` (`Resuming data partitions paused for recovery: [...]`) in any cycle
  where a rebalance lands during recovery. Non-appearance is **not** a failure — this path is hard to
  force deterministically; it is reported if triggered.

## 9. Alternatives considered

- **Reuse `raw-events` directly (no new topic/generator).** The brief's stated preference. Rejected as
  the default because the cross-replica handover requires ≥ 2 input partitions and `raw-events`' partition
  count cannot be assumed or modified. Kept as the cheaper path in §10 Q1 if `raw-events` is confirmed to
  have ≥ 4 partitions.
- **Emit on status change (mirroring the dedup filters).** Rejected as-built — the generator bakes status
  into the key, so per-key status is constant and on-change output goes silent after warm-up, defeating
  the stall metric. Filters emit every message instead (§5.5 note).
- **Deployment restart (rolling) as the churn mechanism.** Simpler, but less sharply targeted than replica
  scaling for the cross-replica lock handover. Kept as the `--mode restart` fallback (§7.5, §10 Q2).
- **A third pre-hardening variant pinned to `0103ccb1`.** Would isolate the base fix from the hardening,
  but adds a deployment and a matrix column for little marginal signal now that the full build is
  deployable. Dropped to keep the matrix small (two builds).
- **`group_by("order_id")` (mirroring `dedup-filter-stable`).** Adds a repartition topic and an extra layer
  between source partitions and state partitions. Dropped — direct keying gives the same partitioned-state
  pressure with fewer moving parts.
- **Sampling the output high-watermark in the report.** Rejected as-built — a broker high-watermark has no
  historical-series API, so it must be sampled live; sampling lives in the churn driver (§5.7 note).
- **Measuring output stall from message timestamps.** Quix topics use CreateTime (the generator's
  timestamp), which does not reflect processing progress. Rejected in favor of 1 Hz broker high-watermark
  sampling, which measures actual output progress and is crash-robust.
- **Deploy stable as a Job to preserve logs of a crash-looping service** (per Quix docs). Rejected as the
  default because Jobs do not fit the rolling churn; mitigated instead by server-side `logs/history/filter`
  + per-cycle incremental capture (§7.6). Available as a manual fallback if logs are lost.

## 10. Open questions

- **Q1 — `raw-events` partition count.** If the target workspace's `raw-events` already has ≥ 4 partitions,
  ArchDev may drop `rebalance-raw-events` + `Rebalance Generator` and point both filters at `raw-events`
  with their own consumer groups (the brief's preferred, cheaper path). Confirm partition count in the
  Portal before choosing. Default spec assumes it is unknown/low → new 8-partition topic.
  *As-built (2026-07-13): moot — the new 8-partition topic was used (spec choice).*
- **Q2 — Portal API replica-set capability + exact endpoints.** *As-built (2026-07-13): RESOLVED against
  `…/swagger/v2/swagger.json`.* Scale = `PATCH /deployments/{id}` body `{"replicas": N}`
  (`DeploymentPatchRequestV2.replicas`) → `--mode scale` is primary; stop/start =
  `PUT /deployments/{id}/stop|start` → `--mode restart` fallback. Logs =
  `GET /deployments/{id}/logs/history/filter?start=<epoch_s>&end=<epoch_s>&limit=N` (windowed;
  `start`/`end` are epoch **seconds**, `entries[].timestamp` is epoch **nanoseconds**) and
  `GET /deployments/{id}/logs/current` (full text). Replicas = `GET /deployments/{id}/replicas`
  (`["0","1",…]`). List = `GET /deployments?workspaceId=<ws>` (400 without the param).
- **Q3 — `quix.yaml` schema placement.** *As-built (2026-07-13): RESOLVED.* Canonical flat form
  `resources: { cpu, memory, replicas }` (the draft's nested-under-`limits` placement was wrong and would
  be silently ignored — see §7.4 note). Topic partitions = `configuration: { partitions: 8 }`
  (`TopicConfiguration`). Fallback: nested `limits` form + set replicas in the Portal UI.
- **Q4 — Crash-loop log availability.** *As-built (2026-07-13): mitigated.* The report prefers server-side
  `logs/history/filter` (retained across crashes) with the driver's per-cycle `logs/current` snapshots as
  fallback; watermark sampling is broker-side and unaffected.
- **Q5 — Warm-up throughput.** Actual cloud throughput sets the warm-up duration; the 300 MB gate is
  throughput-independent but confirm it is reachable within a reasonable operator session at
  `SLEEP_SECONDS=0.0001`. The driver can enforce it via `--warmup-bytes`.

## 11. References

- Architecture (as-built, authoritative for confirmed API details + rationale): `./architecture.md`.
- Analysis + **committed log-reference table**: `C:\repos\quix-streams_revire\docs\rocksdb-lock-contention-analysis.md`
  (three stages, hardening section, message table) — authoritative source for feature-build marker text.
- Feature source (committed HEAD `eb10421c`): `quixstreams/state/rocksdb/partition.py`
  (`_init_rocksdb` open-retry + `OpenDeadline` ~L995-1061, `close()` cancel/benign ~L570-600),
  `quixstreams/checkpointing/checkpoint.py` (`commit(revoking=True)` fast-revoke + bounded sink/producer
  flush ~L272-395), `quixstreams/state/recovery.py` (`resume_reassigned_data_partitions` ~L491-497).
  `RocksDBOptions` defaults: `open_max_retries=10`, `open_retry_backoff=3.0` (`options.py` L63-64).
  Stable lock-retry WARNING (path-less, no attempt suffix): `5e248ef3` `partition.py` ~L1539.
- Pins: feature `eb10421cbcc753045344808f348148a7f49bcd9c`; stable `5e248ef338aec2587ae9e9ce5b0c2551c3409524`
  (as pinned by `dedup-filter/requirements.txt`).
- Patterns to copy: `dedup-filter-stable/main.py` (RocksDBOptions env knobs, logger thread, `_iter_partitions`),
  `dedup-filter-stable/build/Dockerfile`, `duplicate-key-generator/main.py`, `scripts/portal_api.py`,
  `scripts/changelog_stats.py` (Kafka consumer + watermark pattern), `dev-planning/state-recovery-test/spec.md`
  + `runbook.md` (style).
- Prior spec: `dev-planning/state-recovery-test/spec.md`.
```
