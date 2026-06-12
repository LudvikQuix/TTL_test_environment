# State Recovery Test (source offset out of range)

**Status:** Draft
**Project:** TTL_test_environment
**Created:** 2026-06-12
**Planned with:** Buddy

## 1. Summary

Add two Quix Cloud services — `recovery-generator` (producer) and `recovery-filter` (stateful consumer) — that demonstrate and verify quix-streams PR #1115 ("recover state when source offset expires", merged to `main`). The input topic is created with aggressive retention (`segment.ms=60000`, `retention.ms=120000`) so that stopping the filter for a few minutes pushes its committed consumer-group offset below the broker low watermark. On restart, the new `Application` behavior fires: with `auto_recover_from_source_offset_out_of_range=True` the local state partition is destroyed and rebuilt from the chosen reset point; with `False` it raises `StateRecoveryOffsetOutOfRange`. Verification is manual: start/stop deployments via CLI and read deployment logs.

## 2. Goals

- Reproduce the `0 <= committed_offset < lowwater` trigger condition on demand in Quix Cloud within a ~10-minute manual session.
- Demonstrate all three variants: A) auto-recover with `earliest` reset (default), B) fail-fast with `AUTO_RECOVER=false`, C) auto-recover with `OFFSET_RESET=latest`.
- Provide observable evidence in logs: the `DESTRUCTIVE STATE RECOVERY` critical line, the `StateRecoveryOffsetOutOfRange` exception, and `[STATE-SIZE-RECOVERY]` state-size telemetry.
- A runbook (`dev-planning/state-recovery-test/runbook.md`) an operator can follow step by step.

## 3. Non-goals

- No automated tests, CI, or assertions — Phase 1 is build + manual run only.
- No tuning of RocksDB options (defaults are fine; state size is not the subject here).
- No changes to any existing service folder, existing quix.yaml entries, or the 7 existing topics.
- No `state_recovery_offset_reset="match"` variant.

## 4. User stories / scenarios

1. **Variant A (default auto-recover, earliest):** Operator starts both services, lets the filter accumulate per-key counts for 2 min, stops only the filter, waits for retention to delete rolled segments (≥4 min), restarts the filter. The filter logs a CRITICAL `DESTRUCTIVE STATE RECOVERY` message, deletes local state, resumes from the low watermark, and `[STATE-SIZE-RECOVERY]` shows state size dropping to near zero before regrowing.
2. **Variant B (fail-fast):** Same procedure with `AUTO_RECOVER=false` on the filter. On restart the filter logs an ERROR ("State cannot be recovered safely...") and crashes with `StateRecoveryOffsetOutOfRange`. Local state is NOT deleted.
3. **Variant C (latest):** Same procedure with `AUTO_RECOVER=true`, `OFFSET_RESET=latest`. State is destroyed, consumption resumes at the high watermark (skipping retained backlog), counts restart from 1.

## 5. Proposed design

Mirror the existing `duplicate-key-generator/` (producer) and `dedup-filter/` (stateful consumer + `[STATE-SIZE]` logger thread) patterns. The critical new pieces:

- **Topic with aggressive retention, created by the app.** Kafka only advances the low watermark when retention deletes *rolled* segments, so the input topic must carry `segment.ms=60000` (roll the active segment every minute) and `retention.ms=120000` (delete rolled segments older than 2 min). Both apps declare the topic via `app.topic(..., config=TopicConfig(num_partitions=1, replication_factor=1, extra_config={...}))` so start order does not matter (quixstreams `auto_create_topics` defaults to True).
  - **NOTE — API correction vs. brief:** on `main`, the `Application.topic()` kwarg is `config=`, not `create_config=` (`quixstreams/app.py` line 472; it is forwarded internally as `create_config` to the topic manager at line 541). Import: `from quixstreams.models import TopicConfig` (re-exported from `quixstreams/models/topics/topic.py`, `__all__` line 30).
- **Filter wiring of the feature kwargs** (`quixstreams/app.py` lines 151–152): `auto_recover_from_source_offset_out_of_range` from env `AUTO_RECOVER`, `state_recovery_offset_reset` from env `OFFSET_RESET`, plus `auto_offset_reset="earliest"`. Trigger condition at `app.py` line 1134 (`0 <= tp.offset < lowwater`, checked in `_on_assign`).
- **Persistent state volume** on the filter deployment (`state: enabled: true, size: 1`) so local RocksDB state survives the stop/restart — required so "state was destroyed" is meaningful evidence.

## 6. Sub-features / work breakdown

### 6.1 `recovery-generator/` service — owner: ArchDev

Produces forever to topic env `output` (default `recovery-input`):

- Message value (JSON): `{"seq": <int>, "ts": <epoch ms>, "pad": "<PADDING_BYTES of 'x'>"}`.
- Message key: `f"key-{seq % KEY_COUNT}"` (string, default key space 1000).
- Rate: `RATE_PER_SECOND` (default 100) via `time.sleep(1 / RATE_PER_SECOND)` per message.
- Declares the output topic with `TopicConfig(num_partitions=1, replication_factor=1, extra_config={"segment.ms": "60000", "retention.ms": "120000"})`.
- Logs `[GEN] produced=<n>` every 10 s (count-based or a timer; either is fine, must be wall-clock ~10 s).
- Pattern: `duplicate-key-generator/main.py` (`load_dotenv()` before quixstreams imports, `app.get_producer()` loop, `output_topic.serialize(...)`).

File tree:

```
recovery-generator/
├── main.py
├── requirements.txt        # quixstreams @ git+https://github.com/quixio/quix-streams.git@main, python-dotenv
├── app.yaml
├── .env.example
└── build/
    └── dockerfile          # copy of dedup-filter/build/dockerfile
```

### 6.2 `recovery-filter/` service — owner: ArchDev

Stateful consumer, consumer group `recovery-filter-v1`:

- Input topic env `input` (default `recovery-input`) — declared with the SAME `TopicConfig` as the generator (identical config so whichever app starts first creates it correctly).
- Output topic env `output` (default `recovery-output`), default config.
- Per message (use `sdf.apply(fn, stateful=True, metadata=True)` so the key is available for the live-key set):
  - `count = state.get("count", 0) + 1; state.set("count", count)`
  - enrich: `value["count"] = count`, produce to output.
  - add the message key to an in-memory `set` for the live-key telemetry.
- `Application(...)` kwargs:
  - `consumer_group="recovery-filter-v1"`
  - `state_dir=STATE_DIR`
  - `auto_offset_reset="earliest"`
  - `auto_recover_from_source_offset_out_of_range=AUTO_RECOVER` (env string → bool: `os.environ.get("AUTO_RECOVER", "true").lower() == "true"`)
  - `state_recovery_offset_reset=OFFSET_RESET` (env string, one of `earliest|latest|match`)
- Periodic logger thread: copy `dedup-filter/main.py` lines 34–80 (`_dir_size_bytes` walk + `rocksdb.estimate-num-keys` sum), tag changed to `[STATE-SIZE-RECOVERY]`, `live_entries` = size of the in-memory key set (no TTL pruning needed). Interval env `STATE_SIZE_LOG_INTERVAL`, default 10.

File tree: same shape as 6.1 (`main.py`, `requirements.txt`, `app.yaml`, `.env.example`, `build/dockerfile`).

### 6.3 `app.yaml` for both services — owner: ArchDev

Copy field structure from `dedup-filter/app.yaml` (`name`, `language: python`, `variables` with `inputType`/`defaultValue`/`required`, `dockerfile: build/dockerfile`, `runEntryPoint: main.py`, `defaultFile: main.py`). Variables per the env-var tables in §7.

### 6.4 `quix.yaml` additions — owner: ArchDev

Append the two deployments and two topics in §7.3 verbatim. Do not modify existing entries.

### 6.5 Runbook — owner: ArchDev (content provided verbatim in §7.4)

Write `dev-planning/state-recovery-test/runbook.md` with the content in §7.4.

Dependencies: 6.2 references nothing from 6.1 at runtime besides the shared topic; both must agree on the `TopicConfig`. 6.4 depends on the folder names from 6.1/6.2. No Tester/FrontEndEsthetic involvement (Phase 1, manual QA in Quix Cloud).

## 7. Data & interface contracts

### 7.1 Env vars — `recovery-generator`

| Env var | Default | Maps to |
|---|---|---|
| `output` | `recovery-input` | output topic name (`app.topic(...)`) |
| `RATE_PER_SECOND` | `100` | messages/sec via per-message sleep |
| `PADDING_BYTES` | `200` | length of `"x" * n` in `value["pad"]` |
| `KEY_COUNT` | `1000` | key space: `key-{seq % KEY_COUNT}` |
| `Quix__Sdk__Token` | — (local only, via `.env`) | Quix broker auth when run locally |

### 7.2 Env vars — `recovery-filter`

| Env var | Default | Maps to |
|---|---|---|
| `input` | `recovery-input` | input topic name |
| `output` | `recovery-output` | output topic name |
| `AUTO_RECOVER` | `true` | `Application(auto_recover_from_source_offset_out_of_range=...)` (bool) |
| `OFFSET_RESET` | `earliest` | `Application(state_recovery_offset_reset=...)` (`earliest`/`latest`/`match`) |
| `STATE_DIR` | `state` | `Application(state_dir=...)` |
| `STATE_SIZE_LOG_INTERVAL` | `10` | seconds between `[STATE-SIZE-RECOVERY]` lines |
| `Quix__Sdk__Token` | — (local only, via `.env`) | Quix broker auth when run locally |

### 7.3 `quix.yaml` additions (verbatim)

Append to `deployments:`:

```yaml
  - name: Recovery Generator
    application: recovery-generator
    version: latest
    deploymentType: Service
    resources:
      limits:
        cpu: 200
        memory: 200
    desiredStatus: Stopped
    variables:
      - name: output
        inputType: OutputTopic
        description: Topic to produce recovery-test messages to. Created by the app with segment.ms=60000 / retention.ms=120000.
        required: true
        value: recovery-input
      - name: RATE_PER_SECOND
        inputType: FreeText
        description: Messages per second (sleep-paced).
        value: 100
      - name: PADDING_BYTES
        inputType: FreeText
        description: Bytes of 'x' padding per message so segments fill at a predictable rate.
        value: 200
      - name: KEY_COUNT
        inputType: FreeText
        description: Key space size; keys are key-{seq % KEY_COUNT}.
        value: 1000
  - name: Recovery Filter
    application: recovery-filter
    version: latest
    deploymentType: Service
    resources:
      limits:
        cpu: 200
        memory: 500
    state:
      enabled: true
      size: 1
    desiredStatus: Stopped
    variables:
      - name: input
        inputType: InputTopic
        description: Topic with recovery-test messages (aggressive retention).
        required: true
        value: recovery-input
      - name: output
        inputType: OutputTopic
        description: Topic with count-enriched messages.
        required: true
        value: recovery-output
      - name: AUTO_RECOVER
        inputType: FreeText
        description: Maps to auto_recover_from_source_offset_out_of_range. true = destroy + rebuild state when committed offset < low watermark; false = raise StateRecoveryOffsetOutOfRange.
        value: true
      - name: OFFSET_RESET
        inputType: FreeText
        description: Maps to state_recovery_offset_reset. One of earliest | latest | match.
        value: earliest
```

Append to `topics:`:

```yaml
  - name: recovery-input
  - name: recovery-output
```

Topic configuration (segment.ms/retention.ms) comes ONLY from the app's `TopicConfig` — the quix.yaml entries list the names, nothing else (per the existing pattern; see §8 risk R2).

### 7.4 Runbook content (verbatim, for `dev-planning/state-recovery-test/runbook.md`)

```markdown
# State Recovery Test — Runbook

Topic contract: `recovery-input` is created by the apps with
`segment.ms=60000` (segment rolls every 60 s) and `retention.ms=120000`
(rolled segments deleted after 2 min). Generator produces ~100 msg/s.
All start/stop via Quix CLI against the dev workspace
(`quixdev-ttltestenvironment-ttlgenerator`), e.g.
`quix deployments start|stop <deployment>`.

## Variant A — default auto-recover (earliest)

1. Ensure Recovery Filter vars: `AUTO_RECOVER=true`, `OFFSET_RESET=earliest`.
2. Start **Recovery Generator** first (it creates `recovery-input` with the
   aggressive config), then start **Recovery Filter**.
3. Wait ~2 min. Confirm in filter logs: `[STATE-SIZE-RECOVERY]` lines with
   growing `bytes=` and `live_entries=` approaching 1000 (= KEY_COUNT).
4. Stop **Recovery Filter only**. Generator keeps producing.
5. Wait at least 4 min. If the restart in step 6 shows no recovery, the
   broker's retention sweep (log.retention.check.interval.ms, often 5 min)
   has not run yet — stop the filter again and wait a few more minutes.
6. Restart **Recovery Filter**. Expected log evidence, in order:
   - CRITICAL line starting `DESTRUCTIVE STATE RECOVERY: consumer group
     offset <N> for topic <ws>-recovery-input[0] is below the broker low
     watermark <L>` ... `Local state for stream ... has been deleted` ...
     `state_recovery_offset_reset=earliest`.
   - `[STATE-SIZE-RECOVERY]` `bytes=` near 0 immediately after restart,
     then regrowing; `count` values on `recovery-output` restart from 1.
   - Processing resumes from the low watermark (retained backlog is
     reprocessed).
7. PASS if the CRITICAL line appears, the app keeps running (no crash), and
   state size visibly resets.

## Variant B — fail-fast (`AUTO_RECOVER=false`)

1. Set Recovery Filter var `AUTO_RECOVER=false`. Repeat steps 2–5 above.
2. On restart, expected log evidence:
   - ERROR line: `Consumer group offset <N> for topic <ws>-recovery-input[0]
     is below the broker low watermark <L>. State cannot be recovered safely
     from retained source data. ...`
   - Traceback ending in `StateRecoveryOffsetOutOfRange`; the deployment
     crash-loops. No `DESTRUCTIVE STATE RECOVERY` line; local state is NOT
     deleted.
3. PASS if the exception is raised and the service does not silently recover.
4. Cleanup: set `AUTO_RECOVER=true` back, restart once to let it recover.

## Variant C — auto-recover to latest

1. Set `AUTO_RECOVER=true`, `OFFSET_RESET=latest`. Repeat steps 2–5 of A.
2. On restart, expected log evidence:
   - The same CRITICAL `DESTRUCTIVE STATE RECOVERY` line, ending
     `state_recovery_offset_reset=latest`, with the resume offset equal to
     the broker HIGH watermark.
   - Retained backlog is skipped: first `[GEN]`-aged messages on
     `recovery-output` after restart are fresh ones; counts restart from 1.
3. PASS if recovery fires and consumption resumes at the high watermark.

## Teardown

Stop both deployments (`desiredStatus: Stopped` is the resting state).
```

## 8. Risks, constraints, and open questions

- **R1 — Broker retention sweep lag.** Segment deletion (and thus low-watermark advance) happens only when the broker's retention thread runs (`log.retention.check.interval.ms`, commonly 5 min, not configurable per-topic). The 4-min wait in the runbook may need to stretch to ~8 min on first attempt. Mitigated in runbook step A5.
- **R2 — Quix platform may pre-create the topics from quix.yaml with default retention.** If the platform creates `recovery-input` before the app does, the app's `TopicConfig.extra_config` will not be applied (quixstreams only applies create-config on creation). ArchDev must verify after first sync: check the topic's `segment.ms`/`retention.ms` in the Quix portal; if defaults, set them via the portal/CLI topic settings manually. The test contract values (`segment.ms=60000`, `retention.ms=120000`) are non-negotiable.
- **R3 — Changelog topic is unaffected.** The filter's changelog topic keeps default (compacted) config; recovery destroys the local partition and writes through the changelog. No action needed, just don't "fix" it.
- **R4 — `state: enabled: true` is required** on the filter deployment so RocksDB state survives the stop/restart; without it the "state deleted" evidence is meaningless.
- **Constraints:** do not touch existing service folders, the 7 existing topics, or existing quix.yaml entries. Dependencies limited to `quixstreams @ git+https://github.com/quixio/quix-streams.git@main` and `python-dotenv`. `load_dotenv()` must run before any quixstreams import.
- **Open question Q1:** exact CLI verb set for start/stop in the installed Quix CLI version (runbook uses `quix deployments start|stop`; adjust to the actual CLI if it differs).

## 9. Alternatives considered

- **Manually deleting records / resetting offsets with kafka CLI tools** instead of retention-driven expiry: faster, but doesn't reproduce the real-world failure mode (retention outpacing a stopped consumer) and requires broker admin access from Quix Cloud. Rejected.
- **One combined service (producer + consumer in one app):** fewer moving parts, but the test requires stopping the consumer while production continues — impossible in one deployment. Rejected.
- **Configuring retention via quix.yaml topic entries:** the existing project pattern keeps quix.yaml topics name-only, and `segment.ms` is not expressible there; app-side `TopicConfig` keeps the contract in code. Chosen approach, with R2 as the watch-item.

## 10. References

- Feature source: `C:\repos\quix-streams-Main\quixstreams\app.py` — kwargs lines 151–152, trigger `_on_assign` lines 1116–1199 (condition line 1134), fail-fast message lines 1136–1147, CRITICAL recovery log lines 1167–1191, reset-target resolution `_resolve_source_offset_recovery_targets` lines 1061–1088. Exception: `quixstreams/state/exceptions.py` line 25.
- `TopicConfig`: `C:\repos\quix-streams-Main\quixstreams\models\topics\topic.py` lines 41–50; exported via `__all__` line 30 → `from quixstreams.models import TopicConfig`.
- Patterns to copy: `C:\repos\TTL_test_environment\dedup-filter\main.py` (logger thread lines 34–80), `C:\repos\TTL_test_environment\duplicate-key-generator\main.py`, `C:\repos\TTL_test_environment\dedup-filter\build\dockerfile`, `C:\repos\TTL_test_environment\dedup-filter\app.yaml`, `C:\repos\TTL_test_environment\quix.yaml`.
- quix-streams PR #1115 (merged to main).

## Sanity checklist

| Variant | Filter env | Operator steps | Expected log evidence | Pass criterion |
|---|---|---|---|---|
| A — default | `AUTO_RECOVER=true`, `OFFSET_RESET=earliest` | start gen → start filter → 2 min → stop filter → ≥4 min → start filter | CRITICAL `DESTRUCTIVE STATE RECOVERY ... below the broker low watermark ... state_recovery_offset_reset=earliest`; `[STATE-SIZE-RECOVERY] bytes=`~0 then regrowing | recovery fires, no crash, state reset visible, counts restart from 1 |
| B — fail-fast | `AUTO_RECOVER=false` | same | ERROR `State cannot be recovered safely from retained source data`; traceback `StateRecoveryOffsetOutOfRange`; crash loop | exception raised, state NOT deleted, no silent recovery |
| C — latest | `AUTO_RECOVER=true`, `OFFSET_RESET=latest` | same | CRITICAL recovery line ending `state_recovery_offset_reset=latest`, resume offset = high watermark | recovery fires, backlog skipped, counts restart from 1 |
