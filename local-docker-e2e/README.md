# Local docker E2E — TTL legacy→TTL migration harness

Fully-local, self-asserting reproduction of the three Quix Cloud TTL-migration
deployment tests, against a local single-node Kafka and a build of `quixstreams`
taken from the **local working tree** (bind-mounted, no PyPI / git SHA).

- **A — full migration:** seed legacy state, migrate (first `ttl=` write
  flips+backfills the whole store), warm restart, assert zero drops.
- **B — kill mid-backfill + COLD restore:** SIGKILL mid chunk-loop, wipe the
  volume, restart; recovery replays the full mixed changelog and completes.
- **C — kill mid-backfill + WARM restart (C1):** SIGKILL mid chunk-loop, restart
  on the **same** volume; assert the C1 `RESUME STARTED` / `RESUME COMPLETED`
  lines and that every leftover legacy record is stamped exactly once.

Never touches Quix Cloud, the Quix dev broker, or the cloud Duplicate Key
Generator. Outputs are a per-scenario `PASS`/`FAIL` line and a CI exit code.

## Prerequisites

- Docker Desktop (tested with 29.2.1) on Windows; `confluentinc/cp-kafka:7.6.1`
  pulled locally.
- Python 3 on the host (only the stdlib is used by the runner).
- The `quixstreams` working tree at `C:\repos\quix-streams-Main` (bind-mounted
  read-only). Override with `--lib-path` or `QS_LIB_PATH`.

The app image bakes only `quixstreams`' third-party deps; `quixstreams` itself is
put on `PYTHONPATH` from the read-only mount at container start, so "edit library
→ re-run" needs no image rebuild.

## Run

```powershell
# from the harness folder
python runner\run.py --scenario all      # A, B, C in order
python runner\run.py --scenario A        # a single scenario
python runner\run.py --scenario C --keep # keep the state volume on failure
```

Options: `--skip-build` (reuse the app image), `--lib-path <path>` (override the
quix-streams host path), `--kafka-timeout <s>`.

Exit codes: `0` all requested scenarios passed · `1` an assertion failed · `2`
harness/infra error (broker down, kill window missed, inspector broke).

Expected runtime: each scenario ~40–90 s (container start/stop dominates); the
whole suite well under ~5 min. kafka is brought up once and left running for
reuse; each scenario is isolated (fresh `CG_VERSION` `v_a/v_b/v_c`, fresh state
volume, deleted topics).

## Output

Per scenario, `PASS X (Ns)` or `FAIL X: <first unmet condition> (Ns)`, followed
by informational census/notes, then a summary block. Example:

```
PASS A  (58s)
    - seed census totals={'unstamped': 5000, 'stamped': 0, ...}
    - A post-migrate census totals={'total_default_keys': 5000, 'stamped': 5000, ...}
    - A restart census totals={'total_default_keys': 5000, 'stamped': 5000, 'unstamped': 0, ...}
```

### Reading a failure

`FAIL C: C warm restart: RESUME STARTED not seen (pre-C1 tree?)` means the
running library tree does not emit the C1 resume log lines — expected on the
**pre-C1** tree (this is scenario C's regression pin). Re-run C once the C1 fix
has landed in `quix-streams-Main`.

## The `inspect` state census (JSON)

`inspect/inspect_state.py` opens each RocksDB store under the state volume and
prints one JSON object: per partition `total_default_keys`, `stamped`,
`unstamped`, `ttl_enabled_flag`, `migration_done_marker`, `backfill_ledger_count`,
`backfill_pending_count`, `expiry_histogram`, plus roll-up `totals`. It reuses
the library's own CF names and stamp decoder — it never re-implements the byte
layout. Default access is read-only; the runner uses a WAL-replaying copy for the
mid-SIGKILL census so the live volume stays pristine for a warm restart.

## Layout

```
docker-compose.yml   kafka (long-lived) + app image build def
Dockerfile           bakes quixstreams' deps; library mounted at runtime
entrypoint.sh        PYTHONPATH=/quix-streams (ro-mount-safe editable install)
app/main.py          harness copy of dedup-filter/main.py + 2 env plumbing changes
seeder/seeder.py     local deterministic producer (--seed / --trigger)
inspect/inspect_state.py   one-shot RocksDB census -> JSON
runner/run.py        host orchestrator + exit codes
runner/scenarios.py  A/B/C step lists + assertions
runner/docker_ctl.py docker subprocess wrappers + log-follow/kill
config/defaults.env  sizing + topic + TTL defaults
```
