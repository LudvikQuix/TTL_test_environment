"""Scenario definitions + assertions for the TTL-migration E2E harness (spec 10).

Three scenarios share the phase primitives of spec 5.4 (clean / seed / migrate /
kill / cold-wipe / restart / inspect+assert). Each declares required log lines,
forbidden log lines, and RocksDB-census predicates; all must hold => PASS.

Asserted log strings (stable prefixes, robust to arg values) — spec 7.4:
  STARTED   quixstreams/state/rocksdb/partition.py:1545
  PROGRESS  partition.py:1614   FINISHED partition.py:1628
  FLIP      transaction.py:711  REPLAY-FLIP partition.py:351
  COLD-COMPLETE partition.py:614
  RESUME-START / RESUME-COMPLETED  (C1 fix; absent on the pre-C1 tree => C fails)
Forbidden:
  Fail-safe TTL read: (transaction.py:414) — a flipped store holding an
    un-decodable value == leftover un-stamped record (the C1-bug symptom).
  "silently deleting" (partition.py:666) — sentinel-fallback completion WARN.
"""

import re

import docker_ctl as dc

# --------------------------------------------------------------------------
# Log-line patterns (spec 7.4)
# --------------------------------------------------------------------------
STARTED = r"TTL legacy backfill STARTED:"
PROGRESS = r"TTL legacy backfill progress:"
FINISHED = r"TTL legacy backfill FINISHED:"
FLIP = r"Backfilled (\d+) legacy records and flipped state store"
REPLAY_FLIP = r"Recovery: __ttl_stamped__ header on default-CF replay; flipping"
COLD_COMPLETE = r"Recovery: completing interrupted legacy-TTL migration at"
RESUME_START = r"TTL legacy backfill RESUME STARTED:"
RESUME_DONE = r"TTL legacy backfill RESUME COMPLETED:"
FAILSAFE_READ = r"Fail-safe TTL read:"
SILENTLY_DELETING = r"silently deleting"

CG_VERSIONS = {"A": "v_a", "B": "v_b", "C": "v_c"}
KEY_PREFIX = "order-"


# --------------------------------------------------------------------------
# Assertion accumulator
# --------------------------------------------------------------------------
class Checks:
    def __init__(self, scenario):
        self.scenario = scenario
        self.failures = []
        self.notes = []

    def require(self, cond, desc):
        if not cond:
            self.failures.append(desc)
        return bool(cond)

    def note(self, msg):
        self.notes.append(msg)

    @property
    def passed(self):
        return not self.failures

    @property
    def first_failure(self):
        return self.failures[0] if self.failures else None


def _has(lines, pattern):
    rx = re.compile(pattern)
    return any(rx.search(line) for line in lines)


def _flip_count(lines):
    rx = re.compile(FLIP)
    for line in lines:
        m = rx.search(line)
        if m:
            return int(m.group(1))
    return None


# --------------------------------------------------------------------------
# Env builders
# --------------------------------------------------------------------------
_APP_KEYS = (
    "BROKER_ADDRESS", "input", "output", "CG_PREFIX", "STATE_DIR",
    "VALUE_PADDING_BYTES", "LOGGER", "STATE_SIZE_LOG_INTERVAL",
)


def _base_env(cfg):
    return {k: cfg[k] for k in _APP_KEYS if k in cfg}


def seed_env(cfg, cg_version):
    env = _base_env(cfg)
    env.update(
        TTL_MODE="0",
        CG_VERSION=cg_version,
        QUIXSTREAMS_STATE_LOG_LEVEL="INFO",
    )
    return env


def migrate_env(cfg, cg_version):
    env = _base_env(cfg)
    env.update(
        TTL_MODE="1",
        CG_VERSION=cg_version,
        STATE_TTL_SECONDS=cfg["STATE_TTL_SECONDS"],
        LEGACY_RECORDS_TTL_SECONDS=cfg["LEGACY_RECORDS_TTL_SECONDS"],
        LEGACY_BACKFILL_CHUNK_SIZE=cfg["LEGACY_BACKFILL_CHUNK_SIZE"],
        QUIXSTREAMS_STATE_LOG_LEVEL="DEBUG",
    )
    return env


def seeder_env(cfg):
    return {
        "BROKER_ADDRESS": cfg["BROKER_ADDRESS"],
        "input": cfg["input"],
        "output": cfg["output"],
    }


# --------------------------------------------------------------------------
# Phase primitives (spec 5.4)
# --------------------------------------------------------------------------
def clean(cfg, cg_version):
    cg = f"{cfg['CG_PREFIX']}-{cg_version}"
    dc.volume_recreate()
    dc.delete_topics([cg, cfg["input"], cfg["output"]])


def seed(cfg, cg_version, checks):
    """Run the seeder app (legacy mode), produce SEED_N distinct keys, wait
    until all N are persisted, graceful-stop, then census-assert legacy state."""
    n = int(cfg["SEED_N"])
    dc.run_app(seed_env(cfg, cg_version))
    follower = dc.LogFollower(dc.APP_CONTAINER)
    # process alive (daemon logger ticked at least once)
    dc.wait_for_pattern(follower, r"\[STATE-SIZE-STABLE\]", 30)
    dc.run_seeder(
        ["--seed", "--count", str(n), "--key-prefix", KEY_PREFIX],
        seeder_env(cfg),
    )
    reached = dc.wait_for_pattern(
        follower, rf"rocksdb_exact_keys={n} ", int(cfg["SEED_TIMEOUT_S"])
    )
    follower.stop()
    dc.stop_app()
    if reached is None:
        checks.require(False, f"seed: never reached rocksdb_exact_keys={n}")
        return None
    census = dc.run_inspector(seeder_env(cfg), access="ro")
    t = census["totals"]
    checks.require(t["unstamped"] == n, f"seed census: unstamped={t['unstamped']} != {n}")
    checks.require(t["stamped"] == 0, f"seed census: stamped={t['stamped']} != 0")
    part = census["partitions"][0] if census["partitions"] else {}
    checks.require(
        part.get("ttl_enabled_flag") is False,
        f"seed census: ttl_enabled_flag={part.get('ttl_enabled_flag')} != false",
    )
    checks.note(f"seed census totals={t}")
    return census


def _run_trigger(cfg):
    dc.run_seeder(["--trigger", "--count", str(int(cfg["TRIGGER_M"]))], seeder_env(cfg))


def start_migrate(cfg, cg_version):
    dc.run_app(migrate_env(cfg, cg_version))
    return dc.LogFollower(dc.APP_CONTAINER)


# --------------------------------------------------------------------------
# Scenario A — full migration (manual "v9.0")
# --------------------------------------------------------------------------
def scenario_a(cfg):
    cg = CG_VERSIONS["A"]
    n = int(cfg["SEED_N"])
    checks = Checks("A")

    clean(cfg, cg)
    if seed(cfg, cg, checks) is None:
        return checks

    # Migrate: cold-wipe local (changelog in kafka survives) -> rebuild populated
    # legacy from changelog -> trigger flips+backfills the whole store -> FINISH.
    # (spec 10/12 A cold-wipe before migrate; overrides the 5.4 "(B only)" note.)
    dc.volume_recreate()
    follower = start_migrate(cfg, cg)
    _run_trigger(cfg)
    started = dc.wait_for_pattern(follower, STARTED, int(cfg["PHASE_TIMEOUT_S"]))
    finished = dc.wait_for_pattern(follower, FINISHED, int(cfg["PHASE_TIMEOUT_S"]))
    flip = dc.wait_for_pattern(follower, FLIP, int(cfg["PHASE_TIMEOUT_S"]))
    follower.stop()
    dc.stop_app()  # graceful -> durable flip + done-marker
    checks.require(started is not None, "A migrate: STARTED not seen")
    checks.require(finished is not None, "A migrate: FINISHED not seen")
    # The "Backfilled N" log count is NOT equal to the seeded count: live traffic
    # re-writes some keys with their OWN ttl= before the flip (self-stamped, so
    # never backfilled). The correct invariant is 0 < N <= seeded_count; the
    # authoritative reconciliation is the census (stamped == total == seeded,
    # unstamped == 0), asserted by _assert_full_migrated below. (Validated on the
    # v9.0 cloud run: 90,742 backfilled + 9,256 self-stamped = 99,998.)
    fc = _flip_count(follower.lines)
    checks.require(
        fc is not None and 0 < fc <= n,
        f"A migrate: FLIP count={fc} not in (0, {n}] (Backfilled N; census is "
        f"the authoritative reconciliation)",
    )

    census = dc.run_inspector(seeder_env(cfg), access="ro")
    # Post-migrate (pre-restart): the resume ledger is still on disk (4995 keys);
    # the marker-gated cleanup drops it on the NEXT open, so do not require
    # ledger==0 here — the warm-restart census below asserts ledger==0.
    _assert_full_migrated(
        census, n, checks, "A post-migrate", require_ledger_zero=False
    )

    # Warm restart within the TTL window. A cleanly-flipped store opens directly
    # in TTL mode from the durable flag, so recovery has nothing to replay and
    # REPLAY-FLIP need NOT fire (arch-doc 254-256: __changelog_offset__ synced by
    # the final flush). The authoritative zero-drops guarantee is census
    # invariance + NO degraded-read WARN (spec 1/3). REPLAY-FLIP is informational.
    follower2 = start_migrate(cfg, cg)
    dc.wait_for_pattern(follower2, r"\[STATE-SIZE-STABLE\]", int(cfg["PHASE_TIMEOUT_S"]))
    dc.wait_for_pattern(follower2, r"\[STATE-SIZE-STABLE\]", 15)  # a second tick to settle recovery
    follower2.stop()
    dc.stop_app()
    checks.require(
        not _has(follower2.lines, FAILSAFE_READ),
        "A restart: degraded-read WARN present (Fail-safe TTL read)",
    )
    if not _has(follower2.lines, REPLAY_FLIP):
        checks.note("A restart: REPLAY-FLIP absent (expected on a clean warm open)")
    census2 = dc.run_inspector(seeder_env(cfg), access="ro")
    t2 = census2["totals"]
    checks.require(
        t2["total_default_keys"] == n and t2["stamped"] == n and t2["unstamped"] == 0,
        f"A restart: census changed (zero-drops violated) totals={t2}",
    )
    # Single-stamp integrity + deferred ledger cleanup ran on the warm open.
    checks.require(
        t2["double_wrapped"] == 0 and t2["decode_clean"] == t2["total_default_keys"],
        f"A restart: single-stamp integrity broken (double_wrapped="
        f"{t2['double_wrapped']}, decode_clean={t2['decode_clean']}) totals={t2}",
    )
    checks.require(
        t2["ledger"] == 0, f"A restart: ledger={t2['ledger']} != 0 (cleanup deferred)"
    )
    checks.note(f"A restart census totals={t2}")
    return checks


# --------------------------------------------------------------------------
# Scenario B — kill mid-backfill + COLD restore (manual "v9.2")
# --------------------------------------------------------------------------
def scenario_b(cfg):
    cg = CG_VERSIONS["B"]
    n = int(cfg["SEED_N"])
    checks = Checks("B")

    clean(cfg, cg)
    if seed(cfg, cg, checks) is None:
        return checks

    # Migrate on the warm seed volume + kill mid-backfill.
    follower = start_migrate(cfg, cg)
    _run_trigger(cfg)
    err = _kill_mid_backfill(cfg, follower)
    follower.stop()
    if err:
        raise dc.HarnessError(f"B migrate kill: {err}")
    checks.require(_has(follower.lines, STARTED), "B migrate: STARTED not seen")
    checks.require(not _has(follower.lines, FINISHED), "B migrate: FINISHED seen (kill failed)")
    mid = dc.run_inspector(seeder_env(cfg), snapshot=True)
    checks.note(f"B mid-kill census totals={mid['totals']} (informational)")

    # Cold restart: wipe local -> replay the FULL mixed changelog from scratch.
    dc.volume_recreate()
    follower2 = start_migrate(cfg, cg)
    cold = dc.wait_for_pattern(follower2, COLD_COMPLETE, int(cfg["PHASE_TIMEOUT_S"]))
    dc.wait_for_pattern(follower2, r"\[STATE-SIZE-STABLE\]", 15)
    follower2.stop()
    dc.stop_app()
    checks.require(cold is not None, "B cold restart: COLD-COMPLETE not seen")

    census = dc.run_inspector(seeder_env(cfg), access="ro")
    _assert_full_migrated(census, n, checks, "B post-restart", require_pending_zero=True)
    _assert_single_future_bucket(census, checks, "B post-restart")
    return checks


# --------------------------------------------------------------------------
# Scenario C — kill mid-backfill + WARM restart (NEW C1)
# --------------------------------------------------------------------------
def scenario_c(cfg):
    cg = CG_VERSIONS["C"]
    n = int(cfg["SEED_N"])
    checks = Checks("C")

    clean(cfg, cg)
    if seed(cfg, cg, checks) is None:
        return checks

    # Migrate on the warm seed volume + kill mid-backfill (identical to B).
    follower = start_migrate(cfg, cg)
    _run_trigger(cfg)
    err = _kill_mid_backfill(cfg, follower)
    follower.stop()
    if err:
        raise dc.HarnessError(f"C migrate kill: {err}")
    checks.require(_has(follower.lines, STARTED), "C migrate: STARTED not seen")
    checks.require(not _has(follower.lines, FINISHED), "C migrate: FINISHED seen (kill failed)")

    # Mid-kill census (WAL-replaying snapshot; live volume untouched): the resume
    # ledger must be persisted and the migration not yet marked done (spec 12 C).
    mid = dc.run_inspector(seeder_env(cfg), snapshot=True)
    midp = mid["partitions"][0] if mid["partitions"] else {}
    checks.require(
        mid["totals"]["ledger"] > 0,
        f"C mid-kill: resume ledger empty (ledger={mid['totals']['ledger']})",
    )
    checks.require(
        midp.get("migration_done_marker") is False,
        f"C mid-kill: migration_done={midp.get('migration_done_marker')} != false",
    )
    checks.note(f"C mid-kill census totals={mid['totals']}")

    # WARM restart on the SAME volume: recovery replays the produced tail, flips,
    # then resumes (C1). On the pre-C1 tree the RESUME lines never appear -> this
    # scenario FAILS (the regression pin, spec 10 C).
    follower2 = start_migrate(cfg, cg)
    replay = dc.wait_for_pattern(follower2, REPLAY_FLIP, int(cfg["PHASE_TIMEOUT_S"]))
    rstart = dc.wait_for_pattern(follower2, RESUME_START, int(cfg["PHASE_TIMEOUT_S"]))
    rdone = dc.wait_for_pattern(follower2, RESUME_DONE, int(cfg["PHASE_TIMEOUT_S"]))
    dc.wait_for_pattern(follower2, r"\[STATE-SIZE-STABLE\]", 15)
    follower2.stop()
    dc.stop_app()
    checks.require(replay is not None, "C warm restart: REPLAY-FLIP not seen")
    checks.require(rstart is not None, "C warm restart: RESUME STARTED not seen (pre-C1 tree?)")
    checks.require(rdone is not None, "C warm restart: RESUME COMPLETED not seen (pre-C1 tree?)")
    checks.require(
        not _has(follower2.lines, SILENTLY_DELETING),
        "C warm restart: SENTINEL/'silently deleting' WARN present",
    )
    checks.require(
        not _has(follower2.lines, FAILSAFE_READ),
        "C warm restart: degraded-read WARN present (leftover un-stamped)",
    )

    census = dc.run_inspector(seeder_env(cfg), access="ro")
    _assert_full_migrated(census, n, checks, "C post-restart")
    _assert_single_future_bucket(census, checks, "C post-restart")
    return checks


# --------------------------------------------------------------------------
# Shared kill + census assertions
# --------------------------------------------------------------------------
def _kill_mid_backfill(cfg, follower):
    return dc.kill_on_nth_progress(
        follower,
        STARTED, PROGRESS, FINISHED,
        n=int(cfg["KILL_AFTER_CHUNKS"]),
        kill_fn=dc.kill_app,
        timeout_s=int(cfg["KILL_TIMEOUT_S"]),
    )


def _assert_full_migrated(
    census, n, checks, phase, require_pending_zero=False, require_ledger_zero=True
):
    t = census["totals"]
    part = census["partitions"][0] if census["partitions"] else {}
    checks.require(t["total_default_keys"] == n, f"{phase}: total={t['total_default_keys']} != {n}")
    checks.require(t["stamped"] == n, f"{phase}: stamped={t['stamped']} != {n}")
    checks.require(t["unstamped"] == 0, f"{phase}: unstamped={t['unstamped']} != 0")
    # Single-stamp integrity (C1 P0, sc-73191). Every stamped value must strip to
    # its real payload; NONE may be a resume double-wrap stamp(stamp(json)).
    # double_wrapped>0 is the exact durable-corruption signature the P0 produced,
    # so this is the authoritative black-box catch for a warm-restart regression.
    checks.require(
        t["double_wrapped"] == 0,
        f"{phase}: double_wrapped={t['double_wrapped']} != 0 (resume re-wrapped an "
        f"already-stamped value)",
    )
    checks.require(
        t["decode_clean"] == t["total_default_keys"],
        f"{phase}: decode_clean={t['decode_clean']} != total_default_keys="
        f"{t['total_default_keys']} (a value did not single-decode)",
    )
    if require_ledger_zero:
        checks.require(t["ledger"] == 0, f"{phase}: ledger={t['ledger']} != 0")
    checks.require(
        part.get("ttl_enabled_flag") is True,
        f"{phase}: ttl_enabled_flag={part.get('ttl_enabled_flag')} != true",
    )
    checks.require(
        part.get("migration_done_marker") is True,
        f"{phase}: migration_done={part.get('migration_done_marker')} != true",
    )
    if require_pending_zero:
        checks.require(t["pending"] == 0, f"{phase}: pending={t['pending']} != 0")
    checks.note(f"{phase} census totals={t}")


def _assert_single_future_bucket(census, checks, phase):
    part = census["partitions"][0] if census["partitions"] else {}
    hist = part.get("expiry_histogram", {})
    stamped = part.get("stamped", 0)
    future = {k: hist.get(k, 0) for k in ("lt_1h", "h1_24", "gt_24h")}
    nonzero = [k for k, v in future.items() if v > 0]
    checks.require(hist.get("expired", 0) == 0, f"{phase}: expired={hist.get('expired')} != 0 (past-dated)")
    checks.require(hist.get("never", 0) == 0, f"{phase}: never={hist.get('never')} != 0 (sentinel)")
    checks.require(
        len(nonzero) == 1 and future[nonzero[0]] == stamped,
        f"{phase}: not a single future expiry bucket (hist={hist}, stamped={stamped})",
    )


SCENARIOS = {"A": scenario_a, "B": scenario_b, "C": scenario_c}
