import inspect
import os
import threading
import time
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

# ---------------------------------------------------------------------------
# Canonical dedup build — one file for all three dedup services. Behavior is
# driven entirely by Portal ENV VARS (no code push/rebuild to reconfigure):
#
#   TTL_MODE   : "0"/off  -> legacy SEEDER. No ttl= writes, no legacy_records_ttl.
#                           quix-streams stays inert (Rule 1) -> byte-identical
#                           to v3.23.6: un-stamped legacy records, no
#                           __ttl_stamped__ header on the changelog.
#                "1"/on   -> TTL BUILD. ttl= writes + legacy_records_ttl set ->
#                           first write flips + backfills the legacy records.
#   CG_PREFIX  : consumer-group prefix (per service, e.g. "dedup-filter").
#   CG_VERSION : consumer-group suffix (e.g. "v1"). Bump for a fresh store.
#   STATE_TTL_SECONDS / LEGACY_RECORDS_TTL_SECONDS : per-write ttl= and the
#                one-time legacy backfill TTL.
#   LOGGER     : "on"/"off". off disables the periodic status logger (skips its
#                per-interval O(keys) rocksdb scan) — set off in production AND
#                for any timing run, where the scan contends with what you time.
#
# Drain-rate knobs (tombstone sweep throughput = evictions / commit_interval):
#   MAX_EVICTIONS_PER_FLUSH : cap on TTL evictions per checkpoint. Default 10_000.
#                Present in BOTH release/v3.24.0 and this build, so unconditional.
#                MEASURED: 70_000 @ commit_interval=0.001 gives ~89k evictions/s
#                with a worst-case checkpoint of 1.05s (0.35% of a 300s poll
#                interval) — safe. The 10_000 default only sustains ~300/s once
#                checkpoints grow, which loses the race against expiry.
#   COMMIT_INTERVAL : seconds between checkpoints. Default 5.0. Lowering this is
#                preferable to raising the eviction cap: same rate, but each
#                unguarded prepare() step stays small. Below ~the fixed cost of a
#                commit (producer flush barrier + offset commit) the interval
#                stops throttling and checkpoints just run back-to-back.
#
# RocksDB tuning (ROCKSDB_*/BLOCK_CACHE_SIZE): UNSET -> inherit library defaults
#   (64 MiB write buffer, 64 MiB target file, 3 buffers, 128 MiB block cache).
#   Set them only to deliberately stress flush/compaction at small scale — small
#   values make a delete-heavy drain look far worse than production. NOTE for
#   extrapolation: point-get throughput tracks block-cache hit rate, so a rate
#   measured on a small fixture (large cache-to-data ratio) is OPTIMISTIC versus
#   a huge store. To project honestly, shrink BLOCK_CACHE_SIZE so the test's
#   cache-to-data ratio matches the target, or measure at several ratios.
#
# The quixstreams build is pinned ONLY in requirements.txt. The deployment's
# gitReference is a fixed COMMIT, so a push alone never changes what is built —
# PATCH the deployment's gitReference to the new repo commit, then start it.
# The backfill STARTED/progress/FINISHED logs come from quix-streams regardless
# of LOGGER.
# ---------------------------------------------------------------------------
def _envflag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


# The Portal happily stores an env var with an EMPTY value, and
# os.environ.get(name, "10000") then returns "" — not the default — so a bare
# int()/float() would raise ValueError at import and the service would never
# start. Treat empty/whitespace as "unset".
def _envint(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _envfloat(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


TTL_MODE = _envflag("TTL_MODE", "1")
# Sweep changelog tombstones: 1 = expired keys are also deleted from the
# changelog topic (compaction shrinks it); 0 = old behavior, local-only sweep,
# changelog keeps every record.
TTL_CHANGELOG_TOMBSTONES = _envflag("TTL_CHANGELOG_TOMBSTONES", "1")
LOGGER_ENABLED = _envflag("LOGGER", "on")
CG_PREFIX = os.environ.get("CG_PREFIX", "dedup-filter").strip()
CG_VERSION = os.environ.get("CG_VERSION", "v1").strip()
STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "30"))
LEGACY_RECORDS_TTL_SECONDS = int(os.environ.get("LEGACY_RECORDS_TTL_SECONDS", "30"))
# State dir resolution. A bare default of "state" is a relative path, so it
# lands on the container's ephemeral disk rather than the mounted state volume
# -- the store then looks empty on every restart and the app does a full
# changelog replay while reporting "Recovery successful". The platform mounts
# the volume and exports its path as Quix__Deployment__State__Path, so fall back
# to that before giving up on a relative dir. Quix__State__Dir is the deprecated
# spelling, kept for older workspaces.
STATE_DIR = (
    os.environ.get("STATE_DIR")
    or os.environ.get("Quix__Deployment__State__Path")
    or os.environ.get("Quix__State__Dir")
    or "state"
).strip()
STATE_DIR=/app/state17/run2
STATE_MGMT_ENABLED = os.environ.get("Quix__Deployment__State__Enabled", "") == "true"
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "800"))
MAX_EVICTIONS_PER_FLUSH = _envint("MAX_EVICTIONS_PER_FLUSH", 200000)
LEGACY_BACKFILL_CHUNK_SIZE = _envint("LEGACY_BACKFILL_CHUNK_SIZE", 200000)
COMMIT_INTERVAL = _envfloat("COMMIT_INTERVAL", 1.0)
CONSUMER_GROUP = f"{CG_PREFIX}-{CG_VERSION}"

# Version-tolerant options: legacy_records_ttl / ttl_changelog_tombstones are
# feature-branch additions that release/v3.24.0 (the TTL preview) predates, so
# passing them unconditionally would crash the app on that pin. Gate them on the
# installed build's actual constructor signature so the SAME harness runs on
# release/v3.24.0 (stage 2) and this build (stage 3) without a code change.
# max_evictions_per_flush exists on BOTH pins, so it needs no gate.
_supported_opts = set(inspect.signature(RocksDBOptions).parameters)
_opts_kwargs = dict(
    max_evictions_per_flush=MAX_EVICTIONS_PER_FLUSH,
)

# RocksDB tuning: only override when the env var is explicitly set, so an unset
# var inherits the library default rather than silently pinning a small value.
# Each is also signature-gated, so setting one on an older pin cannot crash.
for _env_name, _opt_name in (
    ("ROCKSDB_WRITE_BUFFER_SIZE", "write_buffer_size"),
    ("ROCKSDB_TARGET_FILE_SIZE_BASE", "target_file_size_base"),
    ("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "max_write_buffer_number"),
    ("BLOCK_CACHE_SIZE", "block_cache_size"),
):
    _raw = os.environ.get(_env_name, "").strip()
    if _raw and _opt_name in _supported_opts:
        _opts_kwargs[_opt_name] = int(_raw)

if "legacy_records_ttl" in _supported_opts:
    # Only set in TTL mode; in seeder mode it stays None (inert, Rule 1).
    _opts_kwargs["legacy_records_ttl"] = (
        timedelta(seconds=LEGACY_RECORDS_TTL_SECONDS)
        if (TTL_MODE and LEGACY_RECORDS_TTL_SECONDS > 0)
        else None
    )
if "ttl_changelog_tombstones" in _supported_opts:
    _opts_kwargs["ttl_changelog_tombstones"] = TTL_CHANGELOG_TOMBSTONES
_ROCKSDB_OPTS = RocksDBOptions(**_opts_kwargs)

_PADDING = "x" * VALUE_PADDING_BYTES

# Session-only counter: in-memory, resets to empty on every restart and only
# grows from messages seen THIS run. NOT a measure of persisted state — kept
# purely as an activity signal. The real numbers come from RocksDB below.
_session_seen: set = set()

# One-shot flag so a counter failure is reported once instead of silently
# returning -1 forever (which is what hides an internal-API rename).
_counter_error_logged = False


def _log_counter_error(where: str, exc: BaseException) -> None:
    global _counter_error_logged
    if not _counter_error_logged:
        _counter_error_logged = True
        print(
            f"[STATE-SIZE-ERROR] {where} failed ({type(exc).__name__}: {exc}); "
            f"counters will report -1 from here on",
            flush=True,
        )


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _iter_partitions():
    # _state_manager.stores is {stream_id: {store_name: Store}} — two dict
    # levels, then the Store, then its partitions. (The old code stopped one
    # level short and silently hit the except branch, hence rocksdb_keys=-1.)
    #
    # Snapshot each level with list(): this runs on a background thread while a
    # rebalance can add/remove stores and partitions, and iterating a live dict
    # raises "dictionary changed size during iteration" — a SECOND cause of the
    # -1 readings, distinct from the depth bug above.
    for stream_stores in list(app._state_manager.stores.values()):
        for store in list(stream_stores.values()):
            for partition in list(store.partitions.values()):
                yield partition


def _rocksdb_est_keys() -> int:
    """RocksDB's own key estimate across partitions — cheap, persists across
    restarts. Includes tombstones / not-yet-compacted entries, so it lags
    logical deletes."""
    try:
        total = 0
        for partition in _iter_partitions():
            n = partition._db.property_int_value("rocksdb.estimate-num-keys")
            if n is not None:
                total += int(n)
        return total
    except Exception as exc:
        _log_counter_error("_rocksdb_est_keys", exc)
        return -1


def _rocksdb_exact_keys() -> int:
    """Exact count of keys in the default CF across partitions — the real
    persisted entry count. O(keys); fine at test scale, but it scans the store
    on every interval, so keep LOGGER=off for any timing run."""
    try:
        total = 0
        for partition in _iter_partitions():
            total += sum(1 for _ in partition._db.keys())
        return total
    except Exception as exc:
        _log_counter_error("_rocksdb_exact_keys", exc)
        return -1


def _periodic_status_logger():
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        est = _rocksdb_est_keys()
        exact = _rocksdb_exact_keys()
        print(
            f"[STATE-SIZE-STABLE] mode={'TTL' if TTL_MODE else 'SEED'} "
            f"cg={CONSUMER_GROUP} bytes={size} ({size/1024:.1f} KiB) "
            f"rocksdb_exact_keys={exact} rocksdb_est_keys={est} "
            f"session_seen={len(_session_seen)}",
            flush=True,
        )


if LOGGER_ENABLED:
    threading.Thread(target=_periodic_status_logger, daemon=True).start()
else:
    print("[STARTUP] LOGGER=off — periodic status logger disabled", flush=True)

# Pulled out of the f-string below: a long line with nested quotes is the one
# that keeps getting truncated on copy/paste.
_ttl_opt = getattr(_ROCKSDB_OPTS, "legacy_records_ttl", "unsupported")
_opts_ok = "legacy_records_ttl" in _supported_opts
_mode = "on" if TTL_MODE else "off"
_tombstones = "on" if TTL_CHANGELOG_TOMBSTONES else "off"

print(
    f"[STARTUP] TTL_MODE={_mode} "
    f"consumer_group={CONSUMER_GROUP} "
    f"legacy_records_ttl={_ttl_opt} "
    f"ttl_changelog_tombstones={_tombstones} "
    f"qs_opts_supported={_opts_ok}",
    flush=True,
)
print(
    f"[STARTUP-DRAIN] max_evictions_per_flush={MAX_EVICTIONS_PER_FLUSH} "
    f"commit_interval={COMMIT_INTERVAL} "
    f"=> naive_rate={MAX_EVICTIONS_PER_FLUSH / COMMIT_INTERVAL:.0f}/s "
    f"(the real rate is capped by checkpoint duration, not this ratio) "
    f"write_buffer_size={_ROCKSDB_OPTS.write_buffer_size} "
    f"target_file_size_base={_ROCKSDB_OPTS.target_file_size_base} "
    f"max_write_buffer_number={_ROCKSDB_OPTS.max_write_buffer_number} "
    f"block_cache_size={_ROCKSDB_OPTS.block_cache_size} "
    f"state_ttl_s={STATE_TTL_SECONDS} value_padding_bytes={VALUE_PADDING_BYTES}",
    flush=True,
)
# Warm/cold verdict up front: if the resolved state dir is not the volume the
# platform mounted, the store is on ephemeral disk and every restart replays the
# whole changelog -- which otherwise only shows up as a puzzling recovery on a
# deployment that "has state".
_platform_state_path = os.environ.get("Quix__Deployment__State__Path")
_warm = STATE_MGMT_ENABLED and STATE_DIR == _platform_state_path
print(
    f"[STARTUP-STATE] state_dir={STATE_DIR!r} "
    f"platform_state_path={_platform_state_path!r} "
    f"state_mgmt_enabled={STATE_MGMT_ENABLED} "
    f"=> {'WARM (state persists)' if _warm else 'COLD (state is EPHEMERAL - full changelog replay every restart)'}",
    flush=True,
)

app = Application(
    consumer_group=CONSUMER_GROUP,
    state_dir=STATE_DIR,
    rocksdb_options=_ROCKSDB_OPTS,
    commit_interval=COMMIT_INTERVAL,
)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    new_status = value["status"]
    order_id = value["order_id"]
    stored = state.get("entry")
    stored_status = stored["status"] if stored else None

    if stored_status == new_status:
        return False

    if TTL_MODE:
        # TTL build: ttl= write. On a populated legacy store the first such
        # write flips the partition and backfills the pre-existing records.
        state.set(
            "entry",
            {"status": new_status, "pad": _PADDING},
            ttl=timedelta(seconds=STATE_TTL_SECONDS),
        )
    else:
        # Seeder: plain un-stamped write, no ttl= (legacy state to migrate).
        state.set("entry", {"status": new_status, "pad": _PADDING})
    _session_seen.add(order_id)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
