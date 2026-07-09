import os
import threading
import time
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

# ---------------------------------------------------------------------------
# HARNESS COPY of ../../dedup-filter/main.py (spec 6.1). Verbatim except two
# minimal, clearly-marked plumbing changes for the local docker E2E harness:
#   1. BROKER_ADDRESS switch: when set, talk to a local broker via
#      Application(broker_address=..., auto_offset_reset="earliest") instead of
#      the cloud Quix__Sdk__Token path.
#   2. LEGACY_BACKFILL_CHUNK_SIZE plumbed into RocksDBOptions so the runner can
#      widen the backfill kill window.
# The deployed cloud app (../../dedup-filter/main.py) is NOT modified.
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
#                per-interval O(keys) rocksdb scan) — set off in production.
#
# The image always installs the sc-73191 build (header-signal fix + OP-4 +
# backfill progress logging); the backfill STARTED/progress/FINISHED logs come
# from quix-streams and show regardless of LOGGER.
# ---------------------------------------------------------------------------
def _envflag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


TTL_MODE = _envflag("TTL_MODE", "1")
LOGGER_ENABLED = _envflag("LOGGER", "on")
CG_PREFIX = os.environ.get("CG_PREFIX", "dedup-filter").strip()
CG_VERSION = os.environ.get("CG_VERSION", "v1").strip()
STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "30"))
LEGACY_RECORDS_TTL_SECONDS = int(os.environ.get("LEGACY_RECORDS_TTL_SECONDS", "30"))
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "800"))
CONSUMER_GROUP = f"{CG_PREFIX}-{CG_VERSION}"

_ROCKSDB_OPTS = RocksDBOptions(
    write_buffer_size=int(os.environ.get("ROCKSDB_WRITE_BUFFER_SIZE", str(4 * 1024 * 1024))),
    target_file_size_base=int(os.environ.get("ROCKSDB_TARGET_FILE_SIZE_BASE", str(2 * 1024 * 1024))),
    max_write_buffer_number=int(os.environ.get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "2")),
    # HARNESS CHANGE 2: expose the one-time legacy backfill chunk size so the
    # runner can shrink it (default 50) and span the backfill over many chunks,
    # widening the deterministic mid-backfill kill window (spec 6.1 / 6.5).
    legacy_backfill_chunk_size=int(os.environ.get("LEGACY_BACKFILL_CHUNK_SIZE", "10000")),
    # Only set in TTL mode; in seeder mode it stays None (inert, Rule 1).
    legacy_records_ttl=(
        timedelta(seconds=LEGACY_RECORDS_TTL_SECONDS)
        if (TTL_MODE and LEGACY_RECORDS_TTL_SECONDS > 0)
        else None
    ),
)

_PADDING = "x" * VALUE_PADDING_BYTES

# Session-only counter: in-memory, resets to empty on every restart and only
# grows from messages seen THIS run. NOT a measure of persisted state — kept
# purely as an activity signal. The real numbers come from RocksDB below.
_session_seen: set = set()


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
    for stream_stores in app._state_manager.stores.values():
        for store in stream_stores.values():
            for partition in store.partitions.values():
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
    except Exception:
        return -1


def _rocksdb_exact_keys() -> int:
    """Exact count of keys in the default CF across partitions — the real
    persisted entry count. O(keys); fine at test scale."""
    try:
        total = 0
        for partition in _iter_partitions():
            total += sum(1 for _ in partition._db.keys())
        return total
    except Exception:
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

print(
    f"[STARTUP] TTL_MODE={'on' if TTL_MODE else 'off'} consumer_group="
    f"{CONSUMER_GROUP} legacy_records_ttl={_ROCKSDB_OPTS.legacy_records_ttl}",
    flush=True,
)

# HARNESS CHANGE 1: local-broker switch. When BROKER_ADDRESS is set (the
# harness sets kafka:9092), connect to the local broker and read from the start
# of the topic; otherwise leave the cloud Quix__Sdk__Token path untouched.
_app_kwargs = dict(
    consumer_group=CONSUMER_GROUP,
    state_dir=STATE_DIR,
    rocksdb_options=_ROCKSDB_OPTS,
)
_broker_address = os.environ.get("BROKER_ADDRESS")
if _broker_address:
    _app_kwargs["broker_address"] = _broker_address
    _app_kwargs["auto_offset_reset"] = "earliest"

app = Application(**_app_kwargs)
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
