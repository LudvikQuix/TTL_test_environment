import os
import threading
import time
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "30"))
# Backfill TTL for pre-existing legacy (un-stamped) records on upgrade.
# 0 = off (preserve current reject-on-populated-store behavior).
LEGACY_RECORDS_TTL_SECONDS = int(os.environ.get("LEGACY_RECORDS_TTL_SECONDS", "30"))
# Consumer group drives the state namespace. Own group so this is a standalone
# TTL-on comparison (not sharing the stable store).
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "dedup-filter-feature-v1")
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "800"))
# LOGGER=off disables the periodic status logger (the rocksdb_exact_keys full
# key scan every interval is O(keys) — turn it off in production to avoid the
# scan cost). Default "on" preserves current behavior.
LOGGER_ENABLED = os.environ.get("LOGGER", "on").strip().lower() in ("1", "true", "yes", "on")

# Aggressive RocksDB compaction settings so TTL-driven reclaim is visible in
# a short test window. Defaults are 64 MB memtable / 64 MB SST.
_ROCKSDB_OPTS = RocksDBOptions(
    write_buffer_size=int(os.environ.get("ROCKSDB_WRITE_BUFFER_SIZE", str(4 * 1024 * 1024))),
    target_file_size_base=int(os.environ.get("ROCKSDB_TARGET_FILE_SIZE_BASE", str(2 * 1024 * 1024))),
    max_write_buffer_number=int(os.environ.get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "2")),
    legacy_records_ttl=(
        timedelta(seconds=LEGACY_RECORDS_TTL_SECONDS)
        if LEGACY_RECORDS_TTL_SECONDS > 0
        else None
    ),
)

_PADDING = "x" * VALUE_PADDING_BYTES

# Session-only view of NEW writes made this run: order_id -> wall-clock expiry.
# Pruned in the logger. NOTE: this does NOT include the backfilled legacy
# records — those live only in RocksDB — so to watch the backfill drain, read
# rocksdb_exact_keys below, not this counter.
_session_new: dict = {}


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
    # _state_manager.stores is {stream_id: {store_name: Store}}.
    for stream_stores in app._state_manager.stores.values():
        for store in stream_stores.values():
            for partition in store.partitions.values():
                yield partition


def _rocksdb_est_keys() -> int:
    """RocksDB key estimate across partitions (all CFs). Includes tombstones /
    not-yet-compacted entries, so it lags logical expiry and drops after a
    sweep/compaction. Post-backfill it also counts the __ttl_index__ CF."""
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
    persisted dedup-entry count (excludes the index CF). Drains as expired
    backfilled records are swept. O(keys); fine at test scale."""
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
        now = time.time()
        for k in list(_session_new.keys()):
            exp = _session_new.get(k)
            if exp is not None and exp <= now:
                _session_new.pop(k, None)
        size = _dir_size_bytes(STATE_DIR)
        est = _rocksdb_est_keys()
        exact = _rocksdb_exact_keys()
        print(
            f"[STATE-SIZE] bytes={size} ({size/1024:.1f} KiB) "
            f"rocksdb_exact_keys={exact} rocksdb_est_keys={est} "
            f"session_new_live={len(_session_new)}",
            flush=True,
        )


if LOGGER_ENABLED:
    threading.Thread(target=_periodic_status_logger, daemon=True).start()
else:
    print("[STARTUP] LOGGER=off — periodic status logger disabled", flush=True)

app = Application(
    consumer_group=CONSUMER_GROUP,
    state_dir=STATE_DIR,
    rocksdb_options=_ROCKSDB_OPTS,
)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Re-key from "<order_id>-<STATUS>" down to "<order_id>" so per-key state is
# scoped per order, not per (order, status).
sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    new_status = value["status"]
    order_id = value["order_id"]
    stored = state.get("entry")
    stored_status = stored["status"] if stored else None

    if stored_status == new_status:
        return False

    state.set(
        "entry",
        {"status": new_status, "pad": _PADDING},
        ttl=timedelta(seconds=STATE_TTL_SECONDS),
    )
    _session_new[order_id] = time.time() + STATE_TTL_SECONDS
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
