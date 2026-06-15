import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

# No TTL here — pure seeder that fills state to build a large legacy store.
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "800"))

_ROCKSDB_OPTS = RocksDBOptions(
    write_buffer_size=int(os.environ.get("ROCKSDB_WRITE_BUFFER_SIZE", str(4 * 1024 * 1024))),
    target_file_size_base=int(os.environ.get("ROCKSDB_TARGET_FILE_SIZE_BASE", str(2 * 1024 * 1024))),
    max_write_buffer_number=int(os.environ.get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "2")),
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
            f"[STATE-SIZE-STABLE] bytes={size} ({size/1024:.1f} KiB) "
            f"rocksdb_exact_keys={exact} rocksdb_est_keys={est} "
            f"session_seen={len(_session_seen)}",
            flush=True,
        )


threading.Thread(target=_periodic_status_logger, daemon=True).start()

app = Application(
    consumer_group="dedup-filter-stable-v7.1",
    state_dir=STATE_DIR,
    rocksdb_options=_ROCKSDB_OPTS,
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

    state.set("entry", {"status": new_status, "pad": _PADDING})
    _session_seen.add(order_id)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
