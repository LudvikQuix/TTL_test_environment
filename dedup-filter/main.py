import os
import threading
import time
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "3"))
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "200"))

# Aggressive RocksDB compaction settings so TTL-driven reclaim is visible in
# a short test window. Defaults are 64 MB memtable / 64 MB SST; lowering both
# makes memtable flushes and L0->L1 compactions trigger far sooner.
_ROCKSDB_OPTS = RocksDBOptions(
    write_buffer_size=int(os.environ.get("ROCKSDB_WRITE_BUFFER_SIZE", str(4 * 1024 * 1024))),
    target_file_size_base=int(os.environ.get("ROCKSDB_TARGET_FILE_SIZE_BASE", str(2 * 1024 * 1024))),
    max_write_buffer_number=int(os.environ.get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "2")),
)

_PADDING = "x" * VALUE_PADDING_BYTES

_counts = {
    "forwarded_fresh": 0,
    "forwarded_expired": 0,
    "forwarded_toggle": 0,
    "suppressed": 0,
}
_seen_orders = set()


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _periodic_status_logger():
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        print(
            f"[STATE-SIZE] bytes={size} ({size/1024:.1f} KiB) "
            f"forwarded_fresh={_counts['forwarded_fresh']} "
            f"forwarded_expired={_counts['forwarded_expired']} "
            f"forwarded_toggle={_counts['forwarded_toggle']} "
            f"suppressed={_counts['suppressed']} "
            f"orders_seen={len(_seen_orders)}",
            flush=True,
        )


threading.Thread(target=_periodic_status_logger, daemon=True).start()

app = Application(
    consumer_group="dedup-filter-v4",
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
        _counts["suppressed"] += 1
        return False

    state.set(
        "entry",
        {"status": new_status, "pad": _PADDING},
        ttl=timedelta(seconds=STATE_TTL_SECONDS),
    )

    if stored_status is None:
        if order_id in _seen_orders:
            _counts["forwarded_expired"] += 1
        else:
            _counts["forwarded_fresh"] += 1
            _seen_orders.add(order_id)
    else:
        _counts["forwarded_toggle"] += 1
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
