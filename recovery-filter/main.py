import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.models import TopicConfig

INPUT_TOPIC = os.environ.get("input", "recovery-input")
OUTPUT_TOPIC = os.environ.get("output", "recovery-output")
AUTO_RECOVER = os.environ.get("AUTO_RECOVER", "true").lower() == "true"
OFFSET_RESET = os.environ.get("OFFSET_RESET", "earliest")
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))

# Must match recovery-generator/main.py exactly so whichever app starts first
# creates the topic with the aggressive retention the test depends on.
_TOPIC_CONFIG = TopicConfig(
    num_partitions=1,
    replication_factor=1,
    extra_config={"segment.ms": "60000", "retention.ms": "120000"},
)

# Keys seen since process start; reported as live_entries so a destructive
# state recovery (counts restarting from 1) is visible next to RocksDB size.
_live_keys: set = set()


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _rocksdb_num_keys() -> int:
    """Sum 'rocksdb.estimate-num-keys' across every open state partition.

    Includes tombstones / not-yet-compacted entries, so it can be larger than
    the logical live count and will visibly drop after a compaction.
    """
    try:
        total = 0
        for topic_stores in app._state_manager.stores.values():
            for store in topic_stores.values():
                for partition in store.partitions.values():
                    n = partition._db.property_int_value(
                        "rocksdb.estimate-num-keys"
                    )
                    if n is not None:
                        total += int(n)
        return total
    except Exception:
        return -1


def _periodic_status_logger():
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        rocks_keys = _rocksdb_num_keys()
        print(
            f"[STATE-SIZE-RECOVERY] bytes={size} ({size/1024:.1f} KiB) "
            f"live_entries={len(_live_keys)} rocksdb_keys={rocks_keys}",
            flush=True,
        )


threading.Thread(target=_periodic_status_logger, daemon=True).start()

print(
    f"[RECOVERY-FILTER] auto_recover_from_source_offset_out_of_range={AUTO_RECOVER} "
    f"state_recovery_offset_reset={OFFSET_RESET}",
    flush=True,
)

app = Application(
    consumer_group="recovery-filter-v1",
    state_dir=STATE_DIR,
    auto_offset_reset="earliest",
    auto_recover_from_source_offset_out_of_range=AUTO_RECOVER,
    state_recovery_offset_reset=OFFSET_RESET,
)
input_topic = app.topic(INPUT_TOPIC, value_deserializer="json", config=_TOPIC_CONFIG)
output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

sdf = app.dataframe(input_topic)


def count_per_key(value, key, timestamp, headers, state):
    count = state.get("count", 0) + 1
    state.set("count", count)
    value["count"] = count
    _live_keys.add(key)
    return value


sdf = sdf.apply(count_per_key, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
