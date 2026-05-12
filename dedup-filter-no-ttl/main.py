import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _state_size_logger():
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        print(f"[STATE-SIZE-NO-TTL] dir={STATE_DIR} bytes={size} ({size/1024:.1f} KiB)", flush=True)


threading.Thread(target=_state_size_logger, daemon=True).start()

app = Application(consumer_group="dedup-filter-no-ttl-v2", state_dir=STATE_DIR)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Re-key from "<order_id>-<STATUS>" down to "<order_id>" so per-key state is
# scoped per order, not per (order, status).
sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    """
    Same toggle-detection logic as dedup-filter, but state entries have NO TTL.
    Entries persist forever, so RocksDB state will grow unbounded as new
    order IDs are seen.
    """
    new_status = value["status"]
    stored = state.get("last_status")

    if stored == new_status:
        print(f"[DEDUP-NO-TTL] Suppressed key={key} status={new_status}", flush=True)
        return False

    state.set("last_status", new_status)  # no TTL - state grows forever
    if stored is None:
        print(f"[DEDUP-NO-TTL] Forwarded first-seen key={key} status={new_status}", flush=True)
    else:
        print(f"[DEDUP-NO-TTL] Forwarded toggle key={key} {stored}->{new_status}", flush=True)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
