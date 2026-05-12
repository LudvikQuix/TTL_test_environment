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
        print(f"[STATE-SIZE-STABLE] dir={STATE_DIR} bytes={size} ({size/1024:.1f} KiB)", flush=True)


threading.Thread(target=_state_size_logger, daemon=True).start()

app = Application(consumer_group="dedup-filter-stable-v2", state_dir=STATE_DIR)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

# Re-key from "<order_id>-<STATUS>" down to "<order_id>" so per-key state is
# scoped per order, not per (order, status).
sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    """
    Toggle-detection dedup running on stable quixstreams 3.23.6 (no TTL feature
    available). Provides a baseline to compare against the TTL-branch builds.
    """
    new_status = value["status"]
    stored = state.get("last_status")

    if stored == new_status:
        print(f"[DEDUP-STABLE] Suppressed key={key} status={new_status}", flush=True)
        return False

    state.set("last_status", new_status)
    if stored is None:
        print(f"[DEDUP-STABLE] Forwarded first-seen key={key} status={new_status}", flush=True)
    else:
        print(f"[DEDUP-STABLE] Forwarded toggle key={key} {stored}->{new_status}", flush=True)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
