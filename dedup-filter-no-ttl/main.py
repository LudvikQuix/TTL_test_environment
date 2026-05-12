import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "200"))

_PADDING = "x" * VALUE_PADDING_BYTES

_counts = {"forwarded_fresh": 0, "forwarded_toggle": 0, "suppressed": 0}


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
            f"[STATE-SIZE-NO-TTL] bytes={size} ({size/1024:.1f} KiB) "
            f"forwarded_fresh={_counts['forwarded_fresh']} "
            f"forwarded_toggle={_counts['forwarded_toggle']} "
            f"suppressed={_counts['suppressed']}",
            flush=True,
        )


threading.Thread(target=_periodic_status_logger, daemon=True).start()

app = Application(consumer_group="dedup-filter-no-ttl-v4", state_dir=STATE_DIR)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    new_status = value["status"]
    stored = state.get("entry")
    stored_status = stored["status"] if stored else None

    if stored_status == new_status:
        _counts["suppressed"] += 1
        return False

    state.set("entry", {"status": new_status, "pad": _PADDING})  # no TTL
    if stored_status is None:
        _counts["forwarded_fresh"] += 1
    else:
        _counts["forwarded_toggle"] += 1
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
