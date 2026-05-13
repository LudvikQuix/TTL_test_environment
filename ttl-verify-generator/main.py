import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

app = Application()
output_topic = app.topic(os.environ["output"], value_serializer="json")

# Must EXCEED the feature deduper's STATE_TTL_SECONDS so the cached entry expires.
TTL_WAIT_SECONDS = float(os.environ.get("TTL_WAIT_SECONDS", "7"))
QUICK = 0.2  # gap between same-order events that must stay within TTL

# Each scenario: (order_id, [(status, sleep_after_seconds), ...])
# Expected feature-deduper output (with STATE_TTL_SECONDS=5):
#   order-001: ON, ON, ON, ON, ON                       -> 1 PASS,  4 BLOCK
#   order-002: ON, ON, ON, [wait>TTL], ON               -> 2 PASS,  2 BLOCK
#   order-003: ON, OFF, [wait>TTL], ON                  -> 3 PASS,  0 BLOCK
SCENARIOS = [
    ("order-001", [
        ("ON", QUICK), ("ON", QUICK), ("ON", QUICK), ("ON", QUICK), ("ON", 0.0),
    ]),
    ("order-002", [
        ("ON", QUICK), ("ON", QUICK), ("ON", TTL_WAIT_SECONDS),
        ("ON", 0.0),
    ]),
    ("order-003", [
        ("ON", QUICK), ("OFF", TTL_WAIT_SECONDS),
        ("ON", 0.0),
    ]),
]


def _send(producer, order_id: str, status: str) -> None:
    key = f"{order_id}-{status}"
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    value = {"order_id": order_id, "status": status, "timestamp": ts}
    msg = output_topic.serialize(key=key, value=value)
    producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
    print(f"[GEN] sent key={key}", flush=True)


def main():
    print(f"[GEN] TTL_WAIT_SECONDS={TTL_WAIT_SECONDS}", flush=True)
    with app.get_producer() as producer:
        for order_id, steps in SCENARIOS:
            print(f"[GEN] --- {order_id} ---", flush=True)
            for status, sleep_after in steps:
                _send(producer, order_id, status)
                producer.flush()
                if sleep_after:
                    time.sleep(sleep_after)
    print("[GEN] done.", flush=True)


if __name__ == "__main__":
    main()
