import os
import time
import random
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

app = Application()
output_topic = app.topic(os.environ["output"], value_serializer="json")

KEY_SPACE = int(os.environ.get("KEY_SPACE", "1000000"))
MESSAGE_COUNT = int(os.environ.get("MESSAGE_COUNT", "600"))
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "0.1"))
SEED = os.environ.get("SEED")  # optional - set to fix the random sequence

STATUSES = ["ON", "OFF"]


def main():
    if SEED is not None:
        random.seed(int(SEED))
        print(f"[GENERATOR] Using deterministic seed={SEED}", flush=True)

    print(
        f"[GENERATOR] Producing {MESSAGE_COUNT} messages "
        f"(key_space=1..{KEY_SPACE}, sleep={SLEEP_SECONDS}s)",
        flush=True,
    )

    with app.get_producer() as producer:
        for i in range(1, MESSAGE_COUNT + 1):
            order_id = f"order-{random.randint(1, KEY_SPACE):07d}"
            status = random.choice(STATUSES)
            key = f"{order_id}-{status}"
            ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            value = {
                "order_id": order_id,
                "status": status,
                "payload": "some data",
                "timestamp": ts,
            }
            msg = output_topic.serialize(key=key, value=value)
            producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
            print(f"[GENERATOR] {i}/{MESSAGE_COUNT} key={key}", flush=True)
            time.sleep(SLEEP_SECONDS)

    print(f"[GENERATOR] Done. Produced {MESSAGE_COUNT} messages.", flush=True)


if __name__ == "__main__":
    main()
