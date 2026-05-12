import os
import time
import random
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

app = Application()
output_topic = app.topic(os.environ["output"], value_serializer="json")

KEYS = [f"order-{i:03d}" for i in range(1, 11)]  # order-001 .. order-010


def main():
    with app.get_producer() as producer:
        while True:
            key = random.choice(KEYS)
            ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            value = {"idempotency_key": key, "payload": "some data", "timestamp": ts}
            msg = output_topic.serialize(key=key, value=value)
            producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
            print(f"[GENERATOR] Produced key={key} ts={ts}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
