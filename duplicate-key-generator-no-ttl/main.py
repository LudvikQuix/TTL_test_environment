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
STATUSES = ["ON", "OFF"]


def main():
    with app.get_producer() as producer:
        while True:
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
            print(f"[GENERATOR-NO-TTL] Produced key={key}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
