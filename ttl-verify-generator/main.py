import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

app = Application()
output_topic = app.topic(os.environ["output"], value_serializer="json")

KEY_COUNT = int(os.environ.get("KEY_COUNT", "3"))
WAIT_SECONDS = float(os.environ.get("WAIT_SECONDS", "10"))
STATUS = os.environ.get("STATUS", "ON")


def _send_batch(producer, batch_num: int) -> None:
    print(f"[GEN] --- BATCH {batch_num} ---", flush=True)
    for i in range(1, KEY_COUNT + 1):
        order_id = f"order-{i:03d}"
        key = f"{order_id}-{STATUS}"
        ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        value = {
            "order_id": order_id,
            "status": STATUS,
            "batch": batch_num,
            "payload": "ttl-verify",
            "timestamp": ts,
        }
        msg = output_topic.serialize(key=key, value=value)
        producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
        print(f"[GEN] batch={batch_num} key={key}", flush=True)


def main():
    print(
        f"[GEN] keys={KEY_COUNT} status={STATUS} wait_between_batches={WAIT_SECONDS}s",
        flush=True,
    )
    with app.get_producer() as producer:
        _send_batch(producer, 1)
        producer.flush()
        print(f"[GEN] sleeping {WAIT_SECONDS}s...", flush=True)
        time.sleep(WAIT_SECONDS)
        _send_batch(producer, 2)
        producer.flush()
    print("[GEN] done.", flush=True)


if __name__ == "__main__":
    main()
