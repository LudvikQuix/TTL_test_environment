import os

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

STATE_DIR = os.environ.get("STATE_DIR", "state")

app = Application(
    consumer_group="ttl-verify-stable-v1",
    state_dir=STATE_DIR,
    auto_offset_reset="latest",
)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)
sdf = sdf.group_by("order_id", name="by_order")


def dedup_filter(value, key, timestamp, headers, state):
    new_status = value["status"]
    batch = value.get("batch")
    order_id = value["order_id"]
    stored = state.get("entry")
    stored_status = stored["status"] if stored else None

    if stored_status == new_status:
        print(f"[STABLE] BLOCK order={order_id} batch={batch}", flush=True)
        return False

    state.set("entry", {"status": new_status})
    print(f"[STABLE] PASS  order={order_id} batch={batch}", flush=True)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    print("[STABLE] starting (no TTL — tagged 3.23.6)", flush=True)
    app.run()
