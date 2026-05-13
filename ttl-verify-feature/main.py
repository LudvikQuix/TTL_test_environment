import os
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "5"))
STATE_DIR = os.environ.get("STATE_DIR", "state")

app = Application(
    consumer_group="ttl-verify-feature-v1",
    state_dir=STATE_DIR,
    auto_offset_reset="latest",
)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)


def parse_key(value, key, timestamp, headers):
    key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
    entity, _, status = key_str.rpartition("-")  # "order-001-ON" -> ("order-001", "-", "ON")
    return {**(value or {}), "_entity": entity, "_status": status}


sdf = sdf.apply(parse_key, metadata=True)
sdf = sdf.group_by(lambda v: v["_entity"], name="by_entity")


def dedup_filter(value, key, timestamp, headers, state):
    new_status = value["_status"]
    entity = value["_entity"]
    batch = value.get("batch")
    stored_status = state.get("last_status")

    if stored_status == new_status:
        print(f"[FEATURE] BLOCK entity={entity} status={new_status} batch={batch}", flush=True)
        return False

    state.set("last_status", new_status, ttl=timedelta(seconds=STATE_TTL_SECONDS))
    print(f"[FEATURE] PASS  entity={entity} status={new_status} batch={batch}", flush=True)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    print(f"[FEATURE] starting (key-based), STATE_TTL_SECONDS={STATE_TTL_SECONDS}", flush=True)
    app.run()
