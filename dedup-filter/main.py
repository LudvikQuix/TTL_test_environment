import os
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "30"))

app = Application(consumer_group="dedup-filter")
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)


def dedup_filter(value, key, timestamp, headers, state):
    """
    Return True (forward) if this key has not been seen within the TTL window.
    Return False (drop) if a prior message with the same Kafka key is still live in state.

    State is scoped per Kafka message key by the framework, so 'seen' is a
    fixed key inside each per-key state namespace.
    """
    if state.get("seen") is not None:
        print(f"[DEDUP] Suppressed duplicate key={key}", flush=True)
        return False

    state.set("seen", True, ttl=timedelta(seconds=STATE_TTL_SECONDS))
    print(f"[DEDUP] Forwarded new key={key}", flush=True)
    return True


sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
