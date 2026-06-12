import os
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application
from quixstreams.models import TopicConfig

OUTPUT_TOPIC = os.environ.get("output", "recovery-input")
RATE_PER_SECOND = float(os.environ.get("RATE_PER_SECOND", "100"))
PADDING_BYTES = int(os.environ.get("PADDING_BYTES", "200"))
KEY_COUNT = int(os.environ.get("KEY_COUNT", "1000"))

_PAD = "x" * PADDING_BYTES
_SLEEP = 1 / RATE_PER_SECOND

# Aggressive retention so the broker's low watermark overtakes a stopped
# consumer within minutes: the active segment rolls every 60 s and rolled
# segments are deleted after 2 min. Must match recovery-filter/main.py
# exactly so whichever app starts first creates the topic correctly.
_TOPIC_CONFIG = TopicConfig(
    num_partitions=1,
    replication_factor=1,
    extra_config={"segment.ms": "60000", "retention.ms": "120000"},
)

app = Application()
output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json", config=_TOPIC_CONFIG)


def main():
    print(
        f"[GEN] Producing forever to '{OUTPUT_TOPIC}' "
        f"(rate={RATE_PER_SECOND}/s, key_count={KEY_COUNT}, pad={PADDING_BYTES}B)",
        flush=True,
    )
    seq = 0
    last_log = time.monotonic()
    with app.get_producer() as producer:
        while True:
            key = f"key-{seq % KEY_COUNT}"
            value = {"seq": seq, "ts": int(time.time() * 1000), "pad": _PAD}
            msg = output_topic.serialize(key=key, value=value)
            producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
            seq += 1
            now = time.monotonic()
            if now - last_log >= 10:
                print(f"[GEN] produced={seq}", flush=True)
                last_log = now
            time.sleep(_SLEEP)


if __name__ == "__main__":
    main()
