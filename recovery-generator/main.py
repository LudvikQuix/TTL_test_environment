import os
import random
import time

from dotenv import load_dotenv

load_dotenv()

OUTPUT_TOPIC = os.environ.get("output", "recovery-input")
RATE_PER_SECOND = float(os.environ.get("RATE_PER_SECOND", "100"))
PADDING_BYTES = int(os.environ.get("PADDING_BYTES", "200"))
KEY_COUNT = int(os.environ.get("KEY_COUNT", "1000"))
FLIP_PROBABILITY = float(os.environ.get("FLIP_PROBABILITY", "0.05"))
MESSAGE_COUNT = int(os.environ.get("MESSAGE_COUNT", "10000"))

_PAD = "x" * PADDING_BYTES
_SLEEP = 1 / RATE_PER_SECOND

app = None
output_topic = None


def toggle(status: str) -> str:
    return "OFF" if status == "ON" else "ON"


def should_flip(rand_value: float, probability: float) -> bool:
    return rand_value < probability


def next_status(prev_status, flip: bool) -> str:
    if prev_status is None:
        return "ON"
    if flip:
        return toggle(prev_status)
    return prev_status


def main():
    global app, output_topic

    from quixstreams import Application
    from quixstreams.models import TopicConfig

    # Aggressive retention so the broker's low watermark overtakes a stopped
    # consumer within minutes: the active segment rolls every 60 s and rolled
    # segments are deleted after 2 min. Must match recovery-filter/main.py
    # exactly so whichever app starts first creates the topic correctly.
    topic_config = TopicConfig(
        num_partitions=1,
        replication_factor=1,
        extra_config={"segment.ms": "60000", "retention.ms": "120000"},
    )

    app = Application()
    output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json", config=topic_config)

    run_forever = MESSAGE_COUNT == 0
    target = "forever" if run_forever else f"{MESSAGE_COUNT} messages"
    print(
        f"[GEN] Producing {target} to '{OUTPUT_TOPIC}' "
        f"(rate={RATE_PER_SECOND}/s, key_count={KEY_COUNT}, pad={PADDING_BYTES}B, "
        f"flip_probability={FLIP_PROBABILITY})",
        flush=True,
    )
    seq = 0
    start = time.monotonic()
    last_log = start
    key_status: dict = {}
    with app.get_producer() as producer:
        # MESSAGE_COUNT == 0 keeps the old forever-Service behavior; otherwise
        # emit exactly MESSAGE_COUNT then fall through to flush + summary + exit.
        while run_forever or seq < MESSAGE_COUNT:
            key = f"key-{seq % KEY_COUNT}"
            flip = should_flip(random.random(), FLIP_PROBABILITY)
            status = next_status(key_status.get(key), flip)
            key_status[key] = status
            value = {"seq": seq, "ts": int(time.time() * 1000), "pad": _PAD, "status": status}
            msg = output_topic.serialize(key=key, value=value)
            producer.produce(topic=output_topic.name, key=msg.key, value=msg.value)
            seq += 1
            now = time.monotonic()
            if now - last_log >= 10:
                print(f"[GEN] produced={seq}", flush=True)
                last_log = now
            time.sleep(_SLEEP)
        # Bounded run only (forever never reaches here): confirm delivery, then
        # log the final summary and return so the Job exits 0.
        producer.flush()
        elapsed = time.monotonic() - start
        rate = seq / elapsed if elapsed > 0 else 0.0
        print(
            f"[GEN] done emitted={seq} elapsed={elapsed:.2f}s rate={rate:.1f}/s",
            flush=True,
        )


if __name__ == "__main__":
    main()
