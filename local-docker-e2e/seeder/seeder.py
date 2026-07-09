"""Local, deterministic producer that replaces the cloud Duplicate Key
Generator for the E2E harness (spec 6.2).

Two modes, both producing to the app INPUT topic (``$input``, default
``raw-events``) with the exact message shape of
``duplicate-key-generator/main.py:40-51``::

    key   = f"{order_id}-{status}"
    value = {"order_id", "status", "payload", "timestamp"}   # JSON

--seed --count N --key-prefix order-
    Emit N DISTINCT order_ids (``order-0000001 .. order-{N:07d}``), each ONCE
    with the seed status. The seeder app (TTL_MODE=0) persists exactly N
    default-CF keys (one per order_id via ``group_by("order_id")``).

--trigger --count M
    Emit M messages that force fresh ``ttl=`` writes on the migrate build.
    Default: status-FLIP of the first M seed keys (opposite status) so the
    first such write flips+backfills the store WITHOUT adding keys -> the
    post-migration key count stays exactly N. (Alternative: pass
    --new-keys to emit brand-new order_ids OUTSIDE the seed range, in which
    case the expected total becomes N + M.)

Deterministic: no ``random``; status is positional (seed vs flip).
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application

# Positional statuses: index 0 = seed status, index 1 = the flip (trigger)
# status. Matches the DKG's ["ON", "OFF"] domain.
STATUSES = ["ON", "OFF"]
SEED_STATUS = STATUSES[0]
FLIP_STATUS = STATUSES[1]


def _parse_args(argv):
    p = argparse.ArgumentParser(description="Local deterministic seeder/trigger.")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--seed", action="store_true", help="seed N distinct legacy keys")
    mode.add_argument("--trigger", action="store_true", help="emit M ttl=-triggering msgs")
    p.add_argument("--count", type=int, required=True, help="N (seed) or M (trigger)")
    p.add_argument("--key-prefix", default="order-", help="order_id prefix")
    p.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="messages/sec throttle (0 = as fast as possible)",
    )
    p.add_argument(
        "--new-keys",
        action="store_true",
        help="(trigger only) emit brand-new order_ids outside the seed range "
        "instead of flipping existing keys (expected total becomes N+M)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    broker = os.environ.get("BROKER_ADDRESS")
    if not broker:
        print("[SEEDER] BROKER_ADDRESS is required", flush=True)
        return 2
    input_name = os.environ["input"]

    app = Application(broker_address=broker)
    topic = app.topic(input_name, value_serializer="json")

    prefix = args.key_prefix
    count = args.count
    interval = (1.0 / args.rate) if args.rate and args.rate > 0 else 0.0

    if args.seed:
        status = SEED_STATUS
        # order-0000001 .. order-{N:07d}
        id_range = range(1, count + 1)
        label = "seed"
    else:
        status = FLIP_STATUS
        if args.new_keys:
            # Brand-new ids ABOVE the seed range (start at 9_000_001 to stay
            # clear of realistic seed sizes); every one is first-sight -> +M keys.
            id_range = range(9_000_001, 9_000_001 + count)
            label = "trigger(new-keys)"
        else:
            # Flip the first M seed keys -> status change -> ttl= write, no new keys.
            id_range = range(1, count + 1)
            label = "trigger(flip)"

    produced = 0
    with app.get_producer() as producer:
        for i in id_range:
            order_id = f"{prefix}{i:07d}"
            key = f"{order_id}-{status}"
            ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            value = {
                "order_id": order_id,
                "status": status,
                "payload": "some data",
                "timestamp": ts,
            }
            msg = topic.serialize(key=key, value=value)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            produced += 1
            if interval:
                time.sleep(interval)
        producer.flush()

    print(
        f"[SEEDER] produced {produced} {label} message(s) to topic={input_name} "
        f"status={status} prefix={prefix}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
