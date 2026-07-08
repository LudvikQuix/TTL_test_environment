import logging
import os
import threading
import time
from datetime import timedelta

from dotenv import load_dotenv

from billing_client import build_billing_client

load_dotenv()

INPUT_TOPIC = os.environ.get("input", "recovery-input")
OUTPUT_TOPIC = os.environ.get("output", "recovery-output")
AUTO_RECOVER = os.environ.get("AUTO_RECOVER", "true").lower() == "true"
OFFSET_RESET = os.environ.get("OFFSET_RESET", "earliest")
STATE_DIR = os.environ.get("STATE_DIR", "state")
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "10"))
TTL_MODE = os.environ.get("TTL_MODE", "off").strip().lower() in ("1", "true", "yes", "on")
STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "30"))
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "recovery-filter-v1")
BILLING_KEYS_PER_EVENT = int(os.environ.get("BILLING_KEYS_PER_EVENT", "1000"))


def resolve_logger_level(raw: str) -> str:
    """Normalize the LOGGER env var into one of 'off' | 'info' | 'debug'.

    Legacy 'on' maps to 'info' for backward compatibility. Anything
    unrecognized safely falls back to 'info' instead of crashing.
    """
    value = (raw or "").strip().lower()
    if value == "on":
        return "info"
    if value in ("off", "info", "debug"):
        return value
    return "info"


def resolve_ttl_kwargs(ttl_mode: bool, ttl_seconds: int) -> dict:
    """Return the kwargs to pass to state.set() for the given TTL config."""
    if ttl_mode:
        return {"ttl": timedelta(seconds=ttl_seconds)}
    return {}


LOGGER_LEVEL = resolve_logger_level(os.environ.get("LOGGER", "info"))

# Pass/block counters since process start, mutated inside the nested
# dedup_filter closure; reported next to RocksDB key counts so TTL-driven
# state shrinkage is observable.
_pass_count = [0]
_block_count = [0]
_skip_count = [0]

# Set to the running Application instance by main(); read by
# _rocksdb_est_keys()/_rocksdb_exact_keys() from the periodic status logger
# thread.
app = None

# Billing integration (spec section 5.7). `billing` is set by main() and read by
# dedup_filter; the counters/timestamps drive the keys-processed event cadence.
billing = None
_keys_processed = [0]
_last_keys_fire_ms = [0]
_start_ms = [0]


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _iter_partitions():
    for stream_stores in app._state_manager.stores.values():
        for store in stream_stores.values():
            for partition in store.partitions.values():
                yield partition


def _rocksdb_est_keys() -> int:
    """RocksDB's own key estimate across partitions — cheap, persists across
    restarts. Includes tombstones / not-yet-compacted entries, so it lags
    logical deletes."""
    try:
        total = 0
        for partition in _iter_partitions():
            n = partition._db.property_int_value("rocksdb.estimate-num-keys")
            if n is not None:
                total += int(n)
        return total
    except Exception:
        return -1


def _rocksdb_exact_keys() -> int:
    """Exact count of keys in the default CF across partitions — the real
    persisted entry count. O(keys); fine at test scale."""
    try:
        total = 0
        for partition in _iter_partitions():
            total += sum(1 for _ in partition._db.keys())
        return total
    except Exception:
        return -1


def _periodic_status_logger():
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        exact = _rocksdb_exact_keys()
        est = _rocksdb_est_keys()
        print(
            f"[STATE-SIZE-RECOVERY] bytes={size} ({size/1024:.1f} KiB) "
            f"rocksdb_exact_keys={exact} rocksdb_est_keys={est} "
            f"pass_count={_pass_count[0]} block_count={_block_count[0]} "
            f"skip_count={_skip_count[0]}",
            flush=True,
        )


def decide(stored_status, new_status: str) -> bool:
    """Pass (True) if the status changed from what's stored (or nothing was
    stored yet — including after TTL expiry, since an expired key's
    state.get() returns None). Block (False) if it's the same as stored."""
    return stored_status != new_status


def resolve_new_status(value: dict):
    """Safely read the "status" field. Legacy messages produced before
    recovery-generator added the ON/OFF status field only have
    seq/ts/pad, so this returns None instead of raising KeyError."""
    return value.get("status")


def should_process(new_status) -> bool:
    """False when there's no status to evaluate (legacy message) — such
    messages should be skipped rather than passed or blocked."""
    return new_status is not None


def _maybe_emit_keys_event():
    """Count this message and, every BILLING_KEYS_PER_EVENT-th, POST a
    keys-processed billing event whose duration is the wall-clock ms since the
    previous fire. Fire-and-forget; never raises into the filter (spec 5.7)."""
    _keys_processed[0] += 1
    if billing is None or _keys_processed[0] % BILLING_KEYS_PER_EVENT != 0:
        return
    now = int(time.time() * 1000)
    duration = now - _last_keys_fire_ms[0] if _last_keys_fire_ms[0] else 0
    _last_keys_fire_ms[0] = now
    try:
        billing.emit(
            f"keys-processed-{BILLING_KEYS_PER_EVENT}",
            duration,
            {
                "operation": "dedup-filter",
                "keys": BILLING_KEYS_PER_EVENT,
                "pass": _pass_count[0],
                "block": _block_count[0],
                "skip": _skip_count[0],
            },
        )
    except Exception as exc:  # emit() is already non-raising; extra insurance
        if LOGGER_LEVEL == "debug":
            print(f"[BILLING-CLIENT] keys emit skipped: {exc}", flush=True)


def dedup_filter(value, key, timestamp, headers, state):
    _maybe_emit_keys_event()
    new_status = resolve_new_status(value)
    if not should_process(new_status):
        _skip_count[0] += 1
        if LOGGER_LEVEL != "off":
            print(
                f"[RECOVERY-FILTER] skipping legacy message without status field "
                f"(key={key}, timestamp={timestamp})",
                flush=True,
            )
        return False
    stored_status = state.get("status")
    passed = decide(stored_status, new_status)
    if passed:
        state.set("status", new_status, **resolve_ttl_kwargs(TTL_MODE, STATE_TTL_SECONDS))
        _pass_count[0] += 1
    else:
        _block_count[0] += 1
    if LOGGER_LEVEL == "debug":
        print(
            f"[DEBUG-RECOVERY] key={key} stored={stored_status} new={new_status} "
            f"decision={'PASS' if passed else 'BLOCK'} ttl_mode={TTL_MODE}",
            flush=True,
        )
    return passed


def main():
    global app, billing

    billing = build_billing_client()
    _start_ms[0] = int(time.time() * 1000)
    _last_keys_fire_ms[0] = _start_ms[0]

    logging.getLogger("quixstreams").setLevel(
        logging.DEBUG if LOGGER_LEVEL == "debug" else logging.INFO
    )

    if LOGGER_LEVEL != "off":
        threading.Thread(target=_periodic_status_logger, daemon=True).start()
    else:
        print("[STARTUP] LOGGER=off — periodic status logger disabled", flush=True)

    print(
        f"[RECOVERY-FILTER] mode=dedup-on-off "
        f"auto_recover_from_source_offset_out_of_range={AUTO_RECOVER} "
        f"state_recovery_offset_reset={OFFSET_RESET} ttl_mode={TTL_MODE} "
        f"state_ttl_seconds={STATE_TTL_SECONDS} logger_level={LOGGER_LEVEL} "
        f"consumer_group={CONSUMER_GROUP}",
        flush=True,
    )

    from quixstreams import Application
    from quixstreams.models import TopicConfig

    # Must match recovery-generator/main.py exactly so whichever app starts first
    # creates the topic with the aggressive retention the test depends on.
    topic_config = TopicConfig(
        num_partitions=1,
        replication_factor=1,
        extra_config={"segment.ms": "60000", "retention.ms": "120000"},
    )

    app = Application(
        consumer_group=CONSUMER_GROUP,
        state_dir=STATE_DIR,
        auto_offset_reset="earliest",
        auto_recover_from_source_offset_out_of_range=AUTO_RECOVER,
        state_recovery_offset_reset=OFFSET_RESET,
    )
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json", config=topic_config)
    output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

    sdf = app.dataframe(input_topic)
    sdf = sdf.filter(dedup_filter, stateful=True, metadata=True)
    sdf = sdf.to_topic(output_topic)

    app.run()

    # Graceful shutdown (app.run() returns on SIGTERM/SIGINT): emit one
    # backfill-action event whose duration is the wall-clock ms from service
    # start to shutdown (spec 5.7 Phase-1 default). Blocking best-effort so it
    # lands before the process exits and the daemon worker dies with it.
    duration_ms = int(time.time() * 1000) - _start_ms[0]
    billing.emit_now(
        "backfill-action",
        duration_ms,
        {"operation": "backfill", "messages": _keys_processed[0]},
    )


if __name__ == "__main__":
    main()
