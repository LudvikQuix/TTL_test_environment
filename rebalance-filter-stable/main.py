import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()

from quixstreams import Application  # noqa: E402  (must follow load_dotenv)
from quixstreams.state.rocksdb.options import RocksDBOptions  # noqa: E402

# ---------------------------------------------------------------------------
# Rebalance lock-contention livelock A/B filter.
#
# This file is BYTE-IDENTICAL in rebalance-filter-feature/ and
# rebalance-filter-stable/. The two deployments differ ONLY in
# requirements.txt (the pinned quixstreams commit) and their env defaults
# (output topic + CG_PREFIX). Any behavioral divergence between the two
# deployments under identical churn is therefore attributable to the fix.
#
# Behavior (see dev-planning/rebalance-livelock-test/architecture.md):
#   - Consume `input` (8-partition topic), value JSON.
#   - On EVERY message, write a padded entry into per-key RocksDB state. No
#     group_by -> state partitions == source partitions (8), split 4/4 across
#     the 2 replicas that share one state volume. No TTL -> state grows
#     monotonically, maximizing background flush/compaction debt so a pre-fix
#     db.close() blocks (the livelock precondition).
#   - Emit EVERY message to `output` so the output topic's high-watermark
#     advances steadily, giving scripts/rebalance_report.py a live stall /
#     handover signal. (The reused duplicate-key-generator bakes the status
#     into the record key, so an emit-on-change rule would fall silent after
#     warm-up and defeat stall detection.)
#
# Config knobs (all via env; RocksDB tuned small so flushes are frequent):
#   MAX_POLL_INTERVAL_MS  - consumer_extra_config max.poll.interval.ms. Low
#                           (60000) so a slow revoke/close on the pre-fix build
#                           can exceed it and eviction is observable; also
#                           scales the feature build's bounded-flush (x0.2) and
#                           open-deadline (x0.5) budgets.
#   QS_LOGLEVEL           - quixstreams logger level. DEBUG (default) is
#                           REQUIRED to observe the "Fast revoke: skipping local
#                           state flush" and benign-close markers (both are
#                           logged at DEBUG in the feature build) and the
#                           per-attempt "Opening rocksdb partition ... attempt=N"
#                           line used to derive max_lock_attempt on the stable
#                           build. There is no per-message DEBUG logging in the
#                           hot path, so DEBUG does not flood or slow processing.
# ---------------------------------------------------------------------------


def _envflag(name: str, default: str = "on") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


CG_PREFIX = os.environ.get("CG_PREFIX", "rebalance-filter").strip()
CG_VERSION = os.environ.get("CG_VERSION", "v1").strip()
CONSUMER_GROUP = f"{CG_PREFIX}-{CG_VERSION}"
STATE_DIR = os.environ.get("STATE_DIR", "state")
VALUE_PADDING_BYTES = int(os.environ.get("VALUE_PADDING_BYTES", "2000"))
MAX_POLL_INTERVAL_MS = int(os.environ.get("MAX_POLL_INTERVAL_MS", "60000"))
STATE_SIZE_LOG_INTERVAL = int(os.environ.get("STATE_SIZE_LOG_INTERVAL", "5"))
LOGGER_ENABLED = _envflag("LOGGER", "on")
QS_LOGLEVEL = os.environ.get("QS_LOGLEVEL", "DEBUG").strip()

_ROCKSDB_OPTS = RocksDBOptions(
    write_buffer_size=int(os.environ.get("ROCKSDB_WRITE_BUFFER_SIZE", str(1024 * 1024))),
    target_file_size_base=int(os.environ.get("ROCKSDB_TARGET_FILE_SIZE_BASE", str(2 * 1024 * 1024))),
    max_write_buffer_number=int(os.environ.get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "2")),
)

_PADDING = "x" * VALUE_PADDING_BYTES


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
    # _state_manager.stores is {stream_id: {store_name: Store}}; descend to the
    # RocksDB partitions. Mirrors dedup-filter-stable/main.py._iter_partitions.
    for stream_stores in app._state_manager.stores.values():
        for store in stream_stores.values():
            for partition in store.partitions.values():
                yield partition


def _rocksdb_est_keys() -> int:
    """RocksDB's own key estimate across partitions - cheap and does not scan
    keys. We deliberately do NOT run an O(keys) exact scan: it would read
    RocksDB during a handover and can contend with / slow the close under test."""
    try:
        total = 0
        for partition in _iter_partitions():
            n = partition._db.property_int_value("rocksdb.estimate-num-keys")
            if n is not None:
                total += int(n)
        return total
    except Exception:
        return -1


def _periodic_status_logger() -> None:
    while True:
        time.sleep(STATE_SIZE_LOG_INTERVAL)
        size = _dir_size_bytes(STATE_DIR)
        est = _rocksdb_est_keys()
        print(
            f"[STATE-SIZE-REBAL] cg={CONSUMER_GROUP} bytes={size} "
            f"({size / 1024 / 1024:.1f} MiB) rocksdb_est_keys={est}",
            flush=True,
        )


if LOGGER_ENABLED:
    threading.Thread(target=_periodic_status_logger, daemon=True).start()
else:
    print("[STARTUP] LOGGER=off - periodic status logger disabled", flush=True)

print(
    f"[STARTUP] consumer_group={CONSUMER_GROUP} "
    f"max_poll_interval_ms={MAX_POLL_INTERVAL_MS} "
    f"value_padding_bytes={VALUE_PADDING_BYTES} loglevel={QS_LOGLEVEL} "
    f"write_buffer_size={_ROCKSDB_OPTS.write_buffer_size} "
    f"target_file_size_base={_ROCKSDB_OPTS.target_file_size_base} "
    f"max_write_buffer_number={_ROCKSDB_OPTS.max_write_buffer_number}",
    flush=True,
)

app = Application(
    consumer_group=CONSUMER_GROUP,
    state_dir=STATE_DIR,
    rocksdb_options=_ROCKSDB_OPTS,
    consumer_extra_config={"max.poll.interval.ms": MAX_POLL_INTERVAL_MS},
    loglevel=QS_LOGLEVEL,
)
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)


def write_state(value, key, timestamp, headers, state):
    # Heavy write on EVERY message: the partitioned RocksDB write pressure the
    # rebalance handover test depends on. Keyed by the message key (no group_by).
    state.set("entry", {"status": value.get("status"), "pad": _PADDING})


# update() applies the write as a side effect and passes every message through;
# to_topic() then emits each one -> steady output high-watermark for stall
# detection (see the module docstring for why emit-on-change is not used here).
sdf = sdf.update(write_state, stateful=True, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
