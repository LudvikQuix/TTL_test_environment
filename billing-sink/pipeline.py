"""QuixStreams pipeline: single stateful op over billing-events (spec section 5).

One Application, one topic, one stateful SDF keyed by the constant STATE_KEY, so
all real events and synthetic flush ticks land in one State store the flush op
accumulates in. The flush op writes the State mirror in-context, does a blocking
Lakehouse write, and deletes from State only after a confirmed sink. A daemon
timer produces flush ticks so time-based flushes fire even with no real traffic.
"""

from __future__ import annotations

import threading
import uuid

from config import BillingConfig
from lake_writer import LakehouseWriter
from records import enrich_for_sink, now_ms
from state_buffer import (
    LAST_FLUSH_TS,
    PendingBuffer,
    add_pending,
    confirm_sunk,
    is_replay,
    pending_count,
    read_pending_records,
)

# Message envelopes on billing-events (spec section 5.3).
TYPE_EVENT = "event"
TYPE_FLUSH_TICK = "flush_tick"


class FlushController:
    """Owns the stateful op, the RAM buffer, and the flush retry backoff.

    Bind :meth:`handle` as the SDF callback: it runs on the single main
    processing thread, so its State access is always in-context and its backoff
    fields need no locking.
    """

    def __init__(
        self, config: BillingConfig, buffer: PendingBuffer, writer: LakehouseWriter
    ):
        self._config = config
        self._buffer = buffer
        self._writer = writer
        self._backoff_until = 0  # epoch ms; skip flush attempts until then
        self._failures = 0

    def handle(self, value, state) -> None:
        """SDF stateful callback for every billing-events message."""
        if not self._buffer.is_rebuilt:
            self._buffer.ensure_rebuilt(read_pending_records(state))
        # Seed the flush clock at boot so the first time-flush waits a full
        # interval; a restored value from a prior run is kept (flush recovered
        # records promptly).
        if state.get(LAST_FLUSH_TS) is None:
            state.set(LAST_FLUSH_TS, now_ms())

        if isinstance(value, dict) and value.get("type") == TYPE_EVENT:
            self._ingest_event(value, state)

        self._maybe_flush(state)
        self._buffer.set_pending_state_count(pending_count(state))

    def _ingest_event(self, value: dict, state) -> None:
        record = value.get("record") or {}
        event_id = record.get("event_id")
        if not event_id:
            return
        if is_replay(state, event_id):
            self._buffer.note_dropped_replay()
            return
        add_pending(state, record)
        self._buffer.add(record)

    def _maybe_flush(self, state) -> None:
        now = now_ms()
        pending = len(self._buffer)
        if pending == 0 or now < self._backoff_until:
            return
        last_flush = state.get(LAST_FLUSH_TS, 0)
        size_due = pending >= self._config.batch_size
        time_due = (now - last_flush) >= self._config.flush_interval_ms
        if not (size_due or time_due):
            return

        batch = self._buffer.snapshot(self._config.batch_size)
        if not batch:
            return
        batch_id = uuid.uuid4().hex
        rows = [
            enrich_for_sink(
                record,
                batch_id=batch_id,
                sink_ts=now,
                sink_deployment_id=self._config.deployment_id,
            )
            for record in batch
        ]
        try:
            self._writer.write_batch(rows)  # blocking; raises on failure
        except Exception as exc:
            self._register_failure(now, exc)
            return

        # Confirmed sink: delete from State, mark dedup, trim, clear RAM.
        self._failures = 0
        self._backoff_until = 0
        event_ids = [record["event_id"] for record in batch]
        confirm_sunk(state, event_ids, self._config.dedup_ttl_seconds, now)
        self._buffer.remove_many(event_ids)
        self._buffer.note_flush(now, pending_count(state))
        self._log(
            f"[BILLING-SINK] flush ok batch_id={batch_id} rows={len(event_ids)} "
            f"pending_after={pending_count(state)}"
        )

    def _register_failure(self, now: int, exc: Exception) -> None:
        self._failures += 1
        delay = min(
            self._config.flush_retry_base_ms * (2 ** (self._failures - 1)),
            self._config.flush_retry_cap_ms,
        )
        self._backoff_until = now + delay
        self._log(
            f"[BILLING-SINK] flush failed (attempt {self._failures}); "
            f"retry after {delay}ms: {exc}"
        )

    def _log(self, message: str) -> None:
        if self._config.logger_level != "off":
            print(message, flush=True)


def make_publisher(producer, topic, state_key: str):
    """Return a thread-safe ``publish(record)`` used by the HTTP handler."""

    def publish(record: dict) -> None:
        message = topic.serialize(
            key=state_key, value={"type": TYPE_EVENT, "record": record}
        )
        producer.produce(topic=topic.name, key=message.key, value=message.value)
        producer.poll(0)

    return publish


def start_flush_ticker(
    producer,
    topic,
    state_key: str,
    interval_seconds: int,
    stop_event: threading.Event,
    logger_level: str,
) -> threading.Thread:
    """Start a daemon that produces a synthetic flush tick every interval."""

    def run() -> None:
        while not stop_event.wait(interval_seconds):
            try:
                message = topic.serialize(
                    key=state_key, value={"type": TYPE_FLUSH_TICK, "ts": now_ms()}
                )
                producer.produce(topic=topic.name, key=message.key, value=message.value)
                producer.poll(0)
            except Exception as exc:  # never crash the ticker
                if logger_level != "off":
                    print(f"[BILLING-SINK] flush tick failed: {exc}", flush=True)

    thread = threading.Thread(target=run, name="flush-ticker", daemon=True)
    thread.start()
    return thread
