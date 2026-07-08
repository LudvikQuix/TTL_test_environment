"""State mirror + RAM buffer for pending (not-yet-sunk) billing events.

QuixStreams State (RocksDB + changelog) is the durable source of truth for the
pending buffer; the RAM :class:`PendingBuffer` is a derived cache for fast batch
assembly, rebuilt from State on the first message after boot and safe to lose
(spec section 5.2). All State access happens in-context inside the single
stateful SDF op; the HTTP thread only reads the health snapshot.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import timedelta
from itertools import islice

# State key layout inside the single per-STATE_KEY store (spec section 5.2).
RECORD_PREFIX = "record:"
SUNK_PREFIX = "_sunk:"
PENDING_INDEX = "_pending_index"
LAST_FLUSH_TS = "_last_flush_ts"


def is_replay(state, event_id: str) -> bool:
    """True if the event was already sunk or is already pending (idempotency).

    Uses ``state.get(...) is not None`` rather than ``exists`` for portability;
    an expired ``_sunk`` TTL entry reads back as None and is no longer a replay
    (spec section 5.5).
    """
    return (
        state.get(SUNK_PREFIX + event_id) is not None
        or state.get(RECORD_PREFIX + event_id) is not None
    )


def add_pending(state, record: dict) -> None:
    """Persist a new pending record and append it to the ordered index."""
    event_id = record["event_id"]
    state.set(RECORD_PREFIX + event_id, record)
    index = state.get(PENDING_INDEX, [])
    index.append(event_id)
    state.set(PENDING_INDEX, index)


def read_pending_records(state) -> list[dict]:
    """Read all pending records from State, in index order (used at boot)."""
    index = state.get(PENDING_INDEX, [])
    records = []
    for event_id in index:
        record = state.get(RECORD_PREFIX + event_id)
        if record is not None:
            records.append(record)
    return records


def pending_count(state) -> int:
    """Number of pending event ids recorded in State."""
    return len(state.get(PENDING_INDEX, []))


def confirm_sunk(state, event_ids: list[str], ttl_seconds: int, now_ms: int) -> None:
    """Delete sunk records, drop dedup markers with TTL, trim index, stamp flush.

    Called only after a confirmed Lakehouse write (spec section 5.4). The
    ``_sunk`` TTL must exceed the consumer commit window so a replay before the
    offset commits is still recognised (spec section 5.5).
    """
    sunk = set(event_ids)
    ttl = timedelta(seconds=ttl_seconds)
    for event_id in event_ids:
        state.delete(RECORD_PREFIX + event_id)
        state.set(SUNK_PREFIX + event_id, 1, ttl=ttl)
    index = state.get(PENDING_INDEX, [])
    state.set(PENDING_INDEX, [eid for eid in index if eid not in sunk])
    state.set(LAST_FLUSH_TS, now_ms)


class PendingBuffer:
    """Process-local cache of pending records plus a health snapshot.

    Mutated only by the single stateful SDF thread (one writer); the HTTP
    ``/healthz`` handler calls :meth:`health` (reader). A lock guards the
    snapshot so cross-thread reads are consistent.
    """

    def __init__(self):
        self._records: OrderedDict[str, dict] = OrderedDict()
        self._rebuilt = False
        self._lock = threading.Lock()
        self._last_flush_ts = 0
        self._batches_sunk = 0
        self._dropped_replays = 0
        self._pending_state_count = 0

    @property
    def is_rebuilt(self) -> bool:
        return self._rebuilt

    def ensure_rebuilt(self, records: list[dict]) -> None:
        """Populate the RAM cache from State exactly once after boot."""
        if self._rebuilt:
            return
        with self._lock:
            self._records.clear()
            for record in records:
                self._records[record["event_id"]] = record
            self._pending_state_count = len(self._records)
            self._rebuilt = True

    def add(self, record: dict) -> None:
        with self._lock:
            self._records[record["event_id"]] = record

    def snapshot(self, limit: int) -> list[dict]:
        """Up to ``limit`` oldest pending records, for batch assembly."""
        with self._lock:
            return list(islice(self._records.values(), limit))

    def remove_many(self, event_ids: list[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                self._records.pop(event_id, None)

    def note_flush(self, ts: int, pending_state_count: int) -> None:
        with self._lock:
            self._last_flush_ts = ts
            self._batches_sunk += 1
            self._pending_state_count = pending_state_count

    def note_dropped_replay(self) -> None:
        with self._lock:
            self._dropped_replays += 1

    def set_pending_state_count(self, count: int) -> None:
        with self._lock:
            self._pending_state_count = count

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def health(self) -> dict:
        with self._lock:
            return {
                "buffer_size": len(self._records),
                "pending_state_count": self._pending_state_count,
                "last_flush_ts": self._last_flush_ts,
                "batches_sunk": self._batches_sunk,
                "dropped_replays": self._dropped_replays,
            }
