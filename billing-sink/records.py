"""Billing event record construction and request validation (spec section 7).

Pure, I/O-free helpers so the HTTP handler stays thin and the 14-column row
shape (spec section 7.1) is smoke-testable without a broker or Lakehouse.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

# The Lakehouse row columns (spec section 7.1), in schema order. environment_id,
# deployment_id and event_month become Hive partition path segments at sink time
# but are still logical columns of the table.
SINK_COLUMNS = (
    "event_id",
    "credit_type",
    "duration_ms",
    "environment_id",
    "deployment_id",
    "operation",
    "payload",
    "received_at",
    "event_datetime",
    "event_month",
    "batch_id",
    "sink_ts",
    "sink_deployment_id",
    "schema_version",
)


class ValidationError(Exception):
    """Raised when an incoming billing request is invalid (maps to HTTP 400)."""


def parse_duration_ms(raw: str) -> int:
    """Parse the ``{time-in-ms}`` path segment into a non-negative int."""
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValidationError("time-in-ms must be an integer") from exc
    if value < 0:
        raise ValidationError("time-in-ms must be non-negative")
    return value


def require_header(name: str, value: str | None) -> str:
    """Return a stripped, non-empty header value or raise ValidationError."""
    if value is None or not value.strip():
        raise ValidationError(f"missing or empty header {name}")
    return value.strip()


def require_credit_type(credit_type: str) -> str:
    """Return a non-empty credit-type or raise ValidationError."""
    if not credit_type or not credit_type.strip():
        raise ValidationError("credit-type must be non-empty")
    return credit_type


def extract_operation(payload: str | None) -> str | None:
    """Lift ``operation`` from a JSON-object body; None for absent/malformed."""
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except (ValueError, TypeError):
        return None
    if isinstance(parsed, dict):
        operation = parsed.get("operation")
        return operation if isinstance(operation, str) else None
    return None


def now_ms() -> int:
    """Current wall-clock time in epoch milliseconds."""
    return int(time.time() * 1000)


def build_event_record(
    *,
    credit_type: str,
    duration_ms: int,
    environment_id: str,
    deployment_id: str,
    payload: str | None,
    schema_version: int,
    received_at: int | None = None,
) -> dict:
    """Build the ingest-time record, injecting id/time fields (spec section 7.1).

    The flush-time fields (batch_id, sink_ts, sink_deployment_id) are added later
    by :func:`enrich_for_sink`; they are absent from the pending record.
    """
    received_at = received_at if received_at is not None else now_ms()
    moment = datetime.fromtimestamp(received_at / 1000, tz=timezone.utc)
    return {
        "event_id": uuid.uuid4().hex,
        "credit_type": credit_type,
        "duration_ms": duration_ms,
        "environment_id": environment_id,
        "deployment_id": deployment_id,
        "operation": extract_operation(payload),
        "payload": payload,
        "received_at": received_at,
        "event_datetime": moment.isoformat(),
        "event_month": moment.strftime("%Y-%m"),
        "schema_version": schema_version,
    }


def enrich_for_sink(
    record: dict, *, batch_id: str, sink_ts: int, sink_deployment_id: str
) -> dict:
    """Return a copy of ``record`` with the flush-time columns added."""
    row = dict(record)
    row["batch_id"] = batch_id
    row["sink_ts"] = sink_ts
    row["sink_deployment_id"] = sink_deployment_id
    return row
