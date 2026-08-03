"""Environment-driven configuration for the billing-sink service.

Every tunable comes from an env var (spec sections 7.3 + Amendment A1) with a
safe default and, where relevant, a local-dev fallback so the same code runs on
and off cluster. No business threshold is hard-coded outside this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Hive partition columns for the Lakehouse table (spec section 7.1), passed as
# hive_columns to QuixLakeClient.insert and documented here so the layout has
# one source.
PARTITION_COLUMNS = ["environment_id"]

_TRUE = ("1", "true", "yes", "on")


def resolve_logger_level(raw: str | None) -> str:
    """Normalize LOGGER into 'off' | 'info' | 'debug' (legacy 'on' -> 'info')."""
    value = (raw or "").strip().lower()
    if value == "on":
        return "info"
    if value in ("off", "info", "debug"):
        return value
    return "info"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _bool(name: str, default: str) -> bool:
    return _env(name, default).strip().lower() in _TRUE


def _resolve_state_key() -> str:
    """STATE_KEY if set, else the deployment id, else the literal 'billing-sink'.

    A single constant key means one stream_id -> one State store the flush op can
    accumulate in (spec section 5.2).
    """
    explicit = _env("STATE_KEY").strip()
    if explicit:
        return explicit
    return _env("Quix__Deployment__Id").strip() or "billing-sink"


@dataclass(frozen=True)
class BillingConfig:
    # HTTP + pipeline
    http_port: int
    batch_size: int
    flush_interval_seconds: int
    events_topic: str
    consumer_group: str
    state_key: str
    state_dir: str
    logger_level: str
    dedup_ttl_seconds: int
    schema_version: int
    # Lakehouse (writes go via the Query API /insert; URL+token auto-inject on dev)
    lake_table: str
    workspace_id: str
    deployment_id: str
    query_url: str | None
    query_token: str | None
    # Flush retry backoff (parameterizes the spec section 5.4 bounded-backoff)
    flush_retry_base_ms: int
    flush_retry_cap_ms: int
    # Auth (spec Amendment A1)
    auth_enabled: bool
    auth_cache_seconds: float
    auth_required_permission: str

    @property
    def flush_interval_ms(self) -> int:
        return self.flush_interval_seconds * 1000


def load_config() -> BillingConfig:
    """Read the environment into an immutable config snapshot."""
    return BillingConfig(
        http_port=int(_env("HTTP_PORT", "80")),
        batch_size=int(_env("BATCH_SIZE", "500")),
        flush_interval_seconds=int(_env("FLUSH_INTERVAL_SECONDS", "30")),
        events_topic=_env("BILLING_TOPIC", "billing-events"),
        consumer_group=_env("CONSUMER_GROUP", "billing-sink-v1"),
        state_key=_resolve_state_key(),
        state_dir=_env("STATE_DIR", "state"),
        logger_level=resolve_logger_level(_env("LOGGER", "info")),
        dedup_ttl_seconds=int(_env("DEDUP_TTL_SECONDS", "600")),
        schema_version=int(_env("SCHEMA_VERSION", "1")),
        lake_table=_env("LAKE_TABLE", "billing_events"),
        workspace_id=_env("Quix__Workspace__Id"),
        deployment_id=_env("Quix__Deployment__Id"),
        query_url=(_env("Quix__Lakehouse__Query__Url") or _env("QUIXLAKE_URL")) or None,
        query_token=(_env("Quix__Lakehouse__Query__AuthToken") or _env("QUIX_LAKE_TOKEN"))
        or None,
        flush_retry_base_ms=int(_env("FLUSH_RETRY_BASE_MS", "1000")),
        flush_retry_cap_ms=int(_env("FLUSH_RETRY_CAP_MS", "60000")),
        auth_enabled=_bool("AUTH_ENABLED", "true"),
        auth_cache_seconds=float(_env("AUTH_CACHE_SECONDS", "300")),
        auth_required_permission=_env("AUTH_REQUIRED_PERMISSION", "Write"),
    )
