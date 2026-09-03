"""
Quix Lakehouse Sink - Main Entry Point

This application consumes data from a Kafka topic and writes it to blob storage as
Hive-partitioned Parquet files with optional Iceberg catalog registration.

Blob storage is configured via the Quix__BlobStorage__Connection__Json environment variable,
which is automatically handled by the quixportal library. The bucket name is extracted
automatically from this configuration.

File paths follow the workspace-aware structure:
    {workspaceId}/data-lake/time-series/{table_name}/...

TTL test environment delta — quix-streams PR #1143 (pinned at be5e740c). This
copy of the connector switches on the three parameters that PR added so they are
observable on a live pipeline:
  * `hive_columns` carries a `~`-prefixed VIRTUAL entry (`~order_id`): no
    `key=value/` folder, no file split, the column stays in the parquet and is
    indexed in `.vidx` sidecars. `order_id` spans 40M values here, so a physical
    partition would emit one tiny file per order per batch.
  * `sort_column` is recorded as `properties.sort_column` when the table is
    CREATED (an already-registered table is validated, never updated). It is
    deliberately NOT `timestamp_column`, because an unset `sort_column` falls
    back to the timestamp column — setting the two equal proves nothing.
    Blanking `SORT_COLUMN` in the Portal is what selects that unset arm; the
    default only applies when the variable is absent (see `_envstr_unsettable`).
  * `stats_columns` unset means per-file min/max zone maps for every numeric and
    timestamp column, here `timestamp` plus the stamped `seq`.

`HIVE_COLUMNS` is not a runtime tweak: the sink validates the partition set
against the catalog spec AND the on-disk Hive paths in `setup()` and raises on a
mismatch. Changing it, `TIMESTAMP_COLUMN` or `SORT_COLUMN` means a new table —
bump `TABLE_VERSION`.
"""
import itertools
import os
import re
import logging

from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink


# The Quix Portal happily stores an env var with an EMPTY value, and
# os.getenv(name, "10") then returns "" — not the default — so a bare int() or a
# bare `.lower() == "true"` misreads Portal config at import time. Repo
# convention (dedup-cf-b/main.py:70-80): treat empty/whitespace as "unset".
# The sample's own `_positive_int` covers the integer reads once it goes through
# `_envstr`, so no separate `_envint`/`_envfloat` is needed here. Two variables
# need the opposite reading of an empty value — see `_envstr_unsettable`.
def _envstr(env_var: str, default: str = "") -> str:
    raw = os.getenv(env_var, "").strip()
    return raw if raw else default


def _envstr_unsettable(env_var: str, default: str) -> str:
    """Resolve a var whose EMPTY value is meaningful, not a request for the default.

    Absent -> ``default``. Present but blank -> ``""`` (explicitly unset).
    Present with a value -> that value, stripped. Contrast ``_envstr``, where
    blank means "fall back to the default" — right for every var whose blank
    state has no distinct meaning, wrong for the two that have one: SORT_COLUMN
    (blank = no sort column, fall back to the timestamp column) and
    TABLE_VERSION (blank = no table-name suffix).
    """
    raw = os.getenv(env_var)
    if raw is None:
        return default
    return raw.strip()


def _envflag(env_var: str, default: str = "0") -> bool:
    return _envstr(env_var, default).lower() in ("1", "true", "yes", "on")


# Configure logging
logging.basicConfig(
    level=_envstr("LOGLEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constant for time-series data lake path structure
TIMESERIES_PREFIX = "data-lake/time-series"

# Column stamped on every record before the sink (see `stamp_seq`).
SEQ_COLUMN = "seq"


_TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*$')


def _positive_int(env_var: str, default: str) -> int:
    raw = _envstr(env_var, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{env_var} must be a positive integer, got '{raw}'")
    if value <= 0:
        raise ValueError(f"{env_var} must be a positive integer, got {value}")
    return value


def parse_hive_columns(columns_str: str) -> list:
    """
    Parse comma-separated list of partition columns.

    Only whitespace is stripped, so a leading `~` (the virtual-partition marker)
    survives intact — splitting physical from virtual entries is the sink's own
    job, done in its constructor. Also used for STATS_COLUMNS, which is the same
    comma-separated-column-names shape.

    Args:
        columns_str: Comma-separated column names (e.g., "year,month,day")

    Returns:
        List of column names, or empty list if input is empty
    """
    if not columns_str or columns_str.strip() == "":
        return []
    return [col.strip() for col in columns_str.split(",") if col.strip()]


# Initialize Quix Streams Application. `broker_address` is read from KAFKA_BOOTSTRAP_SERVERS for
# local-dev convenience; in Quix Cloud it stays None and the Application picks up Quix__Broker__*
# from the platform.
app = Application(
    broker_address=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    consumer_group=_envstr("CONSUMER_GROUP", "s3_direct_sink_v1.0"),
    auto_offset_reset=_envstr("AUTO_OFFSET_RESET", "earliest"),
    commit_interval=_positive_int("COMMIT_INTERVAL", "30"),
    commit_every=_positive_int("BATCH_SIZE", "1000")
)

# Parse configuration
hive_columns = parse_hive_columns(
    _envstr("HIVE_COLUMNS", "year,month,day,hour,status,~order_id")
)
auto_discover = _envflag("AUTO_DISCOVER", "true")
# `timestamp` is the generator's epoch-milliseconds field (see
# duplicate-key-generator/main.py:43-49); year/month/day/hour are derived from it.
timestamp_column = _envstr("TIMESTAMP_COLUMN", "timestamp")
# Absent -> SEQ_COLUMN; blanked in the Portal -> "" -> None, which is how the
# documented fallback to timestamp_column is reached without a rebuild. Needs
# `_envstr_unsettable` precisely because the default here is non-empty.
sort_column = _envstr_unsettable("SORT_COLUMN", SEQ_COLUMN) or None
# Empty means "unset" -> None -> the sink computes stats for every numeric and
# timestamp column of each written file.
stats_columns = parse_hive_columns(_envstr("STATS_COLUMNS")) or None
catalog_url = os.getenv("Quix__Lakehouse__Catalog__Url") or os.getenv("CATALOG_URL")
# The table name carries a version suffix (same convention as CG_VERSION on the
# dedup services). HIVE_COLUMNS/TIMESTAMP_COLUMN/SORT_COLUMN changes need a fresh
# table — bump TABLE_VERSION rather than editing them in place. Absent -> "v1";
# blanked in the Portal -> no suffix, i.e. the bare TABLE_NAME.
table_name = _envstr("TABLE_NAME", "ttl_events")
table_version = _envstr_unsettable("TABLE_VERSION", "v1")
if table_version:
    table_name = f"{table_name}_{table_version}"
if not _TABLE_NAME_PATTERN.match(table_name):
    raise ValueError(
        f"Invalid table name '{table_name}'. Table names must start with a letter or digit "
        f"and may only contain letters, digits, dots (.), hyphens (-), and underscores (_)."
    )

# Workspace ID (automatically injected by Quix platform)
workspace_id = os.getenv("Quix__Workspace__Id", "")

# Initialize QuixLakeSink
# Note: Blob storage credentials are configured via Quix__BlobStorage__Connection__Json
# environment variable, which is automatically read by quixportal.
# The bucket name is extracted automatically from the quixportal configuration.
# Quix Portal injects the Catalog URL under both the Quix naming convention
# (`Quix__Lakehouse__Catalog__Url`) and the PyIceberg one (`CATALOG_URL`) when a Lakehouse Catalog
# deployment exists in the workspace; prefer the Quix name, fall back to the PyIceberg one for
# legacy compatibility. The auth token is only injected under the Quix name — it routes via the
# secrets-bag / secretKeyRef path that the platform uses for the Catalog's own credentials.
blob_sink = QuixTSDataLakeSink(
    s3_prefix=TIMESERIES_PREFIX,
    table_name=table_name,
    workspace_id=workspace_id,
    hive_columns=hive_columns,
    timestamp_column=timestamp_column,
    sort_column=sort_column,
    catalog_url=catalog_url,
    catalog_auth_token=os.getenv("Quix__Lakehouse__Catalog__AuthToken"),
    auto_discover=auto_discover,
    namespace=_envstr("CATALOG_NAMESPACE", "default"),
    auto_create_bucket=True,
    max_workers=_positive_int("MAX_WRITE_WORKERS", "10"),
    stats_columns=stats_columns,
    on_client_connect_success=lambda: print("CONNECTED!"),
    on_client_connect_failure=lambda e: print(f"ERROR! {e}"),
)

# Monotonic per-record sequence number, stamped before the sink so `sort_column`
# points at a column that is NOT `timestamp_column`, and so the per-file
# statistics have a second numeric column to index.
#
# Process-local by design: the counter restarts at 0 on every restart, redeploy
# and partition rebalance, so `seq` is monotonic only within one replica's
# lifetime and its values repeat across restarts. That is enough to exercise the
# sort/stats metadata — it is not a global sequence.
_seq = itertools.count()


def stamp_seq(value: dict) -> None:
    """Stamp the process-local sequence number onto the record, in place."""
    value[SEQ_COLUMN] = next(_seq)


# Create streaming dataframe and attach sink
sdf = app.dataframe(topic=app.topic(os.environ["input"]))
sdf = sdf.update(stamp_seq)

# Attach sink (batching is handled by BatchingSink)
sdf.sink(blob_sink)

# Log startup configuration
storage_path = f"{workspace_id}/{TIMESERIES_PREFIX}" if workspace_id else TIMESERIES_PREFIX
physical_columns = [col for col in hive_columns if not col.startswith("~")]
virtual_columns = [col[1:] for col in hive_columns if col.startswith("~")]
sort_column_display = sort_column or f"unset -> falls back to {timestamp_column}"
stats_columns_display = stats_columns or "unset -> every numeric/timestamp column"
# No catalog means no partition_spec, no properties.virtual_partitions, no
# properties.sort_column and no column_stats anywhere — only the blob layout and
# the .vidx sidecars remain observable. On dev the whole Quix__Lakehouse__*
# bundle is injected by blobStorage.bind, so an unresolved URL points there.
catalog_display = "resolved" if catalog_url else "NOT RESOLVED (check blobStorage.bind)"
logger.info("Starting Quix Lakehouse Sink")
logger.info(f"  Input topic: {os.environ['input']}")
logger.info(f"  Storage path: {storage_path}/{table_name}")
logger.info(f"  Partitioning: {hive_columns if hive_columns else 'none'}")
logger.info(f"    physical (own folder, dropped from parquet): {physical_columns}")
logger.info(f"    virtual (no folder, kept in parquet): {virtual_columns}")
logger.info(f"  Timestamp column: {timestamp_column}")
logger.info(f"  Sort column: {sort_column_display}")
logger.info(f"  Stats columns: {stats_columns_display}")
logger.info(f"  Catalog URL: {catalog_display}")

if __name__ == "__main__":
    app.run()