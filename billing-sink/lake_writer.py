"""Lakehouse write backend behind a thin, swappable interface (spec section 5.6).

``LakehouseWriter.write_batch(rows)`` performs a single blocking write of a row
batch and RAISES on any failure, so the flush op only deletes State after a
confirmed sink. Shipped backend: ``QuixTSDataLakeSink`` -- it writes
Hive-partitioned Parquet to blob storage via quixportal (creds from
``Quix__BlobStorage__Connection__Json``) and registers the files in the Iceberg
REST catalog. Swap or add a backend in :func:`build_lakehouse_writer` without
touching flush/flow logic.
"""

from __future__ import annotations

from typing import Protocol

from quixstreams.sinks.base import SinkBatch
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

from config import PARTITION_COLUMNS, BillingConfig


class LakehouseWriter(Protocol):
    """Write a batch of rows to the Lakehouse, raising on any failure."""

    def write_batch(self, rows: list[dict]) -> None: ...


class QuixTSDataLakeWriter:
    """Adapts :class:`QuixTSDataLakeSink` to the synchronous LakehouseWriter API.

    The sink is a framework ``BatchingSink`` normally driven by the commit cycle;
    here we drive it directly -- build a one-shot ``SinkBatch`` and call
    ``write()`` inline -- so the confirmation and the State delete happen in the
    same in-context flush (spec section 5.4).
    """

    def __init__(self, config: BillingConfig, *, timestamp_column: str = "received_at"):
        self._key = config.state_key
        self._topic = config.events_topic
        self._sink = QuixTSDataLakeSink(
            s3_prefix=config.lake_s3_prefix,
            table_name=config.lake_table,
            workspace_id=config.workspace_id,
            hive_columns=list(PARTITION_COLUMNS),
            timestamp_column=timestamp_column,
            catalog_url=config.catalog_url,
            catalog_auth_token=config.catalog_token,
        )
        self._ready = False

    def _ensure_ready(self) -> None:
        # One-time client/catalog setup, deferred so construction needs no creds.
        if not self._ready:
            self._sink.setup()
            self._ready = True

    def write_batch(self, rows: list[dict]) -> None:
        if not rows:
            return
        self._ensure_ready()
        batch = SinkBatch(topic=self._topic, partition=0)
        for offset, row in enumerate(rows):
            batch.append(
                value=row,
                key=self._key,
                timestamp=int(row.get("received_at", 0)),
                headers=[],
                offset=offset,
            )
        self._sink.write(batch)  # blocks; raises on failure (no partial confirm)


def build_lakehouse_writer(config: BillingConfig) -> LakehouseWriter:
    """Return the configured Lakehouse backend.

    Only the QuixTSDataLakeSink backend is shipped -- verified importable in the
    pinned quixstreams build, and it already performs the parquet-to-blob +
    Iceberg-REST registration a manual fallback would reimplement. A fallback
    backend would be selected here without touching flush logic.
    """
    return QuixTSDataLakeWriter(config)
