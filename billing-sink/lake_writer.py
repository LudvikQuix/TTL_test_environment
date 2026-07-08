"""Lakehouse write backend behind a thin, swappable interface (spec Amendment A2).

``LakehouseWriter.write_batch(rows)`` performs a single blocking write of a row
batch and RAISES on any failure, so the flush op only deletes State after a
confirmed sink. Shipped backend: the Quix Lakehouse **Query API** via
``quixlake-sdk`` (``QuixLakeClient.insert``). Writes go through the lake service,
which persists parquet and maintains the Iceberg catalog server-side -- our code
never touches blob storage directly (direct blob writes bypass the service and
corrupt the catalog). Swap backends in :func:`build_lakehouse_writer` without
touching flush/flow logic.
"""

from __future__ import annotations

import logging
from typing import Protocol

import pandas as pd
from quixlake import QuixLakeClient

from config import PARTITION_COLUMNS, BillingConfig
from records import SINK_COLUMNS

logger = logging.getLogger(__name__)


class LakehouseWriter(Protocol):
    """Write a batch of rows to the Lakehouse, raising on any failure."""

    def write_batch(self, rows: list[dict]) -> None: ...


class QuixLakeWriter:
    """Adapts ``QuixLakeClient.insert`` to the synchronous LakehouseWriter API.

    ``insert`` is synchronous (``async_mode=False``): 200 -> confirmed, 409 ->
    ``ValueError`` (partition mismatch), any other non-200 -> ``raise_for_status``.
    Every failure propagates so the flush op keeps the batch pending in State and
    the existing retry/backoff runs -- unchanged by this backend swap.
    """

    def __init__(self, config: BillingConfig, *, client: QuixLakeClient | None = None):
        self._table = config.lake_table
        self._hive_columns = list(PARTITION_COLUMNS)
        self._config = config
        self._client = client  # injectable for tests; built lazily otherwise

    def _get_client(self) -> QuixLakeClient:
        if self._client is None:
            self._client = QuixLakeClient(
                base_url=self._config.query_url,
                token=self._config.query_token,
            )
        return self._client

    def write_batch(self, rows: list[dict]) -> None:
        if not rows:
            return
        # Fixed column order = the 14-col schema (spec 7.1); present in every row.
        frame = pd.DataFrame(rows, columns=list(SINK_COLUMNS))
        try:
            self._get_client().insert(
                table_name=self._table,
                data=frame,
                hive_columns=self._hive_columns,
            )
        except ValueError as exc:
            # 409 partition mismatch: structurally non-retryable. Log loudly and
            # re-raise so the batch stays pending in State; the controller backoff
            # caps the retry rate (no tight loop). Never fall back to direct blob.
            logger.error(
                "[BILLING-SINK] LAKE PARTITION MISMATCH (409) on insert into "
                "table=%s hive=%s -- batch kept pending, needs operator "
                "attention: %s",
                self._table,
                self._hive_columns,
                exc,
            )
            raise


def build_lakehouse_writer(config: BillingConfig) -> LakehouseWriter:
    """Return the configured Lakehouse backend.

    Only the QuixLakeClient (Query API ``/insert``) backend is shipped -- it keeps
    all parquet + Iceberg-catalog maintenance server-side. A different backend
    would be selected here without touching flush logic.
    """
    return QuixLakeWriter(config)
