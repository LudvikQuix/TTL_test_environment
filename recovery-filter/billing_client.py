"""Fire-and-forget billing client for recovery-filter (spec section 5.7 + A1).

A bounded queue plus one daemon worker POSTs credit-spend events to the
billing-sink. It NEVER blocks or raises into the SDF thread: a full queue drops
(counter++), a POST error logs at info and drops (no retries). Every POST carries
the environment/deployment headers and an ``Authorization: Bearer`` token.
Disabled cleanly via BILLING_ENABLED for broker-less local runs.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading

import requests

logger = logging.getLogger(__name__)

_TRUE = ("1", "true", "yes", "on")


class BillingClient:
    """Async, best-effort emitter of billing POSTs to the billing-sink."""

    def __init__(
        self,
        *,
        enabled: bool,
        url: str,
        token: str,
        environment_id: str,
        deployment_id: str,
        timeout: float,
        queue_maxsize: int,
    ):
        self._enabled = enabled
        self._url = url.rstrip("/")
        self._timeout = timeout
        self._headers = {
            "Content-Type": "application/json",
            "X-Environment-Id": environment_id,
            "X-Deployment-Id": deployment_id,
        }
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        self._queue: queue.Queue = queue.Queue(maxsize=queue_maxsize)
        self._dropped = 0
        if self._enabled:
            worker = threading.Thread(
                target=self._run, name="billing-client", daemon=True
            )
            worker.start()

    @property
    def dropped(self) -> int:
        return self._dropped

    def emit(self, credit_type: str, time_in_ms: int, body: dict | None = None) -> None:
        """Enqueue a billing POST; drop (counter++) if disabled or queue full.

        Non-blocking and never raises -- safe to call from the SDF thread.
        """
        if not self._enabled:
            return
        try:
            self._queue.put_nowait((credit_type, int(time_in_ms), body or {}))
        except queue.Full:
            self._dropped += 1

    def emit_now(
        self, credit_type: str, time_in_ms: int, body: dict | None = None
    ) -> None:
        """Blocking best-effort POST for shutdown-time events; never raises.

        Used after ``app.run()`` returns (graceful shutdown), where the daemon
        worker would die with the process before draining the queue.
        """
        if not self._enabled:
            return
        try:
            self._post(credit_type, int(time_in_ms), body or {})
        except Exception as exc:  # never propagate at shutdown
            logger.info("[BILLING-CLIENT] shutdown POST %s failed: %s", credit_type, exc)

    def _run(self) -> None:
        while True:
            credit_type, time_in_ms, body = self._queue.get()
            try:
                self._post(credit_type, time_in_ms, body)
            except Exception as exc:  # never propagate into anything
                logger.info(
                    "[BILLING-CLIENT] dropped POST %s/%s: %s",
                    credit_type,
                    time_in_ms,
                    exc,
                )
            finally:
                self._queue.task_done()

    def _post(self, credit_type: str, time_in_ms: int, body: dict) -> None:
        url = f"{self._url}/billing/{credit_type}/{time_in_ms}"
        requests.post(
            url, data=json.dumps(body), headers=self._headers, timeout=self._timeout
        )


def _bool_env(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in _TRUE


def build_billing_client() -> BillingClient:
    """Construct a BillingClient from BILLING_* env vars (spec 7.4 + A1)."""
    token = (
        os.environ.get("BILLING_TOKEN", "").strip()
        or os.environ.get("Quix__Sdk__Token", "")
    )
    environment_id = (
        os.environ.get("BILLING_ENVIRONMENT_ID", "").strip()
        or os.environ.get("Quix__Workspace__Id", "")
    )
    deployment_id = (
        os.environ.get("BILLING_DEPLOYMENT_ID", "").strip()
        or os.environ.get("Quix__Deployment__Id", "")
    )
    return BillingClient(
        enabled=_bool_env("BILLING_ENABLED", "true"),
        url=os.environ.get("BILLING_URL", "http://billing-sink"),
        token=token,
        environment_id=environment_id,
        deployment_id=deployment_id,
        timeout=float(os.environ.get("BILLING_TIMEOUT_SECONDS", "2.0")),
        queue_maxsize=int(os.environ.get("BILLING_QUEUE_MAXSIZE", "1000")),
    )
