"""HTTP ingress for billing-sink (FastAPI, run on a uvicorn worker thread).

QuixStreams has no HTTP-ingest Source and ``app.run()`` owns the main thread, so
the endpoint runs off-thread and hands events to the pipeline by producing onto
the billing-events topic (spec section 5.1). State is in-context-only and is
never touched here; ``/healthz`` reads a process-local snapshot the SDF updates.
"""

from __future__ import annotations

from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auth import AuthDecision, Authorizer
from config import BillingConfig
from records import (
    ValidationError,
    build_event_record,
    parse_duration_ms,
    require_credit_type,
    require_header,
)
from state_buffer import PendingBuffer

PublishFn = Callable[[dict], None]

# AuthDecision -> (HTTP status, error body). ALLOW is handled inline.
_AUTH_RESPONSES = {
    AuthDecision.UNAUTHENTICATED: (401, "missing bearer token"),
    AuthDecision.FORBIDDEN: (403, "forbidden"),
    AuthDecision.UNAVAILABLE: (503, "auth backend unavailable"),
}


def create_app(
    config: BillingConfig,
    buffer: PendingBuffer,
    publish: PublishFn,
    authorizer: Authorizer,
) -> FastAPI:
    """Build the FastAPI app. ``publish`` enqueues a record onto billing-events."""
    app = FastAPI(title="billing-sink", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    def healthz() -> dict:
        snapshot = buffer.health()
        snapshot["status"] = "ok"
        return snapshot

    @app.post("/billing/{credit_type}/{time_in_ms}")
    async def ingest(credit_type: str, time_in_ms: str, request: Request) -> JSONResponse:
        # 1. Authorize first (spec Amendment A1): 401 / 403 / 503 before any 400.
        decision = authorizer.authorize(request.headers.get("Authorization"))
        if decision is not AuthDecision.ALLOW:
            status, message = _AUTH_RESPONSES[decision]
            return JSONResponse(status_code=status, content={"error": message})

        # 2. Validate request shape (spec section 7.2) -> 400.
        try:
            credit_type = require_credit_type(credit_type)
            duration_ms = parse_duration_ms(time_in_ms)
            environment_id = require_header(
                "X-Environment-Id", request.headers.get("X-Environment-Id")
            )
            deployment_id = require_header(
                "X-Deployment-Id", request.headers.get("X-Deployment-Id")
            )
        except ValidationError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

        # 3. Body is stored raw; a JSON object also yields `operation`.
        raw = await request.body()
        payload = raw.decode("utf-8", errors="replace") if raw else None

        record = build_event_record(
            credit_type=credit_type,
            duration_ms=duration_ms,
            environment_id=environment_id,
            deployment_id=deployment_id,
            payload=payload,
            schema_version=config.schema_version,
        )
        publish(record)  # POST -> Kafka durability; SDF mirrors to State + sinks.
        return JSONResponse(
            status_code=202,
            content={
                "event_id": record["event_id"],
                "received_at": record["received_at"],
                "status": "buffered",
            },
        )

    return app
