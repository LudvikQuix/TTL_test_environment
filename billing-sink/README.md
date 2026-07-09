# Billing Sink — POST API Guide

`billing-sink` accepts credit-spend events from any Quix deployment, buffers them durably, and lands them in the Lakehouse `billing_events` table in configurable batches. The stored value is raw `duration_ms` — there is no currency or credit conversion anywhere in this service.

---

## Endpoint reference

```
POST /billing/{credit-type}/{time-in-ms}
```

| Parameter | Where | Type | Required | Description |
|---|---|---|---|---|
| `{credit-type}` | URL path | string | yes | Freeform label for the operation being billed (e.g. `quixlab-cell-exec`, `backfill-action`). Must be non-empty. |
| `{time-in-ms}` | URL path | integer | yes | Raw wall-clock duration in milliseconds. Must be a non-negative integer. |
| `Authorization` | header | `Bearer <token>` | yes | SDK token or PAT for the environment (see [Auth](#auth)). |
| `X-Environment-Id` | header | string | yes | The caller's Quix environment ID. |
| `X-Deployment-Id` | header | string | yes | The caller's Quix deployment ID. |
| `Content-Type` | header | `application/json` | no | Include when sending a JSON body. |
| Request body | body | JSON object | no | Any JSON object. Stored raw as `payload`. If the object contains an `operation` key its value is also indexed separately. A malformed or absent body is accepted; `operation` is set to null. |

---

## Auth

Every `POST /billing/...` call must include:

```
Authorization: Bearer <token>
```

Both an **environment SDK token** (`Quix__Sdk__Token`) and a **PAT** are accepted. The token must have at least `Write` permission on the target environment.

**Finding your token inside a Quix deployment.** Quix injects these env vars into every deployment automatically — you do not configure them manually:

| Env var | Contains |
|---|---|
| `Quix__Sdk__Token` | Environment SDK token (use this for the `Authorization` header) |
| `Quix__Workspace__Id` | Your environment ID (use this for `X-Environment-Id`) |
| `Quix__Deployment__Id` | Your deployment ID (use this for `X-Deployment-Id`) |

`GET /healthz` does not require a token.

---

## Examples

### curl (from inside the cluster)

```bash
curl -s -X POST http://billing-sink/billing/quixlab-cell-exec/12345 \
  -H "Authorization: Bearer ${Quix__Sdk__Token}" \
  -H "X-Environment-Id: ${Quix__Workspace__Id}" \
  -H "X-Deployment-Id: ${Quix__Deployment__Id}" \
  -H "Content-Type: application/json" \
  -d '{"operation":"quixlab.cell.execute","notebook":"n1"}'
```

Expected response (`202`):

```json
{"event_id":"9f1c2e...","received_at":1720000000000,"status":"buffered"}
```

### Python — fire-and-forget (mirror of `recovery-filter/billing_client.py`)

```python
import json, os, requests

BILLING_URL = os.environ.get("BILLING_URL", "http://billing-sink")
token = os.environ.get("Quix__Sdk__Token", "")
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
    "X-Environment-Id": os.environ.get("Quix__Workspace__Id", ""),
    "X-Deployment-Id": os.environ.get("Quix__Deployment__Id", ""),
}

def post_billing_event(credit_type: str, time_in_ms: int, body: dict) -> None:
    """Best-effort POST — never raise into the caller."""
    try:
        requests.post(
            f"{BILLING_URL}/billing/{credit_type}/{time_in_ms}",
            data=json.dumps(body),
            headers=headers,
            timeout=2.0,
        )
    except Exception as exc:
        # Log and continue — a billing failure must never crash the caller.
        print(f"[BILLING] dropped {credit_type}/{time_in_ms}: {exc}")
```

---

## Response reference

| Status | Meaning | What to do |
|---|---|---|
| `202 Accepted` | Event accepted and buffered. Body: `{"event_id":"...","received_at":<epoch ms>,"status":"buffered"}` | Nothing. Event will land in the Lakehouse within `FLUSH_INTERVAL_SECONDS` or when the buffer reaches `BATCH_SIZE`. |
| `400 Bad Request` | Validation failed. Body: `{"error":"..."}`. Causes: missing/empty `X-Environment-Id` or `X-Deployment-Id`, non-integer or negative `{time-in-ms}`, empty `{credit-type}`. | Fix the request. Do not retry as-is. |
| `401 Unauthorized` | `Authorization` header is missing or malformed. | Add `Authorization: Bearer <token>`. |
| `403 Forbidden` | Token is present but does not pass the permission check for this environment. | Use the correct SDK token or PAT for the target environment. |
| `503 Service Unavailable` | Two possible causes: (a) the auth backend (Quix portal) is temporarily unreachable, or (b) the internal ingest buffer is full. Body distinguishes: `"auth backend unavailable"` vs `"ingest buffer full"`. | Retry with backoff. Do not treat as a permanent error. |

> **Note on auth ordering.** Auth is checked before validation. A request with a bad token returns 401/403 even if the path parameters would also have caused a 400.

---

## Where does my data go?

Events land in the Lakehouse table **`billing_events`**, partitioned by:

```
environment_id / deployment_id / event_month
```

`event_month` is `YYYY-MM` derived from `received_at` at ingest time.

After you POST, the event sits in a durable in-memory buffer (mirrored to QuixStreams State) until one of two flush triggers fires:

- The buffer reaches **`BATCH_SIZE`** events (default: 500), or
- **`FLUSH_INTERVAL_SECONDS`** have elapsed since the last flush (default: 30 s).

Under normal load, expect your event in the Lakehouse within **30 seconds**. Writes go through the Lakehouse Query API (`/insert`) — no direct blob access.

The table schema has 14 columns. The most useful for querying are: `event_id`, `credit_type`, `duration_ms`, `environment_id`, `deployment_id`, `operation`, `payload`, `event_datetime`, and `event_month`.

---

## Health check

```bash
GET http://billing-sink/healthz
```

No auth required. Returns:

```json
{
  "status": "ok",
  "buffer_size": 12,
  "pending_state_count": 12,
  "last_flush_ts": 1720000028000,
  "batches_sunk": 4,
  "dropped_replays": 0
}
```

`dropped_replays` counts events the sink discarded as duplicates on restart. A non-zero value after a clean run is normal when the service has restarted mid-batch.

---

## Configuration reference (billing-sink env vars)

| Name | Default | Description |
|---|---|---|
| `HTTP_PORT` | `80` | Port uvicorn listens on. |
| `BATCH_SIZE` | `500` | Flush after this many buffered events. |
| `FLUSH_INTERVAL_SECONDS` | `30` | Flush after this many seconds even if `BATCH_SIZE` is not reached. |
| `BILLING_TOPIC` | `billing-events` | Name of the internal self-loop Kafka topic (durability hop between the HTTP handler and the SDF). Never produce to it from outside — the only ingress is `POST /billing/...`. |
| `LAKE_TABLE` | `billing_events` | Lakehouse target table name. |
| `SCHEMA_VERSION` | `1` | Stamped on every row; increment when the schema changes. |
| `DEDUP_TTL_SECONDS` | `600` | How long the sink remembers a sunk `event_id` to guard against replay double-writes. |
| `STATE_KEY` | *(deployment ID, then `"billing-sink"`)* | Internal RocksDB key. Leave unset in production. |
| `CONSUMER_GROUP` | `billing-sink-v1` | Kafka consumer group. |
| `LOGGER` | `info` | Log level: `off`, `info`, or `debug`. |
| `AUTH_ENABLED` | `true` | Set `false` only for broker-less local dev (skips portal check). |
| `AUTH_CACHE_SECONDS` | `300` | How long a validated token result is cached before re-checking with the portal. |
| `AUTH_REQUIRED_PERMISSION` | `Write` | Portal permission level the caller's token must have. |
| `FLUSH_RETRY_BASE_MS` | `1000` | Base backoff before retrying a failed Lakehouse write. |
| `FLUSH_RETRY_CAP_MS` | `60000` | Maximum backoff between flush retries. |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Every POST returns `401` | `Authorization` header is absent or the token string is empty. | Confirm `Quix__Sdk__Token` is injected (it always is inside a Quix deployment). Check your `BILLING_TOKEN` override if you set one. |
| Every POST returns `403` | The token does not have `Write` permission on this environment, or it belongs to a different environment. | Use the SDK token for the same environment the sink is deployed in, or use a PAT with `Write` access. |
| POST returns `503 "auth backend unavailable"` | The Quix portal API is temporarily unreachable. | Retry with backoff. The sink will not accept unvalidated requests. |
| POST returns `503 "ingest buffer full"` | The internal Kafka produce queue is saturated (unusual at billing volumes). | Retry with backoff. Check if the pipeline is healthy via `/healthz`. |
| Events are not appearing in the Lakehouse yet | The flush has not fired yet, or the Lakehouse write is retrying after a transient error. | Wait up to `FLUSH_INTERVAL_SECONDS` (default 30 s). Check `/healthz`: a growing `buffer_size` with `batches_sunk` not increasing indicates a stuck flush — check service logs for write errors. |
| Billing sink is unreachable / down | The sink deployment is stopped or restarting. | Callers using the fire-and-forget pattern (like `recovery-filter`) will log dropped events and keep running — no caller should crash because billing is down. Events sent during the outage are lost; there is no retry queue on the caller side. |
