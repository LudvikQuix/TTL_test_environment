# Billing Sink + Recovery-Filter Billing Integration

**Status:** Draft
**Project:** Billing_service
**Created:** 2026-07-08
**Planned with:** Buddy

## 1. Summary
Add a new `billing-sink` service exposing `POST /billing/{credit-type}/{time-in-ms}`.
It accepts credit-spend events (e.g. a QuixLab cell execution, a recovery-filter
backfill run), buffers them in RAM with a durable QuixStreams **State** mirror,
and sinks them to the Quix Lakehouse in configurable-size batches. After a
*confirmed* Lakehouse write the batch is deleted from State and RAM. Separately,
adapt the existing `recovery-filter` to POST two kinds of billing events to the
sink (a per-backfill-run event and a per-N-keys throughput event) as
fire-and-forget calls that never block or crash the filter.

The measured value is **raw duration in milliseconds** (`duration_ms`). There is
**no credit/rate conversion anywhere** — the sink stores time, not money.

## 2. Goals
- `POST /billing/{credit-type}/{time-in-ms}` endpoint with header-based
  `environment_id` / `deployment_id`, optional JSON body.
- Durable path **POST → RAM → State → Lakehouse** with batched sink writes.
- State mirror so an in-flight (not-yet-sunk) event survives a restart; on
  restart, unsunk records are re-sent; `event_id` gives idempotency.
- Batch size is an env-var parameter (`BATCH_SIZE`); a `FLUSH_INTERVAL_SECONDS`
  timeout also flushes small trickles (addition beyond the ticket — see §8).
- Lakehouse rows carry the confirmed schema (§7) partitioned by
  `environment_id → deployment_id → event_month`.
- recovery-filter emits `backfill-action` and `messages-processed-<N>` billing
  events without ever blocking or crashing its main processing.
- All thresholds are env-var parameters; no hard-coded magic numbers.

## 3. Non-goals
- No credits/currency conversion, pricing, or aggregation/reporting.
- No public-internet ingress for external callers in Phase 1 (in-cluster only;
  see §8 for the QuixLab public-ingress follow-up).
- No changes to `recovery-generator` or any file outside `billing-sink/`,
  `recovery-filter/`, root `quix.yaml`, and `dev-planning/`.
- No tuning/optimization phase, no load testing, no auth on the endpoint.
- No git commits or pushes.

## 4. User stories / scenarios
1. **QuixLab-style caller:** `POST /billing/quixlab-cell-exec/12345` with the two
   headers and a JSON body → `202 {event_id, received_at, status:"buffered"}`.
   The row later appears in the Lakehouse `billing_events` table.
2. **recovery-filter throughput:** every `N` messages processed, the filter POSTs
   `messages-processed-<N>` with `duration_ms` = wall-clock ms elapsed since the
   previous fire.
3. **recovery-filter backfill:** the filter POSTs one `backfill-action` event
   with `duration_ms` = wall-clock ms of the backfill run.
4. **Batch flush:** the sink accumulates events until `BATCH_SIZE` is reached (or
   `FLUSH_INTERVAL_SECONDS` elapses), writes them to the Lakehouse in one batch
   tagged with a shared `batch_id`, then forgets them from State and RAM.
5. **Crash recovery:** the sink is killed after events are buffered but before a
   flush. On restart the State changelog rebuilds the pending buffer and the
   events are flushed; already-sunk events are not double-written.
6. **Billing endpoint down:** billing-sink is unreachable; recovery-filter logs a
   dropped billing POST and keeps processing its main stream unaffected.

## 5. Proposed design

### 5.1 Chosen shape (Design B — explicit State mirror, per ticket)
The ticket is explicit: **POST → RAM → State → Lakehouse, with a State mirror so
data is not lost, deleted after a confirmed sink.** We implement that literally,
using QuixStreams-native primitives for everything except the HTTP ingress
(named limitation below).

Data flow:
```
HTTP POST (uvicorn, worker thread)
   → validate + build record (inject event_id, received_at, ...)
   → app.get_producer().produce(billing-events, key=STATE_KEY, value=record)   # Kafka durability
   → 202 buffered

billing-events topic  ──consumed by──▶  ONE stateful SDF (main thread, app.run())
   on each billing record  : state.set(event_id, record); append to RAM buffer; maybe flush
   on each synthetic tick  : maybe flush (time-based)
   flush(): assemble batch → BLOCKING Lakehouse write → on success:
            state.delete(event_id) for each; mark event_id in _sunk (TTL); clear RAM
```

- **Ingress is not a QS Source.** QuixStreams has no HTTP-ingest source, and
  `app.run()` must own the main thread (it installs SIGINT/SIGTERM handlers).
  So uvicorn runs on a **worker thread** and the handler uses
  `app.get_producer()` to publish onto the `billing-events` topic. This is the
  one named non-native piece, exactly the pattern proven in
  `quix-rocksdb-state-api` (uvicorn on a worker thread, SDF on main).
- **Kafka is the ingest-durability layer; State is the ticket's explicit
  mirror.** State is written *in-context* by the single stateful SDF (State is
  in-context-only — unreachable from the HTTP thread), and its changelog topic
  makes the buffer survive restarts.
- **The Lakehouse write is a synchronous, blocking call inside the in-context
  flush op.** This is deliberate: only by writing inline and *then* calling
  `state.delete()` in the same context can we honor "delete after a **confirmed**
  sink." A framework `sdf.sink()` would decouple the confirmation from the State
  mutation (see §9 Design A for why we did not use it, and §8 for the open
  question about which write client to call).

### 5.2 State keying
- Every message on `billing-events` (real events **and** synthetic flush ticks)
  is produced with the **same constant message key** `STATE_KEY` (default the
  service's own `Quix__Deployment__Id`, falling back to the literal
  `"billing-sink"`). One key → one `stream_id`/partition → **one State store**
  the flush op accumulates in. (Per `quixstreams-idioms` §3 and
  `quix-rocksdb-state-api`: multiple keys/`group_by` would create separate
  stores and break accumulation.)
- State layout inside that single store:
  - `record:{event_id}` → the full record dict (pending, not yet sunk).
  - `_pending_index` → ordered list of pending `event_id`s (so the flush op
    knows what to batch without a full key scan).
  - `_sunk:{event_id}` → `1` with **TTL = `DEDUP_TTL_SECONDS`** (idempotency
    guard, see §5.5).
  - `_last_flush_ts` → epoch ms of the last successful flush.
- The **RAM buffer** is a process-local `list`/`dict` rebuilt from
  `_pending_index` on the first message after boot; it is a *cache for fast batch
  assembly*, never the source of truth. (Per `quix-rocksdb-state-api`: no
  persistent RAM view that "lies about durability" — RAM here is derived from
  State and is safe to lose.)

### 5.3 Flush triggers
Flush fires when **either**:
- `len(RAM buffer) >= BATCH_SIZE`, evaluated after each real event, **or**
- `now - _last_flush_ts >= FLUSH_INTERVAL_SECONDS` **and** the buffer is
  non-empty, evaluated on every message including synthetic ticks.

Because a time-based flush needs an in-context message to act on, a small daemon
timer thread produces a synthetic `{"type":"flush_tick"}` message to
`billing-events` (same `STATE_KEY`) every `FLUSH_INTERVAL_SECONDS`. This is the
same synthetic-event round-trip idiom as `quix-rocksdb-state-api`'s
`get_request`. Real billing events carry `{"type":"event", ...}`.

### 5.4 Flush + confirmed delete
Inside the stateful op, when a flush is due:
1. Snapshot up to `BATCH_SIZE` pending records from the RAM buffer.
2. Assign a shared `batch_id` (uuid) and `sink_ts` (epoch ms); set
   `sink_deployment_id` = `Quix__Deployment__Id`.
3. **Blocking write** of the batch to the Lakehouse (see §5.6). On
   `SinkBackpressure`/transient error: do **not** delete; leave records in State
   and retry on the next trigger (bounded exponential backoff, capped).
4. On confirmed success: for each event in the batch
   `state.delete("record:{event_id}")`, add `state.set("_sunk:{event_id}", 1,
   ttl=DEDUP_TTL_SECONDS)`, remove from `_pending_index` and the RAM buffer, set
   `_last_flush_ts = now`.

### 5.5 Restart recovery & idempotency
- On restart, QuixStreams replays the State changelog → `record:*` and
  `_pending_index` are restored. On the first processed message the RAM buffer is
  rebuilt from State; the next trigger flushes the recovered records.
- **Idempotency / no double-billing:** `event_id` is generated once at ingest and
  travels with the message. Before adding a record, the op checks
  `state.exists("_sunk:{event_id}")` and `state.exists("record:{event_id}")`; if
  either is set, the message is a replay and is dropped (counter incremented).
  This closes the window where a message was already sunk but its
  `billing-events` consumer offset had not yet committed (QS commits on
  `commit_interval`); `DEDUP_TTL_SECONDS` must exceed that window (default
  comfortably larger — see §7).
- Downstream contract: `event_id` is unique per logical event, so any Lakehouse
  consumer can also dedup on it. (Iceberg is append-only; the sink cannot upsert,
  so the `_sunk` guard is the primary defense — see §8.)

### 5.6 Lakehouse write mechanism (dependencies & env)
- The target env `quixdev-ludviktestenvironment-billingservice` is a **dev**
  cluster (`*.dev.quix.io`), so the Lakehouse **Catalog/Query** vars
  (`Quix__Lakehouse__Catalog__Url`, `Quix__Lakehouse__Catalog__AuthToken`,
  `Quix__Lakehouse__Query__Url`, `Quix__Lakehouse__Query__AuthToken`)
  **auto-inject** — no need to declare them (per `quix-lakehouse` §2).
- Writing parquet/Iceberg needs **blob credentials**, which do **not**
  auto-inject: add `blobStorage: { bind: true }` at the deployment level so Quix
  injects `Quix__BlobStorage__Connection__Json` (per `quix-lakehouse` §1).
- The **write client** for the in-context blocking write is the open question in
  §8. Recommended primary: the Quix data-lake sink primitive
  (`QuixTSDataLakeSink` / `quixstreams.sinks.community.iceberg.IcebergSink`,
  which subclass `BatchingSink` with a `write(batch: SinkBatch)` method) invoked
  directly on the assembled batch; fallback: `quixlake-sdk` `QuixLakeClient` row
  insert; last resort: direct parquet-to-blob + Iceberg catalog registration.
  ArchDev must confirm which is available in the target image before coding the
  flush.
- Read env with local-dev fallbacks so the same code runs off-cluster:
  `Quix__Lakehouse__Catalog__Url` → `CATALOG_URL`, etc.

### 5.7 recovery-filter adaptation
- New module `recovery-filter/billing_client.py`: a fire-and-forget client — a
  bounded `queue.Queue` + one daemon worker thread that POSTs to
  `BILLING_URL`. On queue-full: drop + increment a dropped counter. On POST
  error/timeout: log at info + drop (no unbounded retries). **Never** raises into
  the SDF thread. Gated by `BILLING_ENABLED` (default `true`).
- Headers on every POST: `X-Environment-Id: BILLING_ENVIRONMENT_ID` (default
  from injected `Quix__Workspace__Id`), `X-Deployment-Id: BILLING_DEPLOYMENT_ID`
  (default from injected `Quix__Deployment__Id`), `Content-Type: application/json`.
- **Event (b) `messages-processed-<N>`:** maintain a counter incremented per
  processed message inside `dedup_filter`. Every `BILLING_MESSAGES_PER_EVENT`-th
  message, enqueue a POST `credit_type = f"messages-processed-{BILLING_MESSAGES_PER_EVENT}"`,
  `time-in-ms` = wall-clock ms since the previous fire, body
  `{"operation":"dedup-filter","messages":N,"pass":..,"block":..,"skip":..}`.
- **Event (a) `backfill-action`:** time the backfill run and enqueue one POST
  `credit_type = "backfill-action"`, `time-in-ms` = wall-clock ms of the run,
  body `{"operation":"backfill","messages":..}`. The exact boundary of "the
  backfill run" in recovery-filter is an open question (§8); Phase-1 default:
  emit on graceful shutdown (SIGTERM handler) with `duration_ms = now - start`.

## 6. Sub-features / work breakdown

1. **billing-sink HTTP layer** — FastAPI app + uvicorn on a worker thread.
   `POST /billing/{credit_type}/{time_in_ms}`, `GET /healthz`. Validation (§7),
   builds the record, injects `event_id`/`received_at`/`event_datetime`/
   `event_month`, publishes via `app.get_producer()` to `billing-events`, returns
   `202`. Touchpoints: `billing-sink/main.py`, `billing-sink/http_api.py`.
   Owner: **ArchDev**.
2. **billing-sink QS pipeline (State + flush)** — `Application`, `billing-events`
   topic, single stateful SDF keyed by `STATE_KEY`; State mirror, RAM buffer,
   flush triggers, confirmed-delete, idempotency guard, changelog recovery.
   Timer thread producing synthetic flush ticks. Touchpoints:
   `billing-sink/pipeline.py`, `billing-sink/state_buffer.py`. Depends on 1
   (shared record shape) and 3. Owner: **ArchDev**.
3. **billing-sink Lakehouse writer** — blocking batch write module using the
   confirmed write client (§5.6), row schema per §7, partitioning
   `environment_id → deployment_id → event_month`. Touchpoints:
   `billing-sink/lake_writer.py`. Owner: **ArchDev**.
4. **billing-sink packaging** — `billing-sink/app.yaml`,
   `billing-sink/requirements.txt` (quixstreams pinned to the same commit as
   recovery-filter, `python-dotenv`, `fastapi`, `uvicorn`, plus the lake write
   dependency), `billing-sink/build/dockerfile` (copy of recovery-filter's;
   `apt-get install git` already present). Owner: **ArchDev**.
5. **quix.yaml wiring** — new `Billing Sink` deployment block (service, `state:
   enabled: true`, `network` with `serviceName: billing-sink` + the HTTP port,
   `blobStorage: bind: true`, `resources`, all `variables` from §7);
   add `billing-events` to `topics:`. Owner: **ArchDev**.
6. **recovery-filter billing integration** — new `billing_client.py`; wire the
   two event emitters into `main.py`/`dedup_filter`; add new env vars to
   `recovery-filter/app.yaml`, root `quix.yaml` (Recovery Filter block), and
   `recovery-filter/requirements.txt` (`requests`). Fire-and-forget guarantees.
   Owner: **ArchDev**.
7. **Manual QA checklist** — §8.5 curl + Lakehouse + restart checks. Owner:
   **user (manual in Quix Cloud)**; DocuGuy may format.

## 7. Data & interface contracts

### 7.1 Lakehouse row schema (`billing_events` table)
| Column | Type | Source |
|---|---|---|
| `event_id` | string (uuid) | injected (ingest) |
| `credit_type` | string | received — path segment |
| `duration_ms` | long | received — path `{time-in-ms}` (raw ms, no conversion) |
| `environment_id` | string | received — header `X-Environment-Id` |
| `deployment_id` | string | received — header `X-Deployment-Id` |
| `operation` | string (nullable) | received — extracted from `payload` if present |
| `payload` | string (nullable) | received — raw request body stored as string |
| `received_at` | long (epoch ms) | injected (ingest) |
| `event_datetime` | string (ISO-8601 UTC) | injected (ingest) |
| `event_month` | string (`YYYY-MM`) | injected (ingest) |
| `batch_id` | string (uuid) | injected (flush) |
| `sink_ts` | long (epoch ms) | injected (flush) |
| `sink_deployment_id` | string | injected — sink's `Quix__Deployment__Id` |
| `schema_version` | int | injected — constant `SCHEMA_VERSION` |

Partitioning: `environment_id` → `deployment_id` → `event_month`.

### 7.2 Endpoint contract
Request:
```
POST /billing/{credit-type}/{time-in-ms}
Headers:
  X-Environment-Id: <required, non-empty>
  X-Deployment-Id:  <required, non-empty>
  Content-Type: application/json      # optional
Body: optional raw JSON payload
```
Example:
```
POST /billing/quixlab-cell-exec/12345
X-Environment-Id: quixdev-ludviktestenvironment-billingservice
X-Deployment-Id: dep-abc123
Content-Type: application/json

{"operation":"quixlab.cell.execute","notebook":"n1"}
```
Success `202 Accepted`:
```json
{"event_id":"9f1c...","received_at":1720000000000,"status":"buffered"}
```
Validation → `400 Bad Request` (JSON `{"error":"..."}`):
- missing/empty `X-Environment-Id` or `X-Deployment-Id`
- `{time-in-ms}` not a non-negative integer
- empty `{credit-type}`
Body handling: stored raw as `payload`; if it parses as a JSON object, `operation`
is lifted from it. Malformed body is non-fatal (accepted; `operation=null`).
Health: `GET /healthz` → `{status, buffer_size, pending_state_count,
last_flush_ts, batches_sunk, dropped_replays}`.

### 7.3 Config — billing-sink
| Name | Type | Default | Required |
|---|---|---|---|
| `HTTP_PORT` | int | `80` | no |
| `BATCH_SIZE` | int | `500` | yes |
| `FLUSH_INTERVAL_SECONDS` | int | `30` | no |
| `LAKE_TABLE` | FreeText | `billing_events` | no |
| `SCHEMA_VERSION` | int | `1` | no |
| `DEDUP_TTL_SECONDS` | int | `600` | no |
| `STATE_KEY` | FreeText | `""` (→ `Quix__Deployment__Id`, else `billing-sink`) | no |
| `CONSUMER_GROUP` | FreeText | `billing-sink-v1` | no |
| `billing-events` (topic) | Topic | `billing-events` | yes |
| `LOGGER` | off/info/debug | `info` | no |
| `STATE_DIR` | (platform-managed) | `state` | — |
| `blobStorage.bind` | deployment flag | `true` | yes |

### 7.4 Config — recovery-filter (new vars only)
| Name | Type | Default | Required |
|---|---|---|---|
| `BILLING_ENABLED` | bool | `true` | no |
| `BILLING_URL` | FreeText | `http://billing-sink` | no |
| `BILLING_ENVIRONMENT_ID` | FreeText | `""` (→ `Quix__Workspace__Id`) | no |
| `BILLING_DEPLOYMENT_ID` | FreeText | `""` (→ `Quix__Deployment__Id`) | no |
| `BILLING_MESSAGES_PER_EVENT` | int | `1000` | no |
| `BILLING_TIMEOUT_SECONDS` | float | `2.0` | no |
| `BILLING_QUEUE_MAXSIZE` | int | `1000` | no |

## 8. Risks, constraints, and open questions

### Open questions (need sign-off before ArchDev starts)
1. **Lakehouse write client (biggest unknown).** Which of `QuixTSDataLakeSink` /
   `IcebergSink` (invoked manually) vs `quixlake-sdk` `QuixLakeClient` insert vs
   direct blob+Iceberg is actually available/importable in the target image?
   The whole flush path (§5.6) depends on this. If none exposes a clean
   **synchronous** row-batch write, that is a genuine QS limitation that pushes
   us toward Design A (§9) — please confirm.
2. **`backfill-action` boundary.** What exactly is "the backfill run" in
   recovery-filter — process start→SIGTERM (Phase-1 default), or a specific
   milestone in the stream? recovery-filter has no explicit completion signal
   today.
3. **"keys processed" meaning.** Count every processed message (Phase-1 default,
   natural running counter) or only distinct keys? Distinct-key counting needs
   extra State and is heavier.
4. **Idempotency depth.** Is the `_sunk` TTL guard + unique `event_id`
   sufficient, or do downstream consumers also dedup on `event_id`? Iceberg is
   append-only (no upsert), so a replay outside the TTL window would double-write.

### Risks / constraints
- **Redundant durability.** With Kafka (`billing-events`) already durable, the
  explicit State mirror duplicates what Kafka + a `BatchingSink` checkpoint give
  for free (this is the Design A vs B tension — §9). We build the State mirror
  because the ticket asks for it explicitly; flagged for awareness.
- **Blocking write inside the SDF thread** stalls consumption of `billing-events`
  during a slow Lakehouse write. Acceptable at billing volume; bound the batch by
  `BATCH_SIZE` and use `SinkBackpressureError` handling.
- **Single `STATE_KEY`** ⇒ single partition ⇒ no ingest parallelism. Intended
  and fine for billing volume; do not `group_by`.
- **`network.serviceName`** must equal `billing-sink` (a version suffix breaks
  in-cluster DNS — see `quix-rocksdb-state-api` deploy notes).
- **Branch mismatch:** the brief names `billing-branch`, but the working tree is
  currently on `ttl-rockdb-test`. ArchDev should confirm/checkout `billing-branch`
  before starting (both exist locally).
- **Public ingress for QuixLab** (external callers) is out of Phase-1 scope;
  in-cluster only. Add `network.publicAccess` + auth in a follow-up when an
  external caller is wired.

## 9. Alternatives considered

**Design A — QS-native Kafka-buffer + BatchingSink (recommended by the skills,
NOT chosen for Phase 1).** POST → produce to `billing-events` → SDF projects the
row → `sdf.sink(QuixTSDataLakeSink(...))` with `commit_every=BATCH_SIZE`,
`commit_interval=FLUSH_INTERVAL_SECONDS`. Kafka is the durable buffer; offsets
commit only after the sink write, giving at-least-once with no State at all. This
is materially simpler, uses the framework's batching/backpressure, and matches
`quixstreams-idioms` §6 + the "no persistent mirror" guidance in
`quix-rocksdb-state-api`. **Not chosen** because the ticket explicitly requires a
State mirror and confirmed-delete-from-State, which Design A does not literally
provide (durability lives in Kafka, not State). If the §8.1 write-client question
resolves badly, switching to Design A is the recommended fallback.

**Single-State-value buffer.** Store the whole pending buffer as one
`state.set("buffer", [...])` list instead of per-`event_id` keys. Simpler, but
weaker for idempotency dedup and rewrites the whole list on every event. Rejected
in favor of per-`event_id` keys + `_pending_index`.

**External KV (Redis) for the buffer.** Violates QuixStreams-first; State +
changelog already give durable, restart-safe storage. Rejected.

## 10. References
- Skills: `quixstreams-idioms`, `quix-rocksdb-state-api`, `quix-lakehouse`,
  `quix-service-update` (all under `C:\Users\lbazj\.claude\skills\`).
- Reference app: `recovery-filter/` (`main.py`, `app.yaml`, `requirements.txt`,
  `build/dockerfile`).
- QuixStreams Sinks API (IcebergSink / BatchingSink):
  https://quix.io/docs/quix-streams/api-reference/sinks.html
- Original ticket (verbatim) — see §1 / brief.
```

---
### Sanity print

**Column table** → §7.1. **Config tables** (billing-sink + recovery-filter) →
§7.3 / §7.4. **Endpoint contract** (request/response/errors) → §7.2.

---
## Amendment A1 — token-protected endpoint (2026-07-08, architect change request)

Overrides/extends §7.2 and §7.3–7.4 additively. Signed off by user.

**Requirement.** `POST /billing/{credit-type}/{time-in-ms}` requires
`Authorization: Bearer <token>`. Both an environment **SDK token** and a
**PAT** must be accepted. `GET /healthz` stays unauthenticated (liveness).

**Mechanism (library-native, verified).** Use `quixportal.auth.Auth`
(`quixportal==2.0.1`, pip package by Quix Analytics; already the package that
ships the Lakehouse writer components):

```python
from quixportal.auth import Auth
auth = Auth(cache_validity=AUTH_CACHE_SECONDS)  # portal from Quix__Portal__Api
ok = auth.validate_permissions(token, "Workspace", WORKSPACE_ID, AUTH_REQUIRED_PERMISSION)
```

- Calls `GET {Quix__Portal__Api}/auth/permissions/query` with the caller's
  bearer; returns bool. Built-in cache keyed by sha256(token)+resource+perm
  (default 300 s) — no per-request portal round-trip.
- `WORKSPACE_ID` = the sink's own injected `Quix__Workspace__Id`.
- Verified 2026-07-08 against `portal-api.dev.quix.io` with the real env SDK
  token and PAT: both → `True` for `Workspace`/`Read` and `Write` on
  `quixdev-ludviktestenvironment-billingservice`; garbage token → `False`.

**Response semantics (extends §7.2):**
- Missing/malformed `Authorization` header → `401 {"error":"missing bearer token"}`.
- Token fails validation → `403 {"error":"forbidden"}`.
- Portal unreachable (httpx error — `Auth` has no retry) → catch →
  `503 {"error":"auth backend unavailable"}` so callers retry rather than
  discard; never treat an outage as 403, and never accept unvalidated.

**Config additions (extends §7.3, billing-sink):**
| Name | Type | Default | Required |
|---|---|---|---|
| `AUTH_ENABLED` | bool | `true` | no (false only for broker-less local dev) |
| `AUTH_CACHE_SECONDS` | float | `300` | no |
| `AUTH_REQUIRED_PERMISSION` | FreeText | `Write` | no |

**Config addition (extends §7.4, recovery-filter):**
| Name | Type | Default | Required |
|---|---|---|---|
| `BILLING_TOKEN` | Secret/FreeText | `""` (→ falls back to injected `Quix__Sdk__Token`) | no |

recovery-filter sends `Authorization: Bearer <BILLING_TOKEN or Quix__Sdk__Token>`
on every billing POST. The SDK token is auto-injected into every deployment, so
the default requires no configuration.

**QA additions:** curl without header → 401; with garbage token → 403; with the
env SDK token and with a PAT → 202.
