# billing-sink + recovery-filter billing integration — architecture

Audience: an engineer who will modify this in 6 months. Companion to
`spec.md` (the contract) and `progress.md` (build log). Implements spec
Design B (explicit State mirror) plus Amendment A1 (token auth).

## What it does

`billing-sink` is a QuixStreams service that accepts credit-spend events over
HTTP (`POST /billing/{credit-type}/{time-in-ms}`), buffers them durably, and
sinks them to the Quix Lakehouse `billing_events` table in configurable batches.
Each event flows **POST → Kafka (billing-events topic) → QuixStreams State
mirror → Lakehouse**, and is deleted from State only after a *confirmed*
Lakehouse write. `recovery-filter` is adapted to emit two kinds of billing
events (`messages-processed-<N>` throughput ticks and one `backfill-action` on
shutdown) as fire-and-forget POSTs that never block or crash filtering. The
stored value is raw `duration_ms` — there is no credit/money conversion
anywhere.

## Why this shape (key decisions + trade-offs)

- **uvicorn on a worker thread, SDF on main.** QuixStreams has no HTTP-ingest
  Source and `app.run()` installs the SIGINT/SIGTERM handlers, so it must own the
  main thread. The HTTP handler therefore runs off-thread and hands events to the
  pipeline by *producing onto the billing-events topic* via a shared
  `app.get_producer()`. This is the one sanctioned non-native piece (the proven
  `quix-rocksdb-state-api` pattern).
- **Kafka is the ingest-durability layer; State is the ticket-mandated mirror.**
  State (RocksDB + changelog) is written in-context by the single stateful SDF and
  makes the pending buffer survive restarts. Trade-off: with Kafka already
  durable, the State mirror is partly redundant (spec §8 / Design A). We build it
  because the ticket requires confirmed-delete-from-State.
- **Blocking Lakehouse write inside the flush op.** Only by writing inline and
  *then* calling `state.delete()` in the same in-context step can we honour
  "delete after a **confirmed** sink". Trade-off: a slow write stalls consumption
  of billing-events; bounded by `BATCH_SIZE` and the retry backoff. Fine at
  billing volume.
- **Single constant `STATE_KEY`.** One key → one stream_id → one partition → one
  State store the flush op accumulates in. Trade-off: no ingest parallelism —
  intended and fine for billing volume; do **not** `group_by`.
- **Per-`event_id` State keys + `_pending_index`** (not one big list value) for
  cheap idempotency lookups and batch assembly without rewriting the whole buffer
  on every event.

## Module map

`billing-sink/` (flat layout; runs with cwd on `sys.path`, entrypoint `main.py`):

| File | Responsibility |
|---|---|
| `config.py` | `load_config() -> BillingConfig` (frozen). Every env var + default + local-dev fallback. `PARTITION_COLUMNS`. STATE_KEY resolution. |
| `records.py` | Pure record build + validation: `build_event_record`, `enrich_for_sink`, `parse_duration_ms`, header/credit-type validators, `SINK_COLUMNS` (the 14-col schema), `now_ms`. |
| `auth.py` | `Authorizer` wrapping `quixportal.auth.Auth` behind an `AuthDecision` enum (ALLOW/UNAUTHENTICATED/FORBIDDEN/UNAVAILABLE). Lazy Auth construction; any error → UNAVAILABLE. |
| `state_buffer.py` | State-key layout + helpers (`add_pending`, `is_replay`, `confirm_sunk`, `read_pending_records`, `pending_count`) and the `PendingBuffer` (RAM cache + health snapshot). |
| `lake_writer.py` | `LakehouseWriter` Protocol + `QuixLakeWriter` (drives `QuixLakeClient.insert` synchronously) + `build_lakehouse_writer` factory. |
| `http_api.py` | FastAPI app: `POST /billing/...` (auth → validate → publish → 202) and open `GET /healthz`. |
| `pipeline.py` | `FlushController` (stateful op `handle`, ingest, `_maybe_flush`, retry backoff), `make_publisher`, `start_flush_ticker`. |
| `main.py` | Entrypoint: load config, build Application/topic/SDF, producer, authorizer, FastAPI app; start uvicorn + flush-tick threads; `app.run()` on main; flush producer on exit. |
| `app.yaml`, `requirements.txt`, `build/dockerfile` | Packaging (see Config + Deployment below). |

`recovery-filter/` changes:

| File | Change |
|---|---|
| `billing_client.py` (new) | `BillingClient` (bounded `queue.Queue` + one daemon worker POSTing to billing-sink). `emit` (non-blocking, drop-on-full), `emit_now` (blocking, shutdown only). `build_billing_client()` reads `BILLING_*`. |
| `main.py` | `_maybe_emit_messages_event()` called at top of `dedup_filter` (counts every message; fires `messages-processed-<N>` every `BILLING_MESSAGES_PER_EVENT`). `main()` builds the client and, after `app.run()` returns, emits one `backfill-action`. |
| `app.yaml`, `requirements.txt` | `BILLING_*` vars; `requests`. |

`quix.yaml`: new `Billing Sink` deployment (state, `network.serviceName:
billing-sink` + port 80, all vars); `BILLING_*` added
to `Recovery Filter`; `billing-events` added to `topics`.

## Data flow

```
QuixLab / recovery-filter
      │  POST /billing/{credit-type}/{ms}  (Authorization: Bearer <token>)
      ▼
[uvicorn worker thread]  authorize → validate → build record (event_id, received_at,
      │                  event_datetime, event_month, schema_version)
      │  producer.produce(billing-events, key=STATE_KEY, {"type":"event","record":..})
      ▼  202 {event_id, received_at, "buffered"}
billing-events topic ──consumed by──► [main thread: app.run(), one stateful SDF]
      │                                   FlushController.handle(value, state):
      │  {"type":"flush_tick"} every        - rebuild RAM from State once (boot)
      │  FLUSH_INTERVAL_SECONDS  ─────────►  - event: dedup guard → state.set(record:{id}),
      │  (daemon timer thread)                 append _pending_index, RAM.add
      │                                       - maybe_flush: size≥BATCH_SIZE or
      ▼                                         (now-_last_flush_ts)≥interval
   flush(): snapshot ≤BATCH_SIZE → enrich (batch_id, sink_ts, sink_deployment_id)
      → writer.write_batch(rows)  [BLOCKING; raises on failure → keep + backoff]
      → on success: state.delete(record:{id}); state.set(_sunk:{id}, ttl=DEDUP_TTL);
        trim _pending_index; RAM.remove; _last_flush_ts=now
      ▼
Lakehouse billing_events  (lake service /insert; hive: environment_id/deployment_id/event_month)
```

## LakehouseWriter backend shipped

**`QuixLakeWriter`** wrapping `quixlake-sdk`'s `QuixLakeClient.insert` (spec
Amendment A2). All writes go through the Lakehouse **Query API `/insert`**
endpoint; the lake service persists parquet and maintains the Iceberg catalog
server-side. Our code never touches blob storage directly — direct blob writes
bypass the service and can corrupt the catalog (the user's rule that drove A2).

- `write_batch(rows)` builds a pandas DataFrame with the fixed 14-column schema
  (`records.SINK_COLUMNS`) and calls `client.insert(table_name=LAKE_TABLE,
  data=frame, hive_columns=["environment_id","deployment_id","event_month"])`,
  synchronous (`async_mode=False`). The client is built lazily from
  `Quix__Lakehouse__Query__Url` + `__AuthToken` (both auto-inject on dev;
  billing-sink fails fast at startup if either is missing).
- **Failure handling:** `insert` raises on any non-200 (`raise_for_status`), which
  propagates so the flush op keeps the batch pending and the existing
  retry/backoff runs. A **409 partition mismatch** (raised as `ValueError`) is
  structurally non-retryable: the writer logs it loudly and re-raises; the batch
  stays pending and the backoff cap prevents a tight loop (an operator must fix
  the partition structure — there is no direct-blob fallback).
- **Swapping backends:** implement the `LakehouseWriter` Protocol
  (`write_batch(rows) -> None`, raise on failure) and return it from
  `build_lakehouse_writer`. Nothing in the flush path changes.

## Restart recovery & idempotency

- On restart, QuixStreams replays the State changelog → `record:*` and
  `_pending_index` are restored. On the first message after boot the RAM buffer is
  rebuilt from State (`read_pending_records`); the flush-tick timer guarantees a
  message arrives within `FLUSH_INTERVAL_SECONDS`, so recovered records flush even
  with zero live traffic. `_last_flush_ts` is seeded at boot only if State has none,
  so a fresh service waits a full interval before the first time-flush while a
  restart with an old `_last_flush_ts` flushes recovered records promptly.
- **No double-billing:** `event_id` is generated once at ingest and travels with
  the message. Before adding a record the op checks `_sunk:{id}` and `record:{id}`;
  either present ⇒ replay ⇒ dropped (`dropped_replays++`). `DEDUP_TTL_SECONDS`
  (default 600) must exceed the consumer commit window so a replay before the
  offset commits is still recognised. Residual risk (accepted, spec §8.4): a replay
  *after* the TTL expires would double-write (Iceberg is append-only).
- **Flush failure:** `write_batch` raising leaves records in State + RAM; the
  controller sets an exponential backoff (`FLUSH_RETRY_BASE_MS` × 2^n, capped at
  `FLUSH_RETRY_CAP_MS`) and retries on the next trigger. A 409 partition mismatch
  is logged loudly and re-raised (kept pending; the cap prevents a tight loop).

## Config surface

Spec §7.3 / §7.4 / A1 vars are implemented verbatim with the spec's names and
defaults. **Additions beyond §7.3** (declared in `app.yaml`/`quix.yaml`, flagged
here for transparency):

- `FLUSH_RETRY_BASE_MS` (1000) / `FLUSH_RETRY_CAP_MS` (60000) — parameterize the
  spec §5.4-mandated bounded backoff (kept out of magic numbers).

Lakehouse writes need `Quix__Lakehouse__Query__Url` + `__AuthToken` (auto-inject on
dev; local fallbacks `QUIXLAKE_URL` / `QUIX_LAKE_TOKEN`); startup fails fast if
either is missing.

Note: the topic variable is named `BILLING_TOPIC` (default `billing-events`).

## Integration with recovery-filter

recovery-filter POSTs to `http://billing-sink` (in-cluster DNS via
`network.serviceName: billing-sink`, port 80). Every POST carries
`X-Environment-Id`/`X-Deployment-Id` and `Authorization: Bearer <BILLING_TOKEN or
Quix__Sdk__Token>` (the SDK token is auto-injected, so the default needs no
config and passes billing-sink's `Workspace/Write` check). The client is
fire-and-forget: a full queue drops (counter), a POST error logs + drops, and it
never raises into the SDF. `messages-processed-<N>` carries `duration_ms` = wall-clock
ms between fires; `backfill-action` carries `duration_ms` = service start →
shutdown (Phase-1 default).

## How to run locally (no cluster)

Uses the repo-global Python (has quixstreams + quixlake-sdk + deps) or a
venv with `billing-sink/requirements.txt`.

- **Byte-compile:** `python -m py_compile billing-sink/*.py`.
- **Logic + HTTP smoke (no broker/lake/portal):** `python .tmp/smoke_billing.py`
  (fake State, fake writer, stub Auth; 28 checks).
- **Full local run against a broker:** set `BROKER_ADDRESS`/Quix SDK env, set
  `AUTH_ENABLED=false` (skip portal), a real `HTTP_PORT`, and `QUIXLAKE_URL` /
  `QUIX_LAKE_TOKEN` (the Query API) for actual Lakehouse writes; then
  `python billing-sink/main.py`. `curl -XPOST localhost:$PORT/billing/test/12 -H
  'X-Environment-Id: e' -H 'X-Deployment-Id: d'` → 202; `GET /healthz` → buffer
  stats.

## Manual cloud QA (owner: user)

Deploy on `quixdev-ludviktestenvironment-billingservice` (dev). Checks: curl
without header → 401; garbage token → 403; env SDK token and a PAT → 202; row
appears in Lakehouse `billing_events`; restart mid-buffer → recovered rows flush,
no double-write; kill billing-sink → recovery-filter keeps filtering and logs
dropped billing POSTs.
```
