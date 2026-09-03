# Lake Sink [#1143]

Sinks the TTL dedup pipeline's output (`deduped-events-no-ttl`) into the Quix
lakehouse through the built-in `QuixTSDataLakeSink`, configured so that the three
features added by quix-streams **PR #1143** (`702a2d48`, first on `main` in
`be5e740c`, version `3.25.0`) are exercised and observable.

Nothing here is hand-rolled: the sink owns the parquet writing, the blob path
building, the `.vidx` sidecars and the catalog calls. This service only wires it
up and stamps one extra column.

## Provenance

`main.py`, `requirements.txt` and `build/dockerfile` are the upstream Quix
connector `quix-samples/python/destinations/lakehouse-sink` (branch `main`),
copied verbatim and then modified by a deliberately small, enumerable delta.
`app.yaml` carries that sample's `library.json` variables across into the
repo-app form. Diff against the sample before reviewing anything else — the
delta is:

1. `requirements.txt` pins `quixstreams[quixdatalake]` to git SHA `be5e740c`
   (3.25.0, contains #1143) instead of `>=3.24.0`; the dockerfile gains an
   `apt-get install git` because pip now resolves a git ref.
2. The three #1143 constructor arguments: `hive_columns` with `~order_id`,
   `sort_column`, `stats_columns`.
3. `seq` is stamped on every record before `.sink()`.
4. Env reads go through the repo's empty-string-tolerant `_envstr`/`_envint`/
   `_envflag` helpers (the Portal stores blanks as `""`, and the sample's bare
   `int()` / `.lower() == "true"` misread that).
5. `TABLE_NAME` + `TABLE_VERSION` compose the effective table name.
6. `AUTO_OFFSET_RESET` defaults to `earliest` (the sample's `main.py` said
   `latest` while its own `library.json` said `earliest`).
7. `TIMESTAMP_COLUMN` defaults to `timestamp`, this pipeline's epoch-ms field,
   instead of the sample's `ts_ms`.

## What each parameter tests

| Parameter | Value | #1143 feature it exercises |
|---|---|---|
| `hive_columns` | `year,month,day,hour,status,~order_id` | **Virtual partitions.** `~order_id` gets no `key=value/` folder, does not split files, and stays in the parquet. `order_id` spans 40M values, so a physical partition would emit one tiny file per order per batch. `status` is a low-cardinality physical partition for contrast; `year/month/day/hour` are derived from `timestamp_column` and are physical-only. |
| `sort_column` | `seq` | **Sort column.** Recorded as `properties.sort_column`; compaction orders files by it. Deliberately not `timestamp` — an unset `sort_column` falls back to `timestamp_column`, so setting the two equal would be indistinguishable from leaving it unset. |
| `stats_columns` | unset (`None`) | **Per-file statistics.** Auto-detects every numeric/timestamp column of each written file, which here is `timestamp` **and** `seq` — two columns, proving multi-column zone maps. Set `STATS_COLUMNS=timestamp,seq` to pin the set explicitly, or `STATS_COLUMNS=timestamp` to prove restriction works. |

### The `seq` column

`main.py` stamps a process-local `itertools.count()` value onto every record
(`sdf.update(stamp_seq)`) before the sink. It exists purely to give
`sort_column` a target that is not `timestamp_column`, and to give the
statistics a second numeric column.

**It resets to 0 on every restart, redeploy and rebalance.** `seq` is monotonic
only within one replica's lifetime and its values repeat across restarts. It is
a metadata probe, not a global sequence — do not build anything on it.

## Changing the layout is a migration

The sink validates the configured partition set against the catalog's
`partition_spec` **and** against the Hive paths already on disk during
`setup()`, and raises on a mismatch. Table properties (`sort_column`,
`virtual_partitions`, `timestamp_column`) are written **only** by the
table-creation `PUT` — an already-registered table is validated, never updated.

So any change to `HIVE_COLUMNS`, `TIMESTAMP_COLUMN` or `SORT_COLUMN` needs a
**fresh table**: bump `TABLE_VERSION` (`v1` -> `v2`), which changes the
effective table name from `ttl_events_v1` to `ttl_events_v2`. Bump
`CONSUMER_GROUP` alongside it to re-sink the backlog into the new table.

## Deployment requirement

`blobStorage: { bind: true }` at the deployment level in `quix.yaml` is
**mandatory**. On dev that bind is the injection vehicle for the whole
`Quix__Lakehouse__*` bundle — remove it and `Quix__Lakehouse__Catalog__Url` /
`__AuthToken` disappear, the sink boots with no catalog, and none of the
catalog-side evidence below exists. `main.py` logs
`Catalog URL: NOT RESOLVED (check blobStorage.bind)` in that case.

## How to read the results back

### 1. Storage layout (blob)

```
{workspaceId}/data-lake/time-series/ttl_events_v1/
  year=2026/month=09/day=03/hour=14/status=SHIPPED/data_<uuid>.parquet
  year=2026/month=09/day=03/hour=14/status=SHIPPED/.vidx/data_<uuid>.parquet
```

What to check:

- There is **no `order_id=` folder anywhere** — that is the virtual partition
  working. A physical `order_id` would have produced one folder (and one file)
  per order per batch.
- `order_id` and `seq` **are** columns inside the data parquet;
  `year`/`month`/`day`/`hour`/`status` are **not** (their values live in the
  path).
- Each data file has a `.vidx/` sibling with the same basename, holding one row
  per distinct virtual tuple plus the file's physical partition values as
  constant columns. Glob `.vidx/*.parquet` — never assume one layout, because a
  reindex or compaction collapses a folder's sidecars into a single
  `.vidx/index.parquet`.
- A sidecar failure is logged, never raised, and self-heals on the next write,
  so a missing sidecar is not a data problem.

Read the sidecars directly (they carry the physical values in the content, so no
`hive_partitioning` is needed):

```sql
SELECT DISTINCT order_id
FROM read_parquet('s3://<bucket>/<ws>/data-lake/time-series/ttl_events_v1/*/*/*/*/*/.vidx/*.parquet')
WHERE year = '2026' AND month = '09';
```

### 2. Catalog manifest — one call, ~130 ms, size-independent

This is where `partition_values`, the `partition_spec`,
`properties.virtual_partitions`, `properties.sort_column` and the per-file
`column_stats` live. Use the **catalog** URL, not the Query URL:

```
GET  {Quix__Lakehouse__Catalog__Url}/namespaces/default/tables/ttl_events_v1/manifest
Authorization: Bearer {Quix__Lakehouse__Catalog__AuthToken}

-> { "entries": [ { "partition_values": {...}, "column_stats": {...}, ... }, ... ] }
```

Table-level metadata (spec + properties) comes from the table endpoint:

```
GET  {Quix__Lakehouse__Catalog__Url}/namespaces/default/tables/ttl_events_v1
```

Expect:

- `partition_spec` = `["year","month","day","hour","status","order_id"]` — the
  full tree order, `~` stripped. A physical-only table would instead register an
  empty spec and let the catalog derive it from the file paths; the presence of a
  virtual column is what makes the sink send the spec up front.
- `properties.virtual_partitions` = `["order_id"]`.
- `properties.sort_column` = `"seq"`, and `properties.timestamp_column` =
  `"timestamp"` beside it.
- Per manifest entry, `partition_values` contains only the **physical**
  columns, and `column_stats` contains `timestamp` and `seq`, each with
  `type` (`numeric`), `min`, `max`, `null_count`, `value_count`. `payload` and
  `order_id` are strings and `__key` is internal, so all three are skipped.
  Numeric bounds are widened outward to the nearest float, so a `min`/`max` may
  sit slightly outside the true range — that is deliberate (the stored range is
  always a superset, so rounding can cost pruning but never wrongly skip a file).

### 3. SQL (Lakehouse Query API)

A **different** endpoint from the catalog: `Quix__Lakehouse__Query__Url` is the
public `https://lh-query-….deployments-dev.quix.io`, while
`Quix__Lakehouse__Catalog__Url` (and its legacy `CATALOG_URL` / `QUIX_LAKE_URL`
aliases) is the in-cluster `http://lh-cat-…:5001`. Do not swap them.

```bash
curl -s -X POST "$Quix__Lakehouse__Query__Url/query" \
  -H "Authorization: Bearer $Quix__Lakehouse__Query__AuthToken" \
  -H "Content-Type: text/plain" \
  --data 'SELECT order_id, seq, timestamp FROM ttl_events_v1 WHERE status = '"'"'SHIPPED'"'"' LIMIT 20'
```

The reply is CSV. Query gotchas on this DuckDB-backed API:

- **No CTEs / `WITH`** — they silently return 0 rows.
- **Aggregations are slow** — `MIN`/`GROUP BY`/`FILTER` over derived tables hit a
  ~30 s timeout. Prefer a raw scan with a `LIMIT` and aggregate in Python.
- **Partition-equality filters push down** to an S3 prefix, so
  `WHERE status = 'SHIPPED'` (physical) is cheap and skips non-matching files.
  `WHERE order_id = 'order-0123456'` (virtual) still resolves, because the
  column is in the parquet data — it just does not prune by folder. That
  contrast is the point of the experiment.

## Configuration

Every variable is declared in `app.yaml` and in the `quix.yaml` deployment
block, with the defaults `main.py` uses. Empty values are treated as "unset"
everywhere — the Portal stores a blank as `""`, not as a missing key, so
`STATS_COLUMNS=""` means "auto-detect" and `SORT_COLUMN=""` means "fall back to
`timestamp`", both switchable without a rebuild.

`BATCH_SIZE` maps to `commit_every`, which counts **input messages**, not output
rows. This topology is 1:1 (no `expand=True` before the sink), so with the
defaults a checkpoint holds ~1000 rows and flushes at most every 30 s.
