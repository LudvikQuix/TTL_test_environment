# Quix Streams — State TTL (preview)

State entries can now expire automatically. Pass a `ttl=` to `state.set(...)` and the entry disappears after that duration.

## Install

```bash
pip install quixstreams==3.24.1a2 \
  --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
```

Or in `requirements.txt`:

```
--extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
quixstreams==3.24.1a2
```

## How to use

```python
from datetime import timedelta

state.set("entry", value, ttl=timedelta(seconds=60))
```

- After 60 s, `state.get("entry")` returns `None`.
- TTL is per entry — each `state.set` resets that entry's clock.
- **Entries set without `ttl=` never expire.** Omit the argument when you want a permanent key.

## Important — backwards compatibility

- **If you never pass `ttl=` anywhere**, the store behaves exactly as before: every entry is stored forever. Existing apps need no changes.
- **As soon as you pass `ttl=` for at least one entry**, every other entry set *without* `ttl=` is automatically treated as "never expires". You will not accidentally drop keys just by introducing TTL on one of them.

## Where to use it

Any stateful callback receives a `state` handle. Pass `stateful=True, metadata=True` so the callback signature is `(value, key, timestamp, headers, state)`.

### `sdf.filter` — deduplicate by key

```python
def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False  # drop duplicate
    state.set("status", value["status"], ttl=timedelta(minutes=5))
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

### `sdf.apply` — enrich from short-lived cache

```python
def enrich(value, key, timestamp, headers, state):
    cached = state.get("profile")
    if cached is None:
        cached = fetch_profile(value["user_id"])
        state.set("profile", cached, ttl=timedelta(hours=1))
    return {**value, "profile": cached}

sdf = sdf.apply(enrich, stateful=True, metadata=True)
```

### `sdf.update` — rolling counter with expiry

```python
def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=timedelta(minutes=10))
    value["count"] = count

sdf = sdf.update(bump, stateful=True, metadata=True)
```

## `sdf.filter` — same TTL for every key

Every state entry written by the filter expires after the same duration.

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("last_status") == value["status"]:
        return False
    state.set("last_status", value["status"], ttl=DEFAULT_TTL)
    state.set("last_seen_at",  value["timestamp"], ttl=DEFAULT_TTL)
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

Both `last_status` and `last_seen_at` expire 5 minutes after the most recent write.

## `sdf.filter` — emit only on toggle or after expiration

Example: a window sensor emits readings like `window_open_ON` and `window_open_OFF`. We want the output topic to receive an event **only** when:

1. **The value toggles** — e.g. `ON` → `OFF`, or `OFF` → `ON`.
2. **The TTL has expired** — no event has been seen for 5 minutes, so the next reading (even if identical to the previous one) is treated as fresh and forwarded.

A single stateful key (`last_window_event`) with a TTL does both at once:

```python
from datetime import timedelta

TTL = timedelta(minutes=5)

def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    # value["event"] is e.g. "window_open_ON" or "window_open_OFF"
    cached = state.get("last_window_event")

    if cached == value["event"]:
        # Same value, still within TTL → drop
        return False

    # Either the value toggled, or the cached entry expired (cached is None)
    state.set("last_window_event", value["event"], ttl=TTL)
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)
```

Behavior summary:

| Incoming event | Cached state         | Result      |
|----------------|----------------------|-------------|
| `ON`           | `None` (expired/new) | **emit**    |
| `ON`           | `ON`                 | drop        |
| `OFF`          | `ON`                 | **emit** (toggle) |
| `OFF`          | `None` (expired)     | **emit** (TTL) |

## `sdf.filter` — same pattern when the status lives in the message **key**

If the status is encoded in the Kafka key itself (e.g. `window_open_ON` / `window_open_OFF`) rather than in the value, the previous pattern breaks: Quix Streams partitions state by message key, so `window_open_ON` and `window_open_OFF` each get their **own** state scope and never see each other's `last_status` — the filter would always emit.

Fix: parse the key into `entity` + `status`, then `group_by` the entity so both variants share one state scope.

```python
import os
from datetime import timedelta
from quixstreams import Application

TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))

app = Application(consumer_group="window-dedup", auto_offset_reset="earliest")
input_topic  = app.topic(os.environ["input"])   # keyed as window_open_ON / window_open_OFF
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# 1. Split the key into entity + status, stash both on the value.
def parse_key(value, key, timestamp, headers):
    key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
    entity, _, status = key_str.rpartition("_")   # "window_open_ON" -> ("window_open", "_", "ON")
    return {**(value or {}), "_entity": entity, "_status": status}

sdf = sdf.apply(parse_key, metadata=True)

# 2. Repartition by entity so ON and OFF share one state scope.
sdf = sdf.group_by(lambda v: v["_entity"], name="by_entity")

# 3. Emit only on toggle or after TTL expiry.
def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    status = value["_status"]
    if state.get("last_status") == status:
        return False                              # same status, still within TTL -> drop
    state.set("last_status", status, ttl=TTL)     # toggled OR expired -> emit + reset clock
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)

sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
```

Behavior table is the same as the value-based version above — `state.get("last_status")` returns `None` once the TTL expires, so the next event (even with identical status) is treated as fresh and forwarded.

## Same TTL for every entry

Hoist the TTL into a single constant and reuse it everywhere.

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False
    state.set("status", value["status"], ttl=DEFAULT_TTL)
    return True

def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=DEFAULT_TTL)
    value["count"] = count

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf = sdf.update(bump, stateful=True, metadata=True)
```

Or read it from an env var so it can be tuned without code changes:

```python
import os
from datetime import timedelta

DEFAULT_TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))
```

## Upgrading an existing store — `legacy_records_ttl`

Everything above assumes a store that used TTL from the start. But what about a
store that was **already running before TTL existed** (e.g. a dedup app on an
older Quix Streams) and is now upgraded? Its records were written **without**
`ttl=`, so they are "never expires" — and the new `ttl=` only applies to records
written *after* the upgrade. Worse: the first `ttl=` write on such a populated
store used to **reject** with `IncompatibleStateStoreError` ("delete the state
directory"), which is unusable in Quix Cloud (no customer-callable state reset).

`legacy_records_ttl` fixes this. Set it on `RocksDBOptions` and, on upgrade, the
pre-existing un-stamped records are **backfilled** with a TTL in place — no state
deletion.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    consumer_group="my-dedup",
    rocksdb_options=RocksDBOptions(
        legacy_records_ttl=timedelta(days=7),   # opt-in; default None
    ),
)
```

### Full example — upgrading a dedup app

**Before (the old version, pre-TTL):** a dedup filter that stores a key forever.
Over time the store grows unbounded — every key seen is remembered.

```python
from quixstreams import Application

app = Application(consumer_group="my-dedup", auto_offset_reset="earliest")
sdf = app.dataframe(app.topic("input"))

def dedup(value, key, timestamp, headers, state):
    if state.get("seen"):
        return False                      # already seen — drop
    state.set("seen", True)               # no ttl= → remembered forever
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf = sdf.to_topic(app.topic("output"))
app.run()
```

**After (upgrade):** keep the *same* deployment and state, add
`legacy_records_ttl`, and add `ttl=` to the write. On the first `ttl=` write the
existing forever-keys are backfilled to expire in 7 days, and new keys expire 7
days after they're seen.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    consumer_group="my-dedup",            # same group / same state
    auto_offset_reset="earliest",
    rocksdb_options=RocksDBOptions(
        legacy_records_ttl=timedelta(days=7),   # migrate the old keys on enable
    ),
)
sdf = app.dataframe(app.topic("input"))

def dedup(value, key, timestamp, headers, state):
    if state.get("seen"):
        return False
    state.set("seen", True, ttl=timedelta(days=7))   # now expires
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf = sdf.to_topic(app.topic("output"))
app.run()
```

On startup the log shows the one-time migration:

```
[INFO] [quixstreams] : Backfilled 1234567 legacy records and flipped state store partition into TTL mode (legacy_records_ttl)
```

After it has run once you can drop `legacy_records_ttl` from the options again —
the store stays migrated and never re-backfills.

### How it behaves

- **Opt-in, default `None`.** Unset → behavior is byte-identical to before
  (a populated legacy store still rejects on the first `ttl=` write). Only set it
  when you want the upgrade to migrate old data.
- **Activation gate — only when your code actually uses `ttl=`.** Setting the
  option alone does nothing. The store only migrates when the application
  performs a real `state.set(..., ttl=...)`. No `ttl=` in your code ⇒ the store
  stays legacy and the option is inert.
- **What happens on the first `ttl=` write** (decided at flush):
  - store already migrated (`__ttl_enabled__` set) → nothing, never re-runs;
  - **empty** store → clean flip, nothing to backfill;
  - **populated** + `legacy_records_ttl` set → **backfill** every pre-existing
    record with the TTL, then flip into TTL mode;
  - **populated** + `legacy_records_ttl` unset → reject (with a message pointing
    at `legacy_records_ttl`).
- **One-time, set-once-then-remove.** Backfill is durable (the re-stamped values
  are written to the changelog too), so once it has run you can remove the option
  again — it never re-backfills. The `__ttl_enabled__` flag guarantees it runs
  exactly once in the store's lifetime.
- **Bounded memory.** Backfill runs in chunks
  (`RocksDBOptions.legacy_backfill_chunk_size`, default `10_000`), so peak memory
  is one chunk regardless of store size — a multi-million-record store migrates
  without OOM. Processing is paused for the duration of the (one-time) backfill.
- **Migration-only — it never touches steady-state writes.** `legacy_records_ttl`
  applies *only* to the pre-existing legacy records, once. New records always get
  their lifetime from the **per-write** `ttl=` on `state.set(...)` — a write with
  no `ttl=` stays never-expires, exactly as before. So there are two independent
  sources: `legacy_records_ttl` for the old data (migration), and per-write `ttl=`
  for everything new.

### Reference clock — when do migrated records expire?

Legacy records carry **no original timestamp**, so their true age is
unrecoverable. They are all stamped to expire `legacy_records_ttl` after the
**enable moment**, measured in **event time** (the stream's high-water mark) —
not after each record's real age. So they drain together once the stream's event
time advances `legacy_records_ttl` past the upgrade. Because expiry is event-time
based, the count only drains while **new messages keep arriving**; an idle stream
freezes the clock and nothing expires until traffic resumes.

On a **cold restore** (state volume lost, rebuilt from the changelog), expiry is
judged against **wall-clock at rebuild time** rather than a stamp-derived clock,
so migrated records don't collapse to one on replay.

## Notes

- `state.set(...)` **without** `ttl=` → entry persists forever (always — even on
  a store migrated with `legacy_records_ttl`; that option only stamps the
  pre-existing records, never steady-state writes).
- `state.set(..., ttl=...)` → entry returns `None` from `state.get` once expired,
  regardless of `legacy_records_ttl`.
- On-disk reclamation happens during RocksDB compaction, so disk size lags the
  logical expiry. Watch the logical key count, not the on-disk byte size.
- `legacy_records_ttl` migrates a *populated* legacy store on upgrade; it is a
  one-time, opt-in migration and is inert until your code issues a `ttl=` write.
