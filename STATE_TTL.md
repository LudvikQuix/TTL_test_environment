# Quix Streams — State TTL (preview)

State entries can now expire automatically. Pass a `ttl=` to `state.set(...)` and the entry disappears after that duration.

## Install

```bash
pip install quixstreams==3.24.0a1 \
  --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
```

Or in `requirements.txt`:

```
--extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
quixstreams==3.24.0a1
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

## Notes

- `state.set(...)` **without** `ttl=` → entry persists forever.
- `state.set(..., ttl=...)` → entry returns `None` from `state.get` once expired.
- On-disk reclamation happens during RocksDB compaction, so disk size lags the logical expiry.
