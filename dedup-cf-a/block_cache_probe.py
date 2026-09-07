"""
Report the RocksDB table options each store partition is ACTUALLY running with.

Why this exists
---------------
`RocksDBOptions(block_cache_size=..., bloom_filter_bits_per_key=...)` is applied
by rocksdict only to the column families it *creates*. Because
`RocksDBStorePartition._open_rocksdict` opens the DB without a
`column_families=` argument, every pre-existing CF on a **reopen** silently
falls back to rocksdict's defaults: an 8 MiB block cache and no filter policy.
So the very first run of a deployment looks correctly configured and every
restart after that runs degraded, permanently.

Nothing in the SDK logs this, and there is no shell into a Quix container, so
the only way to see it from the Portal logs is to read what RocksDB itself
reports. RocksDB writes the effective per-CF table options into the store's
`LOG` file at open, and rotates the previous one to `LOG.old.<ts>`, so the
current `LOG` always describes the current open.

Reading `LOG` is deliberate: it is RocksDB's own account of what it is running,
not a read-back of the options object we passed in (which would report what we
asked for and tell us nothing).

Expected output
---------------
    unfixed SDK, first run  -> capacity=134217728  filter=bloomfilter
    unfixed SDK, restart    -> capacity=8388608    filter=nullptr      <-- bug
    fixed SDK, restart      -> capacity=134217728  filter=bloomfilter
"""

import os
import re
import threading
import time

# "capacity : 12345" with whitespace around the colon is the block-cache line in
# the block_based_table_factory section. The create-time log also carries an
# unrelated "capacity: 128" (no space before the colon) which this must not
# match, hence the mandatory whitespace.
_CAPACITY_RE = re.compile(r"capacity\s+:\s+(\d+)")
_FILTER_RE = re.compile(r"filter_policy:\s*(\S+)")


def _read_log(path):
    """Effective (capacities, filter policies) from one RocksDB LOG file."""
    try:
        with open(path, "r", errors="replace") as fh:
            text = fh.read()
    except OSError:
        return None
    caps = [int(c) for c in _CAPACITY_RE.findall(text)]
    filters = _FILTER_RE.findall(text)
    if not caps and not filters:
        return None
    return caps, filters


def _find_logs(state_dir):
    """Every current LOG under the state dir — one per store partition."""
    found = []
    for root, _dirs, files in os.walk(state_dir):
        if "LOG" in files:
            found.append(os.path.join(root, "LOG"))
    return sorted(found)


def _rotated_log_count(store_dir):
    """
    How many times RocksDB has opened this store BEFORE now.

    RocksDB rotates LOG to LOG.old.<ts> on every open, so a count of 0 means
    this process CREATED the store and no reopen has happened -- which is the
    one case where the options-dropping bug cannot show up. On an ephemeral
    volume this stays 0 forever no matter how often the deployment restarts,
    so it distinguishes "restarted" from "actually reopened a persisted store".
    """
    try:
        return sum(1 for f in os.listdir(store_dir) if f.startswith("LOG.old."))
    except OSError:
        return -1


def report(state_dir, tag="BLOCKCACHE", publish=None, label=""):
    """
    Print one line per store partition. Returns the number of partitions seen so
    a caller can tell "no stores yet" from "stores reporting".

    ``publish``, if given, is called with a dict per partition. Quix exposes no
    REST endpoint for runtime logs, so stdout alone is unreadable from outside
    the container — publishing the same verdict to a topic is the only way to
    collect this result programmatically.
    """
    logs = _find_logs(state_dir)
    if not logs:
        return 0

    seen = 0
    for log_path in logs:
        parsed = _read_log(log_path)
        if parsed is None:
            continue
        caps, filters = parsed
        seen += 1
        store_dir = os.path.dirname(log_path)
        rel = os.path.relpath(store_dir, state_dir)
        reopens = _rotated_log_count(store_dir)
        uniq_caps = sorted(set(caps))
        uniq_filters = sorted(set(filters))
        # A reopen that dropped the options reports 8388608 / nullptr. Flag it
        # explicitly so the Portal log is readable without doing the arithmetic.
        degraded = any(c == 8 * 1024 * 1024 for c in uniq_caps) or "nullptr" in uniq_filters
        verdict = "DEGRADED (options dropped on reopen)" if degraded else "OK"
        line = (
            f"[STARTUP-{tag}] store={rel} "
            f"block_cache_capacity={uniq_caps} "
            f"filter_policy={uniq_filters} "
            f"cf_count={len(caps)} reopens={reopens} => {verdict}"
        )
        print(line, flush=True)
        if publish is not None:
            try:
                publish(
                    {
                        "label": label,
                        "store": rel,
                        "block_cache_capacity": uniq_caps,
                        "filter_policy": uniq_filters,
                        "cf_count": len(caps),
                        "reopens": reopens,
                        "state_dir": state_dir,
                        "warm": os.environ.get("Quix__Deployment__State__Path") == state_dir,
                        "degraded": degraded,
                        "verdict": verdict,
                    }
                )
            except Exception as exc:  # never let reporting kill the app
                print(f"[STARTUP-{tag}] publish failed: {exc}", flush=True)
    return seen


def start(
    state_dir,
    tag="BLOCKCACHE",
    poll_s=5.0,
    timeout_s=600.0,
    repeat_s=None,
    publish=None,
    label="",
):
    """
    Wait for the first store partition to open, report once, then optionally keep
    reporting every ``repeat_s``.

    Partitions open on assignment, not at ``Application(...)`` construction, so
    this cannot run inline at startup — hence the polling thread.
    """

    def _run():
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if report(state_dir, tag=tag, publish=publish, label=label):
                break
            time.sleep(poll_s)
        else:
            print(
                f"[STARTUP-{tag}] no RocksDB LOG found under {state_dir!r} "
                f"after {timeout_s:.0f}s — no partition assigned, or state is "
                f"not on the path being inspected",
                flush=True,
            )
            return
        if repeat_s:
            while True:
                time.sleep(repeat_s)
                report(state_dir, tag=tag, publish=publish, label=label)

    threading.Thread(target=_run, daemon=True).start()
