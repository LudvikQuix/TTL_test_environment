"""One-shot RocksDB state census -> single JSON object on stdout (spec 6.3).

Walks the state volume, opens every RocksDB store directory, and reports the
migration-relevant facts the runner asserts on:

    total_default_keys, stamped, unstamped, ttl_enabled_flag,
    migration_done_marker, backfill_ledger_count, backfill_pending_count,
    expiry_histogram

It uses the LIBRARY's own column-family names, key constants, and stamp decoder
(never re-implementing the byte layout, spec R-4):

  * CF names / marker keys  <- quixstreams.state.metadata + .rocksdb.metadata
  * stamped-vs-legacy split <- quixstreams.state.rocksdb.transaction
                               ._safe_decode_stamp (falls back to an identical
                               local classifier built on ttl_codec if the
                               transaction module is mid-edit / unimportable)

Access mode (--access):
  * ro (default) - AccessType.read_only(). Correct for a store closed cleanly
                   (graceful `docker stop` flushes memtables -> SST).
  * rw           - AccessType.read_write(), which REPLAYS the WAL on open so
                   fsynced-but-unflushed writes (the mid-SIGKILL case: the ~50-
                   record chunks live in the WAL, not yet in SST) are visible.
                   The runner only uses rw against a THROWAWAY COPY of the
                   volume, never the live one, so a warm restart is undisturbed.

Exit: 0 if every store parsed; 3 if any store dir could not be opened/parsed
(so the runner can tell "census says fail" from "inspector broke"). stdout is
always exactly one JSON object.
"""

import argparse
import json
import os
import sys
import time
import traceback

from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.rocksdb.metadata import (
    TTL_ENABLED_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    TTL_STAMP_BYTES,
    decode_ttl_value,
)

from rocksdict import AccessType, Options, Rdict

# Prefer the library's canonical strict stamp validator. If the transaction
# module is unimportable (e.g. mid-edit by the C1 work), fall back to a byte-
# for-byte-identical local classifier built on the stable ttl_codec leaf.
try:
    from quixstreams.state.rocksdb.transaction import (
        _safe_decode_stamp as _lib_safe_decode,
    )
except Exception:
    _lib_safe_decode = None

# Mirrors quixstreams.state.rocksdb.transaction._MAX_PLAUSIBLE_STAMP_MS (~year
# 33658): any 8-byte prefix at/above this is treated as genuine user bytes, not
# a stamp. This app's legacy JSON values begin with b'{' -> a first-8-bytes
# value ~8.8e18 >> 10**15, so legacy records are correctly classified unstamped.
_MAX_PLAUSIBLE_STAMP_MS = 10**15

_HOUR_MS = 3_600_000
_DAY_MS = 24 * _HOUR_MS


def _fallback_safe_decode(value):
    if len(value) < TTL_STAMP_BYTES:
        return None
    try:
        stamp, payload = decode_ttl_value(value)
    except ValueError:
        return None
    if stamp == SENTINEL_NEVER:
        return stamp, payload
    if 0 < stamp < _MAX_PLAUSIBLE_STAMP_MS:
        return stamp, payload
    return None


safe_decode = _lib_safe_decode or _fallback_safe_decode


def _is_rocksdb_dir(path: str) -> bool:
    # Every RocksDB database directory carries a CURRENT file pointing at the
    # live MANIFEST. This is the robust store-detector (no path assumptions
    # about consumer-group / store-name / partition layout).
    return os.path.isfile(os.path.join(path, "CURRENT"))


def _find_store_dirs(root: str) -> list:
    found = []
    for dirpath, _dirnames, _filenames in os.walk(root):
        if _is_rocksdb_dir(dirpath):
            found.append(dirpath)
    return sorted(found)


def _open_store(path: str, access: str):
    opts = Options(raw_mode=True)
    try:
        cf_names = Rdict.list_cf(path, opts)
    except TypeError:
        cf_names = Rdict.list_cf(path)
    extra = {n: Options(raw_mode=True) for n in cf_names if n != "default"}
    access_type = (
        AccessType.read_write() if access == "rw" else AccessType.read_only()
    )
    db = Rdict(
        path,
        options=opts,
        column_families=extra,
        access_type=access_type,
    )
    return db, set(cf_names)


def _bucket(stamp: int, now_ms: int) -> str:
    if stamp == SENTINEL_NEVER:
        return "never"
    delta = stamp - now_ms
    if delta <= 0:
        return "expired"
    if delta < _HOUR_MS:
        return "lt_1h"
    if delta < _DAY_MS:
        return "h1_24"
    return "gt_24h"


def _count_cf(db, cf_names: set, name: str) -> int:
    if name not in cf_names:
        return 0
    cf = db.get_column_family(name)
    return sum(1 for _ in cf.keys())


def _census_partition(path: str, state_dir: str, access: str, now_ms: int) -> dict:
    db, cf_names = _open_store(path, access)
    try:
        stamped = 0
        unstamped = 0
        # Per-value single-stamp integrity (C1 P0, sc-73191 resume double-wrap).
        # decode_clean   = stamped values whose stripped residue is the real
        #                  payload (NOT itself a stamp) -> single-stamped, healthy.
        # double_wrapped = stamped values whose stripped residue ITSELF decodes as
        #                  a plausible 8-byte stamp -> stamp(stamp(json)), the exact
        #                  corruption the resume produced before the fix.
        decode_clean = 0
        double_wrapped = 0
        histogram = {"expired": 0, "lt_1h": 0, "h1_24": 0, "gt_24h": 0, "never": 0}

        # default CF: iterate user values, classify by the library's stamp
        # detector. Internal keys never land in the default CF for this app, but
        # skip any __...__-shaped key defensively.
        for raw_k, raw_v in db.items():
            key = bytes(raw_k)
            if key.startswith(b"__") and key.endswith(b"__"):
                continue
            value = bytes(raw_v)
            decoded = safe_decode(value)
            if decoded is None:
                unstamped += 1
            else:
                stamp, payload = decoded
                stamped += 1
                histogram[_bucket(stamp, now_ms)] += 1
                # HARNESS-ONLY double-wrap heuristic (byte-inference). Strip the
                # one stamp we just decoded and re-run the SAME strict validator on
                # the residue: a correctly single-stamped value's residue is this
                # app's JSON (first 8 bytes ~8.8e18 >> 10**15 -> not a stamp), so it
                # counts clean; a resume-produced stamp(stamp(json)) leaves a
                # residue that is itself a decodable stamp. This is a TEST-TOOLING
                # assertion only -- the LIBRARY never infers stamped-ness from value
                # content (byte-inference ban, spec R-4 / §6); it decides purely on
                # the __ttl_stamped__ header and CF/ledger membership.
                if safe_decode(payload) is not None:
                    double_wrapped += 1
                else:
                    decode_clean += 1

        total = stamped + unstamped

        # metadata CF: TTL-enabled flag.
        ttl_enabled = False
        if METADATA_CF_NAME in cf_names:
            meta = db.get_column_family(METADATA_CF_NAME)
            flag = meta.get(TTL_ENABLED_KEY, default=None)
            ttl_enabled = flag is not None and bool(bytes(flag))

        # __ttl_system__ CF: durable migration-done marker.
        migration_done = False
        if TTL_SYSTEM_CF_NAME in cf_names:
            system = db.get_column_family(TTL_SYSTEM_CF_NAME)
            migration_done = (
                system.get(TTL_MIGRATION_DONE_KEY, default=None) is not None
            )

        ledger = _count_cf(db, cf_names, TTL_BACKFILL_STAMPED_CF_NAME)
        pending = _count_cf(db, cf_names, TTL_BACKFILL_PENDING_CF_NAME)
        index_count = _count_cf(db, cf_names, TTL_INDEX_CF_NAME)

        try:
            rel = os.path.relpath(path, os.path.dirname(state_dir.rstrip("/\\")))
        except ValueError:
            rel = path

        return {
            "path": rel.replace("\\", "/"),
            "total_default_keys": total,
            "stamped": stamped,
            "unstamped": unstamped,
            "decode_clean": decode_clean,
            "double_wrapped": double_wrapped,
            "ttl_enabled_flag": ttl_enabled,
            "migration_done_marker": migration_done,
            "backfill_ledger_count": ledger,
            "backfill_pending_count": pending,
            "ttl_index_count": index_count,
            "expiry_histogram": histogram,
            "column_families": sorted(cf_names),
        }
    finally:
        db.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="One-shot RocksDB state census.")
    parser.add_argument(
        "--state-dir",
        default=os.environ.get("STATE_DIR", "/app/state"),
        help="root under which to find RocksDB store directories",
    )
    parser.add_argument(
        "--access",
        choices=("ro", "rw"),
        default="ro",
        help="ro=read-only (clean store); rw=read_write (WAL replay, use only "
        "on a throwaway copy for the mid-SIGKILL census)",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    now_ms = int(time.time() * 1000)
    state_dir = args.state_dir
    store_dirs = _find_store_dirs(state_dir) if os.path.isdir(state_dir) else []

    partitions = []
    had_error = False
    for path in store_dirs:
        try:
            partitions.append(
                _census_partition(path, state_dir, args.access, now_ms)
            )
        except Exception as exc:  # per-store parse failure -> non-zero exit
            had_error = True
            partitions.append(
                {
                    "path": path.replace("\\", "/"),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            sys.stderr.write(
                f"[inspector] failed to census {path}: {exc}\n{traceback.format_exc()}\n"
            )

    totals = {
        "total_default_keys": sum(
            p.get("total_default_keys", 0) for p in partitions
        ),
        "stamped": sum(p.get("stamped", 0) for p in partitions),
        "unstamped": sum(p.get("unstamped", 0) for p in partitions),
        "decode_clean": sum(p.get("decode_clean", 0) for p in partitions),
        "double_wrapped": sum(p.get("double_wrapped", 0) for p in partitions),
        "ledger": sum(p.get("backfill_ledger_count", 0) for p in partitions),
        "pending": sum(p.get("backfill_pending_count", 0) for p in partitions),
    }
    result = {
        "state_dir": state_dir,
        "access": args.access,
        "now_ms": now_ms,
        "store_count": len(store_dirs),
        "partitions": partitions,
        "totals": totals,
    }
    print(json.dumps(result))
    return 3 if had_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
