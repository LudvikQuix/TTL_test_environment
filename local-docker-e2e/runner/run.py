"""Host-side orchestrator for the local docker E2E TTL-migration harness.

    python runner/run.py --scenario all        # A, B, C
    python runner/run.py --scenario C           # one scenario
    python runner/run.py --scenario A --keep    # leave volumes on failure

Exit codes (spec 7.5):
    0  all requested scenarios passed
    1  at least one assertion failed
    2  harness / infra error (broker down, kill window missed, inspector broke)

Runs on the Windows host, Python-only; drives docker via subprocess. kafka is
brought up once (long-lived) and reused; the shared app image is built from the
local tree; each scenario is fully isolated (fresh CG_VERSION + volume + topics).
"""

import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import docker_ctl as dc  # noqa: E402
import scenarios  # noqa: E402

DEFAULTS_ENV = os.path.join(ROOT, "config", "defaults.env")


def load_env(path):
    cfg = {}
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            cfg[key.strip()] = val.strip()
    return cfg


def parse_args(argv):
    p = argparse.ArgumentParser(description="Local docker E2E TTL-migration harness runner.")
    p.add_argument("--scenario", choices=["A", "B", "C", "all"], default="all")
    p.add_argument("--keep", action="store_true", help="on failure, leave the state volume for debugging")
    p.add_argument("--skip-build", action="store_true", help="reuse the existing app image")
    p.add_argument("--lib-path", default=None, help="override the quix-streams host path (default C:/repos/quix-streams-Main)")
    p.add_argument("--kafka-timeout", type=int, default=120)
    return p.parse_args(argv)


def _print_result(checks, elapsed):
    if checks.passed:
        print(f"PASS {checks.scenario}  ({elapsed:.0f}s)", flush=True)
    else:
        print(f"FAIL {checks.scenario}: {checks.first_failure}  ({elapsed:.0f}s)", flush=True)
    for note in checks.notes:
        print(f"    - {note}", flush=True)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.lib_path:
        dc.LIB_HOST_PATH = args.lib_path
        dc.LIB_MOUNT = f"{args.lib_path}:/quix-streams:ro"

    if not os.path.isdir(dc.LIB_HOST_PATH):
        print(f"[runner] library path not found: {dc.LIB_HOST_PATH}", flush=True)
        return 2

    cfg = load_env(DEFAULTS_ENV)
    want = ["A", "B", "C"] if args.scenario == "all" else [args.scenario]

    # ---- infra bring-up (infra failures => exit 2) ----
    try:
        print("[runner] bringing up kafka ...", flush=True)
        dc.compose_up_kafka(args.kafka_timeout)
        if not args.skip_build:
            print("[runner] building app image (quixstreams is bind-mounted, not baked) ...", flush=True)
            dc.build_app_image()
    except dc.HarnessError as exc:
        print(f"[runner] INFRA ERROR during bring-up: {exc}", flush=True)
        return 2

    # ---- scenarios ----
    results = []
    infra_error = None
    for name in want:
        print(f"\n[runner] ===== scenario {name} =====", flush=True)
        start = time.time()
        try:
            checks = scenarios.SCENARIOS[name](cfg)
        except dc.HarnessError as exc:
            print(f"[runner] INFRA ERROR in scenario {name}: {exc}", flush=True)
            infra_error = exc
            break
        results.append(checks)
        _print_result(checks, time.time() - start)

    # ---- teardown ----
    any_failed = any(not c.passed for c in results)
    keep = args.keep and (any_failed or infra_error is not None)
    print("\n[runner] tearing down (kafka left up for reuse) ...", flush=True)
    dc.stop_app()
    if keep:
        print("[runner] --keep: leaving the state volume in place for debugging", flush=True)
    else:
        dc.volume_rm(dc.STATE_VOLUME)
        dc.volume_rm(dc.SNAP_VOLUME)

    # ---- summary + exit code ----
    print("\n[runner] summary:", flush=True)
    for c in results:
        print(f"    {c.scenario}: {'PASS' if c.passed else 'FAIL'}", flush=True)
    if infra_error is not None:
        print(f"    (aborted on infra error: {infra_error})", flush=True)
        return 2
    if any_failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
