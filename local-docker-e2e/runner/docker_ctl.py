"""Thin subprocess wrappers around the docker CLI for the E2E runner (spec 6.4).

The runner drives app/seeder/inspector lifecycle with `docker run` (NOT compose
depends_on) because the scenarios need precise, ordered start -> kill -> wipe ->
restart control that the compose graph cannot express. Only the long-lived
kafka broker is a compose service.

All host paths are absolute; cwd is irrelevant.
"""

import json
import os
import queue
import re
import subprocess
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COMPOSE_FILE = os.path.join(ROOT, "docker-compose.yml")

IMAGE = "ttl-e2e-app:latest"
NETWORK = "ttl_e2e_net"
KAFKA_CONTAINER = "ttl_e2e_kafka"
APP_CONTAINER = "ttl_e2e_app"
STATE_VOLUME = "ttl_e2e_state"
SNAP_VOLUME = "ttl_e2e_state_snap"

# Read-only host bind mount of the library working tree (spec 4/5.1).
LIB_HOST_PATH = os.environ.get("QS_LIB_PATH", "C:/repos/quix-streams-Main")
LIB_MOUNT = f"{LIB_HOST_PATH}:/quix-streams:ro"
STATE_MOUNT = f"{STATE_VOLUME}:/app/state"


class HarnessError(Exception):
    """Infra / docker failure -> runner exit code 2."""


def _run(cmd, check=True, capture=True, timeout=None):
    proc = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise HarnessError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
        )
    return proc


def _compose(args, **kw):
    return _run(["docker", "compose", "-f", COMPOSE_FILE, *args], **kw)


def _env_args(env):
    out = []
    for key, val in env.items():
        out += ["-e", f"{key}={val}"]
    return out


# --------------------------------------------------------------------------
# Broker lifecycle (compose)
# --------------------------------------------------------------------------
def compose_up_kafka(timeout_s=90):
    _compose(["up", "-d", "kafka"])
    wait_for_healthy(KAFKA_CONTAINER, timeout_s)


def wait_for_healthy(container, timeout_s):
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        proc = _run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            check=False,
        )
        last = (proc.stdout or "").strip()
        if last == "healthy":
            return
        if last == "unhealthy":
            raise HarnessError(f"{container} reported unhealthy")
        time.sleep(2)
    raise HarnessError(f"{container} not healthy within {timeout_s}s (last={last!r})")


def compose_down(remove_state=True):
    _compose(["down", "-v", "--remove-orphans"], check=False)
    # The app container is started via `docker run` (not a compose service), so
    # `compose down` may not remove it; remove it explicitly so the state volume
    # is free for the now-loud volume_rm below (else teardown would raise).
    rm_container(APP_CONTAINER)
    if remove_state:
        volume_rm(STATE_VOLUME)
    volume_rm(SNAP_VOLUME)


def build_app_image():
    _compose(["build", "app"], capture=True)


# --------------------------------------------------------------------------
# Volumes
# --------------------------------------------------------------------------
def _volume_exists(name):
    return _run(["docker", "volume", "inspect", name], check=False).returncode == 0


def volume_rm(name):
    _run(["docker", "volume", "rm", "-f", name], check=False)
    # Fail LOUDLY if the volume survived removal. `docker volume rm -f` does NOT
    # force-remove an in-use volume (-f only suppresses the "no such volume"
    # error) — so a surviving volume means a stopped-but-not-removed container
    # (e.g. a SIGKILLed app) still references it. Swallowing that failure silently
    # turns a "cold wipe" (volume_recreate) into a WARM restart on the old data,
    # masking real regressions — the exact scenario-B false-fail. Raise instead.
    if _volume_exists(name):
        raise HarnessError(
            f"docker volume {name!r} still exists after 'docker volume rm -f' — it "
            f"is still referenced by a stopped-but-not-removed container. Refusing "
            f"to continue: a cold wipe that silently no-ops would run WARM and mask "
            f"a regression. Remove the referencing container before recreating."
        )


def volume_create(name):
    _run(["docker", "volume", "create", name])


def volume_recreate(name=STATE_VOLUME):
    volume_rm(name)
    volume_create(name)


# --------------------------------------------------------------------------
# App container (long-lived; runner owns start/kill/restart)
# --------------------------------------------------------------------------
def rm_container(name):
    _run(["docker", "rm", "-f", name], check=False)


def run_app(env, name=APP_CONTAINER):
    rm_container(name)
    cmd = [
        "docker", "run", "-d", "--name", name,
        "--network", NETWORK,
        "-v", STATE_MOUNT,
        "-v", LIB_MOUNT,
        *_env_args(env),
        IMAGE,
        "python", "/app/app/main.py",
    ]
    _run(cmd)
    return name


def stop_app(name=APP_CONTAINER, timeout_s=30):
    """Graceful SIGTERM stop -> quixstreams flushes + closes stores cleanly
    (memtables -> SST), so a subsequent read-only census is complete."""
    _run(["docker", "stop", "-t", str(timeout_s), name], check=False)
    rm_container(name)


def kill_app(name=APP_CONTAINER):
    """SIGKILL — no flush; fsynced per-chunk writes stay in the WAL. The killed
    container is then REMOVED (mirroring stop_app). The named state volume and its
    WAL persist across `docker rm` (only the container is removed), so the SIGKILL
    semantics are intact — but the volume is no longer referenced by a dead
    container, so a subsequent `volume_recreate` cold wipe is not silently blocked
    (which would run WARM). See volume_rm's loud check."""
    _run(["docker", "kill", "--signal=KILL", name], check=False)
    rm_container(name)


# --------------------------------------------------------------------------
# Seeder / inspector (one-shot; --rm)
# --------------------------------------------------------------------------
def run_seeder(seeder_args, env):
    cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-v", LIB_MOUNT,
        *_env_args(env),
        IMAGE,
        "python", "/app/seeder/seeder.py", *seeder_args,
    ]
    proc = _run(cmd, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def _inspect_live(access, env):
    cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-v", STATE_MOUNT,
        "-v", LIB_MOUNT,
        *_env_args(env),
        IMAGE,
        "python", "/app/inspect/inspect_state.py",
        "--state-dir", "/app/state", "--access", access,
    ]
    return _run(cmd, check=False)


def _snapshot_volume():
    """Copy the live state volume into a throwaway volume so a WAL-replaying
    (rw) census can run without disturbing the live volume (needed for the
    mid-SIGKILL census; the live volume must stay pristine for a warm restart)."""
    volume_rm(SNAP_VOLUME)
    volume_create(SNAP_VOLUME)
    _run([
        "docker", "run", "--rm",
        "-v", f"{STATE_VOLUME}:/src:ro",
        "-v", f"{SNAP_VOLUME}:/dst",
        "--entrypoint", "sh",
        IMAGE,
        "-c", "cp -a /src/. /dst/ 2>/dev/null || true",
    ])


def _inspect_snapshot(env):
    cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-v", f"{SNAP_VOLUME}:/app/state",
        "-v", LIB_MOUNT,
        *_env_args(env),
        IMAGE,
        "python", "/app/inspect/inspect_state.py",
        "--state-dir", "/app/state", "--access", "rw",
    ]
    return _run(cmd, check=False)


def run_inspector(env, access="ro", snapshot=False):
    """Return the parsed census dict. snapshot=True copies the volume and does a
    WAL-replaying rw census on the copy (mid-SIGKILL). Raises HarnessError if the
    inspector process errored (non-zero exit) or emitted no parseable JSON."""
    if snapshot:
        _snapshot_volume()
        proc = _inspect_snapshot(env)
    else:
        proc = _inspect_live(access, env)

    census = _parse_last_json(proc.stdout)
    if snapshot:
        volume_rm(SNAP_VOLUME)

    if census is None:
        raise HarnessError(
            f"inspector emitted no parseable JSON (rc={proc.returncode})\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
        )
    if proc.returncode != 0:
        raise HarnessError(
            f"inspector reported a parse error (rc={proc.returncode}); "
            f"census={json.dumps(census)}\nstderr: {proc.stderr}"
        )
    return census


def _parse_last_json(text):
    # The inspector prints exactly one JSON object on stdout, but be defensive
    # about any stray lines: take the last line that parses as a JSON object.
    for line in reversed((text or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


# --------------------------------------------------------------------------
# Topics
# --------------------------------------------------------------------------
def _kafka_exec(args, check=False, timeout=None):
    return _run(
        ["docker", "exec", KAFKA_CONTAINER, *args], check=check, timeout=timeout
    )


def list_topics():
    proc = _kafka_exec(
        ["kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
    )
    return [t.strip() for t in (proc.stdout or "").splitlines() if t.strip()]


def delete_topics(match_substrings, wait_s=30):
    """Best-effort delete of every topic whose name contains any of the given
    substrings; then wait until they are gone (async delete)."""
    to_delete = [
        t for t in list_topics()
        if any(sub in t for sub in match_substrings) and not t.startswith("__")
    ]
    for topic in to_delete:
        _kafka_exec([
            "kafka-topics", "--bootstrap-server", "localhost:9092",
            "--delete", "--topic", topic,
        ])
    deadline = time.time() + wait_s
    while time.time() < deadline:
        remaining = [t for t in list_topics() if t in to_delete]
        if not remaining:
            return to_delete
        time.sleep(1)
    return to_delete


def topic_end_offset(topic):
    """Sum of end offsets across partitions for `topic` (0 if absent)."""
    proc = _kafka_exec([
        "kafka-get-offsets", "--bootstrap-server", "localhost:9092",
        "--topic", topic, "--time", "-1",
    ])
    total = 0
    for line in (proc.stdout or "").splitlines():
        # format: topic:partition:offset
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            try:
                total += int(parts[2])
            except ValueError:
                pass
    return total


# --------------------------------------------------------------------------
# Log following (deterministic kill timing, spec 6.5)
# --------------------------------------------------------------------------
class LogFollower:
    """Stream `docker logs -f <name>` (container stdout+stderr merged) line by
    line into a queue, keeping a full capture for later grep."""

    def __init__(self, name):
        self.name = name
        self.lines = []
        self._q = queue.Queue()
        self._proc = subprocess.Popen(
            ["docker", "logs", "-f", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._t = threading.Thread(target=self._reader, daemon=True)
        self._t.start()

    def _reader(self):
        try:
            for line in self._proc.stdout:
                line = line.rstrip("\n")
                self.lines.append(line)
                self._q.put(line)
        finally:
            self._q.put(None)  # EOF sentinel

    def next_line(self, timeout):
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return _TIMEOUT

    def stop(self):
        try:
            self._proc.terminate()
        except Exception:
            pass


_TIMEOUT = object()  # sentinel distinct from None(EOF)/str(line)


def wait_for_pattern(follower, pattern, timeout_s):
    """Block until a log line matches `pattern` (regex). Returns the matching
    line, or None on timeout / stream EOF."""
    rx = re.compile(pattern)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        line = follower.next_line(timeout=max(0.5, deadline - time.time()))
        if line is _TIMEOUT:
            continue
        if line is None:
            return None
        if rx.search(line):
            return line
    return None


def kill_on_nth_progress(follower, started_rx, progress_rx, finished_rx,
                         n, kill_fn, timeout_s):
    """Wait for STARTED, then count progress lines; on the Nth, call kill_fn().
    Aborts (returns an error string) if STARTED is not seen in time or FINISHED
    appears before the Nth progress line (kill window missed, spec 6.5)."""
    started = re.compile(started_rx)
    progress = re.compile(progress_rx)
    finished = re.compile(finished_rx)

    deadline = time.time() + timeout_s
    seen_started = False
    count = 0
    while time.time() < deadline:
        line = follower.next_line(timeout=max(0.5, deadline - time.time()))
        if line is _TIMEOUT:
            continue
        if line is None:
            return "log stream ended before kill (container exited?)"
        if not seen_started:
            if started.search(line):
                seen_started = True
            continue
        if finished.search(line):
            return "backfill FINISHED before the Nth progress line (kill window missed)"
        if progress.search(line):
            count += 1
            if count >= n:
                kill_fn()
                return None
    if not seen_started:
        return f"STARTED not seen within {timeout_s}s"
    return f"only {count} progress line(s) before timeout (needed {n})"
