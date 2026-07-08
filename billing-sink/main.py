"""billing-sink entrypoint.

Threading model (spec section 5.1 + quix-rocksdb-state-api): ``app.run()`` owns
the main thread (it installs SIGINT/SIGTERM handlers); uvicorn and the flush-tick
timer run on daemon worker threads. The HTTP handler produces onto billing-events
via a shared producer; the single stateful SDF consumes it, mirrors to State, and
sinks to the Lakehouse.
"""

from __future__ import annotations

import logging
import threading

from dotenv import load_dotenv

load_dotenv()


def _uvicorn_log_level(logger_level: str) -> str:
    return {"off": "warning", "info": "info", "debug": "debug"}.get(logger_level, "info")


def start_http_server(app, port: int, logger_level: str) -> threading.Thread:
    """Run uvicorn on a daemon worker thread (no signal handlers off-main)."""
    import uvicorn

    config = uvicorn.Config(
        app, host="0.0.0.0", port=port, log_level=_uvicorn_log_level(logger_level)
    )
    server = uvicorn.Server(config)
    # app.run() owns SIGINT/SIGTERM; uvicorn must not install its own here.
    server.install_signal_handlers = lambda: None
    thread = threading.Thread(target=server.run, name="uvicorn", daemon=True)
    thread.start()
    return thread


def main() -> None:
    from quixstreams import Application

    from auth import Authorizer
    from config import load_config
    from http_api import create_app
    from lake_writer import build_lakehouse_writer
    from pipeline import FlushController, make_publisher, start_flush_ticker
    from state_buffer import PendingBuffer

    config = load_config()

    logging.basicConfig(level=logging.DEBUG if config.logger_level == "debug" else logging.INFO)
    logging.getLogger("quixstreams").setLevel(
        logging.DEBUG if config.logger_level == "debug" else logging.INFO
    )

    print(
        f"[STARTUP] billing-sink events_topic={config.events_topic} "
        f"state_key={config.state_key} consumer_group={config.consumer_group} "
        f"batch_size={config.batch_size} flush_interval_s={config.flush_interval_seconds} "
        f"dedup_ttl_s={config.dedup_ttl_seconds} lake_table={config.lake_table} "
        f"query_url_set={bool(config.query_url)} schema_version={config.schema_version} "
        f"auth_enabled={config.auth_enabled} auth_permission={config.auth_required_permission} "
        f"http_port={config.http_port} logger={config.logger_level}",
        flush=True,
    )

    # Fail fast: Lakehouse writes go through the Query API /insert, which needs
    # both vars (auto-injected on dev). Missing => no writer can ever confirm.
    if not config.query_url or not config.query_token:
        print(
            "[STARTUP] FATAL: Quix__Lakehouse__Query__Url / __AuthToken not set "
            "(auto-inject on dev). quixlake-sdk writes require both; set them or "
            "QUIXLAKE_URL / QUIX_LAKE_TOKEN locally. Exiting.",
            flush=True,
        )
        raise SystemExit(1)

    buffer = PendingBuffer()
    writer = build_lakehouse_writer(config)
    controller = FlushController(config, buffer, writer)

    app = Application(
        consumer_group=config.consumer_group,
        state_dir=config.state_dir,
        auto_offset_reset="earliest",
    )
    topic = app.topic(
        config.events_topic, value_serializer="json", value_deserializer="json"
    )
    sdf = app.dataframe(topic)
    sdf.update(controller.handle, stateful=True)

    producer = app.get_producer()
    publish = make_publisher(producer, topic, config.state_key)
    authorizer = Authorizer(
        workspace_id=config.workspace_id,
        required_permission=config.auth_required_permission,
        cache_seconds=config.auth_cache_seconds,
        enabled=config.auth_enabled,
    )
    api = create_app(config, buffer, publish, authorizer)

    stop_event = threading.Event()
    start_http_server(api, config.http_port, config.logger_level)
    start_flush_ticker(
        producer,
        topic,
        config.state_key,
        config.flush_interval_seconds,
        stop_event,
        config.logger_level,
    )

    try:
        app.run()
    finally:
        stop_event.set()
        producer.flush(5)


if __name__ == "__main__":
    main()
