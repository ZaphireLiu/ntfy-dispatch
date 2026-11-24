"""Application wiring and lifecycle."""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from .config import (
    AppConfig,
    load_config,
    missing_secret_envs,
    validate_runtime_config,
)
from .dedupe import DedupePruner, DedupeStore
from .publisher import Publisher
from .registry import (
    Registry,
    get_registry,
    load_handler_packages,
    validate_registry,
)
from .stats import Stats, StatsReporter
from .stream import NtfyReader
from .version import __version__
from .worker import HandlerWorker

logger = logging.getLogger("ntfy_dispatch")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    # ntfy topic names are effectively passwords; don't let httpx log the
    # full subscription URL at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class App:
    def __init__(
        self,
        *,
        config: AppConfig,
        registry: Registry,
        stats: Stats,
        dedupe: DedupeStore,
        publisher: Publisher,
        workers: dict[str, HandlerWorker],
        reader: NtfyReader,
        shutdown_event: threading.Event,
        worker_stop_event: threading.Event,
        stats_reporter: StatsReporter,
        dedupe_pruner: DedupePruner,
    ) -> None:
        self.config = config
        self.registry = registry
        self.stats = stats
        self.dedupe = dedupe
        self.publisher = publisher
        self.workers = workers
        self.reader = reader
        self.shutdown_event = shutdown_event
        self.worker_stop_event = worker_stop_event
        self.stats_reporter = stats_reporter
        self.dedupe_pruner = dedupe_pruner
        self._started = False
        self._shutdown_done = False

    @classmethod
    def build(cls, config: AppConfig, registry: Registry) -> App:
        topics = validate_registry(registry)
        specs = registry.handlers()
        known_names = {spec.name for spec in specs}
        unknown_configs = sorted(set(config.handler_configs) - known_names)
        if unknown_configs:
            logger.warning(
                "configuration sections for unknown handlers: %s",
                ", ".join(unknown_configs),
            )

        config.core.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stats = Stats()
        publisher = Publisher(config.ntfy, timeout=config.core.http_timeout)
        dedupe = DedupeStore(
            config.dedupe.path,
            retention_days=config.dedupe.retention_days,
            max_rows=config.dedupe.max_rows,
            memory_cache=config.dedupe.memory_cache,
            stats=stats,
        )

        shutdown_event = threading.Event()
        worker_stop_event = threading.Event()
        workers: dict[str, HandlerWorker] = {}

        try:
            for spec in specs:
                handler_config = dict(config.handler_configs.get(spec.name, {}))
                workers[spec.name] = HandlerWorker(
                    spec,
                    handler_config,
                    instance=config.ntfy.name,
                    default_queue_size=config.core.queue_size,
                    http_timeout=config.core.http_timeout,
                    shutdown_event=shutdown_event,
                    stop_event=worker_stop_event,
                    stats=stats,
                    publisher=publisher,
                    listened_topics=topics,
                )

            reader = NtfyReader(
                config=config.ntfy,
                topics=topics,
                handlers_by_topic=registry.by_topic(),
                workers=workers,
                dedupe=dedupe,
                stats=stats,
                stop_event=shutdown_event,
            )
            stats_reporter = StatsReporter(
                stats,
                config.core.stats_interval,
                shutdown_event,
                logger=logging.getLogger("ntfy_dispatch.stats"),
            )
            dedupe_pruner = DedupePruner(
                dedupe,
                config.dedupe.prune_interval_seconds,
                shutdown_event,
            )
        except Exception:
            publisher.close()
            dedupe.close()
            raise

        return cls(
            config=config,
            registry=registry,
            stats=stats,
            dedupe=dedupe,
            publisher=publisher,
            workers=workers,
            reader=reader,
            shutdown_event=shutdown_event,
            worker_stop_event=worker_stop_event,
            stats_reporter=stats_reporter,
            dedupe_pruner=dedupe_pruner,
        )

    def start(self) -> None:
        if self._started:
            raise RuntimeError("app is already started")
        for worker in self.workers.values():
            worker.start()
        self.stats_reporter.start()
        self.dedupe_pruner.start()
        self.reader.start()
        self._started = True
        logger.info(
            "started ntfy-dispatch %s: instance=%s base_url=%s topics=%d handlers=%d",
            __version__,
            self.config.ntfy.name,
            self.config.ntfy.base_url,
            len(self.registry.topics()),
            len(self.workers),
        )

    def run_until_stopped(self) -> None:
        try:
            self.start()
            while not self.shutdown_event.wait(1.0):
                pass
        except KeyboardInterrupt:
            self.shutdown_event.set()
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        logger.info("shutting down")
        self.shutdown_event.set()

        self.reader.stop()
        reader_stopped = self.reader.join(timeout=5.0)
        if not reader_stopped:
            logger.warning(
                "ntfy reader did not stop within 5s; daemon thread will exit with the process"
            )

        deadline = time.monotonic() + self.config.core.shutdown_drain_seconds
        for worker in self.workers.values():
            worker.request_stop(deadline)
        for name, worker in self.workers.items():
            remaining = max(0.0, deadline - time.monotonic())
            if not worker.join(remaining):
                logger.warning("handler %s did not stop within drain deadline", name)

        if self.stats_reporter.ident is not None:
            self.stats_reporter.join(timeout=1.0)
        if self.dedupe_pruner.ident is not None:
            self.dedupe_pruner.join(timeout=1.0)

        try:
            self.publisher.close()
        except Exception:
            logger.exception("failed to close publisher")

        if reader_stopped:
            self.dedupe.close()
        else:
            logger.warning(
                "skipping dedupe close because the reader thread is still running; "
                "SQLite will recover from the WAL on next start"
            )
        logger.info("final stats: %s", self.stats.format_summary())
        logger.info("shutdown complete")


def _missing_secret_warnings(config: AppConfig) -> list[str]:
    return missing_secret_envs(config.ntfy)


def _print_check_summary(config: AppConfig, registry: Registry, topics: frozenset[str]) -> None:
    specs = registry.handlers()
    print("config OK")
    print(f"ntfy: {config.ntfy.name} {config.ntfy.base_url}")
    print(f"handlers: {len(specs)}")
    for spec in specs:
        queue_size = spec.queue_size if spec.queue_size is not None else config.core.queue_size
        print(f"  - {spec.name}: topics={','.join(sorted(spec.topics))} queue={queue_size}")
    print(f"topics ({len(topics)}): {','.join(sorted(topics))}")

    known_names = {spec.name for spec in specs}
    unknown_configs = sorted(set(config.handler_configs) - known_names)
    if unknown_configs:
        print(
            "warning: configuration sections for unknown handlers: " + ", ".join(unknown_configs),
            file=sys.stderr,
        )

    for env_name in _missing_secret_warnings(config):
        print(f"warning: environment variable {env_name} is not set", file=sys.stderr)


def run_app(
    config_path: str | Path,
    *,
    check: bool = False,
    log_level: str | None = None,
) -> int:
    config = load_config(config_path)
    setup_logging(log_level or config.core.log_level)

    load_handler_packages(config.core.handler_packages)
    registry = get_registry()
    topics = validate_registry(registry)

    if check:
        _print_check_summary(config, registry, topics)
        return 0

    validate_runtime_config(config)
    app = App.build(config, registry)

    def handle_signal(signum: int, frame: Any) -> None:
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        logger.info("received %s; shutting down", name)
        app.shutdown_event.set()

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except ValueError:
        logger.debug("not running in the main thread; signal handlers not installed")

    app.run_until_stopped()
    return 0
