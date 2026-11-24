"""In-process counters and periodic reporting."""

from __future__ import annotations

import logging
import threading
from typing import Any


class Stats:
    """Small thread-safe counter store."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._handlers: dict[str, dict[str, int]] = {}
        self._queue_depths: dict[str, int] = {}

    def inc(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def inc_handler(self, handler: str, key: str, amount: int = 1) -> None:
        with self._lock:
            values = self._handlers.setdefault(handler, {})
            values[key] = values.get(key, 0) + amount

    def set_handler_depth(self, handler: str, depth: int) -> None:
        with self._lock:
            self._queue_depths[handler] = depth

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "handlers": {name: dict(values) for name, values in self._handlers.items()},
                "queue_depth": dict(self._queue_depths),
            }

    def format_summary(self) -> str:
        snapshot = self.snapshot()
        counters = " ".join(f"{key}={value}" for key, value in sorted(snapshot["counters"].items()))
        if not counters:
            counters = "no global activity"
        handler_parts = []
        for name in sorted(snapshot["handlers"]):
            values = snapshot["handlers"][name]
            depth = snapshot["queue_depth"].get(name, 0)
            details = " ".join(f"{key}={value}" for key, value in sorted(values.items()))
            handler_parts.append(f"{name}({details} queue={depth})")
        handlers = "; ".join(handler_parts) if handler_parts else "no handler activity"
        return f"{counters} | {handlers}"


class StatsReporter(threading.Thread):
    def __init__(
        self,
        stats: Stats,
        interval: float,
        stop_event: threading.Event,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(name="stats", daemon=True)
        self._stats = stats
        self._interval = interval
        self._stop_event = stop_event
        self._logger = logger or logging.getLogger("ntfy_dispatch.stats")

    def run(self) -> None:
        while not self._stop_event.wait(self._interval):
            self._logger.info("stats: %s", self._stats.format_summary())
