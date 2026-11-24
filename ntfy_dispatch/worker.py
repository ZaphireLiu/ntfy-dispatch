"""Per-handler queues and worker threads."""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from collections.abc import Mapping
from typing import Any

import httpx

from .models import Message
from .registry import HandlerSpec
from .stats import Stats

DROP_LOG_INTERVAL = 30.0


class HandlerContext:
    """Runtime services available to a handler.

    A context belongs to exactly one worker thread, so handlers do not need to
    lock their own state.
    """

    def __init__(
        self,
        *,
        name: str,
        config: Mapping[str, Any],
        logger: logging.Logger,
        instance: str,
        shutdown_event: threading.Event,
        http_timeout: float,
    ) -> None:
        self.name = name
        self.config = dict(config)
        self.logger = logger
        self.instance = instance
        self.shutdown_event = shutdown_event
        self._http_timeout = http_timeout
        self._http: httpx.Client | None = None

    @property
    def http(self) -> httpx.Client:
        """A lazily created HTTP client owned by this handler's worker."""

        if self._http is None:
            self._http = httpx.Client(
                timeout=self._http_timeout,
                follow_redirects=True,
                trust_env=True,
            )
        return self._http

    def env(self, name: str, default: str | None = None) -> str:
        value = os.environ.get(name, default)
        if value is None:
            raise RuntimeError(f"environment variable {name!r} is not set")
        return value

    def close(self) -> None:
        client = self._http
        self._http = None
        if client is not None:
            client.close()


class HandlerWorker:
    """A bounded FIFO queue plus one consuming thread for a single handler."""

    def __init__(
        self,
        spec: HandlerSpec,
        handler_config: Mapping[str, Any],
        *,
        instance: str,
        default_queue_size: int,
        http_timeout: float,
        shutdown_event: threading.Event,
        stop_event: threading.Event,
        stats: Stats,
    ) -> None:
        self.spec = spec
        queue_size = spec.queue_size if spec.queue_size is not None else default_queue_size
        if queue_size <= 0:
            raise ValueError(f"invalid queue size for handler {spec.name!r}: {queue_size}")
        self.queue: queue.Queue[Message] = queue.Queue(maxsize=queue_size)
        self._stats = stats
        self._stop_event = stop_event
        self._logger = logging.getLogger(f"ntfy_dispatch.handler.{spec.name}")
        self._thread = threading.Thread(
            target=self._run,
            name=f"handler:{spec.name}",
            daemon=True,
        )
        self._deadline: float | None = None
        self._last_drop_log: float | None = None
        self._dropped = 0
        self.ctx = HandlerContext(
            name=spec.name,
            config=handler_config,
            logger=self._logger,
            instance=instance,
            shutdown_event=shutdown_event,
            http_timeout=http_timeout,
        )

    def start(self) -> None:
        self._thread.start()

    def submit(self, message: Message) -> bool:
        """Enqueue a message without ever blocking the reader."""

        try:
            self.queue.put_nowait(message)
        except queue.Full:
            self._dropped += 1
            self._stats.inc("dropped")
            self._stats.inc_handler(self.spec.name, "dropped")
            self._log_drop(message)
            return False

        self._stats.inc("enqueued")
        self._stats.inc_handler(self.spec.name, "enqueued")
        self._stats.set_handler_depth(self.spec.name, self.queue.qsize())
        return True

    def _log_drop(self, message: Message) -> None:
        now = time.monotonic()
        if self._last_drop_log is not None and now - self._last_drop_log < DROP_LOG_INTERVAL:
            return
        self._last_drop_log = now
        self._logger.warning(
            "queue full; dropping new message handler=%s id=%s topic=%s dropped=%d depth=%d",
            self.spec.name,
            message.id,
            message.topic,
            self._dropped,
            self.queue.qsize(),
        )

    def _run(self) -> None:
        try:
            while True:
                if self._stop_event.is_set():
                    if self.queue.empty():
                        break
                    if self._deadline is not None and time.monotonic() >= self._deadline:
                        remaining = self.queue.qsize()
                        if remaining:
                            self._logger.warning(
                                "drain deadline reached; %d message(s) left in queue", remaining
                            )
                        break

                try:
                    message = self.queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                try:
                    self.spec.func(message, self.ctx)
                except Exception:
                    self._stats.inc("failed")
                    self._stats.inc_handler(self.spec.name, "failed")
                    self._logger.exception(
                        "handler failed handler=%s id=%s topic=%s",
                        self.spec.name,
                        message.id,
                        message.topic,
                    )
                else:
                    self._stats.inc("processed")
                    self._stats.inc_handler(self.spec.name, "processed")
                finally:
                    self.queue.task_done()
                    self._stats.set_handler_depth(self.spec.name, self.queue.qsize())
        finally:
            try:
                self.ctx.close()
            except Exception:
                self._logger.exception("failed to close handler context")
            self._logger.info("worker stopped")

    def request_stop(self, deadline: float | None = None) -> None:
        self._deadline = deadline
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> bool:
        if self._thread.ident is None:
            return True
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    @property
    def queue_depth(self) -> int:
        return self.queue.qsize()
