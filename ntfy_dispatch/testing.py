"""Helpers for testing handlers outside the running application.

The helpers in this module do not depend on pytest, so they can also be used
from plain scripts.  Pytest users additionally get registry isolation from the
``pytest11`` plugin shipped with this package.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from typing import Any

from .models import Message
from .registry import reset_registry

__all__ = ["FakeContext", "make_message", "reset_registry"]


def make_message(
    *,
    id: str = "test-message",
    topic: str = "test-topic",
    message: str = "",
    instance: str = "test",
    time: int = 1_700_000_000,
    title: str | None = None,
    priority: int = 3,
    tags: tuple[str, ...] = (),
    click: str | None = None,
    raw: Mapping[str, Any] | None = None,
) -> Message:
    """Build a :class:`Message` with sensible defaults for tests."""

    return Message(
        instance=instance,
        topic=topic,
        id=id,
        time=time,
        message=message,
        title=title,
        priority=priority,
        tags=tags,
        click=click,
        raw=raw or {},
    )


class FakeContext:
    """A stand-in for :class:`~ntfy_dispatch.worker.HandlerContext`.

    ``config`` mirrors ``[handlers.<name>]``.  ``env`` is an explicit mapping
    instead of the process environment so tests stay hermetic.  ``http`` can be
    injected, usually an ``httpx.Client`` built on ``httpx.MockTransport``;
    accessing it without a client raises.
    """

    def __init__(
        self,
        *,
        name: str = "test",
        config: Mapping[str, Any] | None = None,
        instance: str = "test",
        env: Mapping[str, str] | None = None,
        http: Any = None,
    ) -> None:
        self.name = name
        self.config = dict(config or {})
        self.instance = instance
        self.logger = logging.getLogger(f"test.handler.{name}")
        self.shutdown_event = threading.Event()
        self._env = dict(env or {})
        self._http = http

    @property
    def http(self) -> Any:
        if self._http is None:
            raise RuntimeError("FakeContext has no http client; pass http=... to use it")
        return self._http

    def env(self, name: str, default: str | None = None) -> str:
        if name in self._env:
            return self._env[name]
        if default is not None:
            return default
        raise RuntimeError(f"environment variable {name!r} is not set")

    def close(self) -> None:
        """Match ``HandlerContext.close``; fake clients are owned by the test."""
