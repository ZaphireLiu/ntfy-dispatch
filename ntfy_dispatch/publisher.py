"""Publish messages to the ntfy instance this process is subscribed to."""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import httpx

from . import ntfy_http
from .config import NtfyConfig
from .registry import validate_topic

logger = logging.getLogger("ntfy_dispatch.publish")

RESERVED_FIELDS = frozenset(
    {
        "topic",
        "message",
        "title",
        "priority",
        "tags",
        "actions",
        "click",
        "attach",
        "filename",
        "markdown",
        "icon",
        "delay",
        "timezone",
        "email",
        "call",
        "sequence_id",
    }
)


class PublishError(RuntimeError):
    """Raised when publishing to ntfy fails or is refused."""


class SelfPublishError(PublishError):
    """Raised when a handler tries to publish to one of its own topics."""


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The message object ntfy returns after a successful publish."""

    id: str
    topic: str
    time: int
    raw: Mapping[str, Any] = MappingProxyType({})


def check_publish_target(
    *,
    handler: str,
    topic: str,
    own_topics: frozenset[str],
    listened_topics: frozenset[str],
) -> bool:
    """Validate a publish target.

    Returns ``True`` when the target is consumed by this dispatch instance but
    not by the calling handler (a warning is due).  Raises
    :class:`SelfPublishError` when the handler would publish to a topic it
    consumes itself.
    """

    try:
        validate_topic(topic)
    except ValueError as exc:
        raise PublishError(str(exc)) from exc

    if topic in own_topics:
        raise SelfPublishError(f"handler {handler!r} cannot publish to its own topic {topic!r}")
    return topic in listened_topics


class Publisher:
    """Thread-safe publisher shared by all handlers in the process."""

    def __init__(
        self,
        config: NtfyConfig,
        *,
        timeout: float,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._url = f"{config.base_url.rstrip('/')}/"
        self._client = ntfy_http.build_client(config, timeout=timeout, transport=transport)
        self._lock = threading.Lock()
        self._warned: set[tuple[str, str]] = set()

    def close(self) -> None:
        self._client.close()

    def publish(
        self,
        *,
        handler: str,
        topic: str,
        own_topics: frozenset[str],
        listened_topics: frozenset[str],
        message: str = "",
        title: str | None = None,
        priority: int = 3,
        tags: str | Iterable[str] = (),
        actions: Sequence[Mapping[str, Any]] | None = None,
        click: str | None = None,
        attach: str | None = None,
        filename: str | None = None,
        markdown: bool | None = None,
        icon: str | None = None,
        delay: str | None = None,
        timezone: str | None = None,
        email: str | None = None,
        call: str | None = None,
        sequence_id: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> PublishResult:
        """Publish one message using ntfy's JSON publish API.

        The request goes to the instance root URL (``POST {base_url}/``) with
        ``topic`` in the JSON body, as required by ntfy.
        """

        if not isinstance(message, str):
            raise PublishError("message must be a string")
        bool_priority = isinstance(priority, bool)
        if bool_priority or not isinstance(priority, int) or not 1 <= priority <= 5:
            raise PublishError("priority must be an integer between 1 and 5")
        if markdown is not None and not isinstance(markdown, bool):
            raise PublishError("markdown must be a boolean")
        for name, value in (
            ("title", title),
            ("click", click),
            ("attach", attach),
            ("filename", filename),
            ("icon", icon),
            ("delay", delay),
            ("timezone", timezone),
            ("email", email),
            ("call", call),
            ("sequence_id", sequence_id),
        ):
            if value is not None and not isinstance(value, str):
                raise PublishError(f"{name} must be a string")

        warn = check_publish_target(
            handler=handler,
            topic=topic,
            own_topics=own_topics,
            listened_topics=listened_topics,
        )
        if warn:
            self._warn_once(handler, topic)

        payload: dict[str, Any] = {}
        if extra:
            for key in extra:
                if not isinstance(key, str) or not key:
                    raise PublishError("extra keys must be non-empty strings")
                if key in RESERVED_FIELDS:
                    raise PublishError(f"extra must not override the {key!r} field")
            payload.update(extra)

        payload["topic"] = topic
        payload["message"] = message
        payload["priority"] = priority
        for name, value in (
            ("title", title),
            ("click", click),
            ("attach", attach),
            ("filename", filename),
            ("markdown", markdown),
            ("icon", icon),
            ("delay", delay),
            ("timezone", timezone),
            ("email", email),
            ("call", call),
            ("sequence_id", sequence_id),
        ):
            if value is not None:
                payload[name] = value
        if tags:
            payload["tags"] = _normalize_tags(tags)
        if actions is not None:
            payload["actions"] = _normalize_actions(actions)

        try:
            response = self._client.post(self._url, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise PublishError(
                f"ntfy rejected publish to {topic!r}: "
                f"HTTP {exc.response.status_code} {exc.response.text[:200]}"
            ) from exc
        except httpx.HTTPError as exc:
            raise PublishError(f"publish to {topic!r} failed: {exc}") from exc
        except TypeError as exc:
            raise PublishError(f"publish payload is not JSON serializable: {exc}") from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise PublishError("ntfy returned a non-JSON response") from exc
        if not isinstance(data, dict):
            raise PublishError("ntfy returned an unexpected response")

        time_value = data.get("time")
        published_at = (
            time_value if isinstance(time_value, int) and not isinstance(time_value, bool) else 0
        )
        return PublishResult(
            id=str(data.get("id", "")),
            topic=str(data.get("topic", topic)),
            time=published_at,
            raw=MappingProxyType(dict(data)),
        )

    def _warn_once(self, handler: str, topic: str) -> None:
        key = (handler, topic)
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        logger.warning(
            "handler %r published to topic %r which this dispatch instance also consumes; "
            "a feedback loop is possible",
            handler,
            topic,
        )


def _normalize_tags(tags: str | Iterable[str]) -> list[str]:
    if isinstance(tags, str):
        return [tags]
    values = list(tags)
    if not all(isinstance(tag, str) for tag in values):
        raise PublishError("tags must be strings")
    return values


def _normalize_actions(
    actions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    values = list(actions)
    for action in values:
        if not isinstance(action, Mapping):
            raise PublishError("actions must be a sequence of mappings")
    return [dict(action) for action in values]
