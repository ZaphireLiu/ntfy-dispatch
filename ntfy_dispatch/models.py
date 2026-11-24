"""Message model used by handlers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


def _required_str(event: Mapping[str, Any], key: str) -> str:
    value = event.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing or invalid {key!r}")
    return value


@dataclass(frozen=True, slots=True)
class Message:
    """A read-only view of a ntfy ``message`` event."""

    instance: str
    topic: str
    id: str
    time: int
    event: str = "message"
    message: str = ""
    title: str | None = None
    priority: int = 3
    tags: tuple[str, ...] = ()
    click: str | None = None
    actions: tuple[Mapping[str, Any], ...] | None = None
    attachment: Mapping[str, Any] | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_event(cls, instance: str, event: Mapping[str, Any]) -> Message:
        """Build a :class:`Message` from a raw ntfy JSON event."""

        event_type = event.get("event", "message")
        if event_type != "message":
            raise ValueError(f"not a message event: {event_type!r}")

        msg_id = _required_str(event, "id")
        topic = _required_str(event, "topic")
        if "," in topic:
            raise ValueError("message event topic must be a single topic")

        time_value = event.get("time")
        if isinstance(time_value, bool) or not isinstance(time_value, (int, float)):
            raise ValueError("missing or invalid 'time'")
        event_time = int(time_value)

        body = event.get("message", "")
        if body is None:
            body = ""
        elif not isinstance(body, str):
            body = str(body)

        title_value = event.get("title")
        title = title_value if isinstance(title_value, str) else None

        priority_value = event.get("priority", 3)
        if (
            isinstance(priority_value, bool)
            or not isinstance(priority_value, int)
            or not 1 <= priority_value <= 5
        ):
            priority = 3
        else:
            priority = priority_value

        tags_value = event.get("tags", ())
        tags: tuple[str, ...]
        if isinstance(tags_value, str):
            tags = tuple(part for part in tags_value.split(",") if part)
        elif isinstance(tags_value, (list, tuple)):
            tags = tuple(str(part) for part in tags_value)
        else:
            tags = ()

        click_value = event.get("click")
        click = click_value if isinstance(click_value, str) else None

        actions_value = event.get("actions")
        actions: tuple[Mapping[str, Any], ...] | None = None
        if isinstance(actions_value, list):
            actions = tuple(
                MappingProxyType(dict(item)) for item in actions_value if isinstance(item, dict)
            )
            if not actions:
                actions = None

        attachment_value = event.get("attachment")
        attachment = (
            MappingProxyType(dict(attachment_value)) if isinstance(attachment_value, dict) else None
        )

        return cls(
            instance=instance,
            topic=topic,
            id=msg_id,
            time=event_time,
            event="message",
            message=body,
            title=title,
            priority=priority,
            tags=tags,
            click=click,
            actions=actions,
            attachment=attachment,
            raw=MappingProxyType(dict(event)),
        )
