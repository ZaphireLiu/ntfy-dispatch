"""Handler registration and discovery."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import re
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .models import Message

TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_TOPICS_PER_CONNECTION = 150

HandlerFunc = Callable[[Message, Any], None]


class RegistryError(RuntimeError):
    """Raised when handlers cannot be loaded or validated."""


def validate_topic(topic: str) -> str:
    if not isinstance(topic, str) or not TOPIC_RE.fullmatch(topic):
        raise ValueError(f"invalid topic {topic!r}: topics must match {TOPIC_RE.pattern}")
    return topic


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    name: str
    topics: frozenset[str]
    func: HandlerFunc
    queue_size: int | None
    registration_index: int


class Registry:
    """A thread-safe collection of handler specs."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handlers: dict[str, HandlerSpec] = {}
        self._next_index = 0

    def register(
        self,
        *,
        name: str,
        topics: Iterable[str],
        func: HandlerFunc,
        queue_size: int | None = None,
    ) -> HandlerSpec:
        if not isinstance(name, str) or not name:
            raise ValueError("handler name must be a non-empty string")
        if not callable(func):
            raise TypeError("handler must be callable")
        if queue_size is not None and (
            isinstance(queue_size, bool) or not isinstance(queue_size, int) or queue_size <= 0
        ):
            raise ValueError("queue_size must be a positive integer")

        topic_values = (topics,) if isinstance(topics, str) else topics
        topic_set = frozenset(validate_topic(topic) for topic in topic_values)
        if not topic_set:
            raise ValueError("handler must declare at least one topic")

        with self._lock:
            if name in self._handlers:
                raise ValueError(f"duplicate handler name: {name!r}")
            spec = HandlerSpec(
                name=name,
                topics=topic_set,
                func=func,
                queue_size=queue_size,
                registration_index=self._next_index,
            )
            self._next_index += 1
            self._handlers[name] = spec
            return spec

    def handlers(self) -> tuple[HandlerSpec, ...]:
        with self._lock:
            return tuple(self._handlers.values())

    def topics(self) -> frozenset[str]:
        with self._lock:
            return frozenset(topic for spec in self._handlers.values() for topic in spec.topics)

    def by_topic(self) -> dict[str, tuple[HandlerSpec, ...]]:
        with self._lock:
            grouped: dict[str, list[HandlerSpec]] = {}
            for spec in self._handlers.values():
                for topic in spec.topics:
                    grouped.setdefault(topic, []).append(spec)
            return {
                topic: tuple(sorted(specs, key=lambda item: item.registration_index))
                for topic, specs in grouped.items()
            }

    def clear(self) -> None:
        with self._lock:
            self._handlers.clear()
            self._next_index = 0


_registry = Registry()


def get_registry() -> Registry:
    return _registry


def reset_registry() -> None:
    _registry.clear()


def handler(
    *,
    topics: Iterable[str] | str,
    name: str | None = None,
    queue_size: int | None = None,
) -> Callable[[HandlerFunc], HandlerFunc]:
    """Register a synchronous handler for one or more ntfy topics."""

    if isinstance(topics, str):
        topic_values = (topics,)
    else:
        topic_values = tuple(topics)
    topic_set = frozenset(validate_topic(topic) for topic in topic_values)
    if not topic_set:
        raise ValueError("handler must declare at least one topic")
    if queue_size is not None and (
        isinstance(queue_size, bool) or not isinstance(queue_size, int) or queue_size <= 0
    ):
        raise ValueError("queue_size must be a positive integer")

    def decorator(func: HandlerFunc) -> HandlerFunc:
        if inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func):
            raise TypeError("async handlers are not supported; use a synchronous function")
        handler_name = name or f"{func.__module__}.{func.__qualname__}"
        _registry.register(
            name=handler_name,
            topics=topic_set,
            func=func,
            queue_size=queue_size,
        )
        return func

    return decorator


def _import_package(package_name: str) -> None:
    try:
        package = importlib.import_module(package_name)
    except Exception as exc:
        raise RegistryError(f"failed to import handler package {package_name!r}: {exc}") from exc

    package_path = getattr(package, "__path__", None)
    if package_path is None:
        return

    try:
        modules = sorted(
            pkgutil.iter_modules(package_path, prefix=f"{package_name}."),
            key=lambda module: module.name,
        )
    except Exception as exc:
        raise RegistryError(f"failed to scan handler package {package_name!r}: {exc}") from exc

    for module_info in modules:
        short_name = module_info.name.rsplit(".", 1)[-1]
        if short_name.startswith("_"):
            continue
        if module_info.ispkg:
            _import_package(module_info.name)
            continue
        try:
            importlib.import_module(module_info.name)
        except Exception as exc:
            raise RegistryError(
                f"failed to import handler module {module_info.name!r}: {exc}"
            ) from exc


def load_handler_packages(packages: Iterable[str]) -> None:
    """Import configured packages so decorator registration runs."""

    importlib.invalidate_caches()
    for package_name in packages:
        _import_package(package_name)


def validate_registry(registry: Registry) -> frozenset[str]:
    specs = registry.handlers()
    if not specs:
        raise RegistryError("no handlers registered")
    topics = registry.topics()
    if len(topics) > MAX_TOPICS_PER_CONNECTION:
        raise RegistryError(
            f"{len(topics)} topics registered, but a single ntfy subscription "
            f"supports at most {MAX_TOPICS_PER_CONNECTION}"
        )
    return topics
