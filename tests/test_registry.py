from __future__ import annotations

import sys
from uuid import uuid4

import pytest

from ntfy_dispatch.registry import (
    RegistryError,
    get_registry,
    handler,
    load_handler_packages,
    validate_registry,
)


def test_handler_registration():
    @handler(topics=["a", "b"], name="h1", queue_size=10)
    def h1(msg, ctx):
        return None

    specs = get_registry().handlers()
    assert len(specs) == 1
    assert specs[0].name == "h1"
    assert specs[0].topics == frozenset({"a", "b"})
    assert specs[0].queue_size == 10
    assert specs[0].func is h1


def test_handler_default_name():
    @handler(topics="a")
    def default_handler_name(msg, ctx):
        return None

    spec = get_registry().handlers()[0]
    assert spec.name.endswith("default_handler_name")


def test_duplicate_handler_name():
    @handler(topics=["a"], name="dup")
    def first(msg, ctx):
        return None

    with pytest.raises(ValueError, match="duplicate"):

        @handler(topics=["b"], name="dup")
        def second(msg, ctx):
            return None


def test_invalid_topic():
    with pytest.raises(ValueError, match="invalid topic"):
        handler(topics=["bad topic"], name="bad")


def test_empty_topics():
    with pytest.raises(ValueError, match="at least one topic"):
        handler(topics=[], name="empty")


def test_async_handler_is_rejected():
    with pytest.raises(TypeError, match="async"):

        @handler(topics=["a"], name="async")
        async def async_handler(msg, ctx):
            return None


def test_invalid_queue_size():
    with pytest.raises(ValueError, match="queue_size"):
        handler(topics=["a"], name="bad-queue", queue_size=0)


def test_registry_register_accepts_string_topic():
    from ntfy_dispatch.registry import Registry

    registry = Registry()
    registry.register(name="one", topics="solo", func=lambda message, ctx: None)
    assert registry.topics() == frozenset({"solo"})


def test_by_topic_preserves_registration_order():
    @handler(topics=["shared"], name="first")
    def first(msg, ctx):
        return None

    @handler(topics=["shared"], name="second")
    def second(msg, ctx):
        return None

    grouped = get_registry().by_topic()
    assert [spec.name for spec in grouped["shared"]] == ["first", "second"]


def test_validate_registry_rejects_empty():
    with pytest.raises(RegistryError, match="no handlers"):
        validate_registry(get_registry())


def test_validate_registry_rejects_too_many_topics():
    @handler(topics=[f"topic{i}" for i in range(151)], name="many")
    def many(msg, ctx):
        return None

    with pytest.raises(RegistryError, match="at most 150"):
        validate_registry(get_registry())


def test_load_handler_packages(tmp_path, monkeypatch):
    package_name = f"tmp_handlers_{uuid4().hex}"
    package_dir = tmp_path / package_name
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "one.py").write_text(
        "from ntfy_dispatch import handler\n"
        "@handler(topics=['loaded'], name='loaded.handler')\n"
        "def loaded(msg, ctx):\n"
        "    return None\n",
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        load_handler_packages([package_name])
        assert [spec.name for spec in get_registry().handlers()] == ["loaded.handler"]
    finally:
        for module_name in list(sys.modules):
            if module_name == package_name or module_name.startswith(package_name + "."):
                sys.modules.pop(module_name, None)
