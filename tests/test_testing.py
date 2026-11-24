from __future__ import annotations

import httpx
import pytest

from ntfy_dispatch import handler
from ntfy_dispatch.registry import get_registry
from ntfy_dispatch.testing import FakeContext, make_message


def test_make_message_defaults():
    message = make_message()

    assert message.id == "test-message"
    assert message.topic == "test-topic"
    assert message.instance == "test"
    assert message.event == "message"
    assert message.priority == 3
    assert message.tags == ()


def test_make_message_overrides():
    message = make_message(
        id="m1",
        topic="alerts",
        message="body",
        title="title",
        priority=5,
        tags=("a", "b"),
        raw={"event": "message"},
    )

    assert (message.id, message.topic, message.message) == ("m1", "alerts", "body")
    assert message.title == "title"
    assert message.priority == 5
    assert message.tags == ("a", "b")
    assert message.raw["event"] == "message"


def test_fake_context_config_and_env():
    ctx = FakeContext(
        name="forward",
        config={"target": "https://example.com"},
        env={"TOKEN": "secret"},
    )

    assert ctx.name == "forward"
    assert ctx.config["target"] == "https://example.com"
    assert ctx.env("TOKEN") == "secret"
    assert ctx.env("MISSING", "fallback") == "fallback"
    assert ctx.shutdown_event.is_set() is False
    with pytest.raises(RuntimeError, match="MISSING"):
        ctx.env("MISSING")


def test_fake_context_requires_http_client():
    ctx = FakeContext()

    with pytest.raises(RuntimeError, match="no http client"):
        _ = ctx.http


def test_fake_context_accepts_http_client():
    transport = httpx.MockTransport(lambda request: httpx.Response(204))
    client = httpx.Client(transport=transport)
    ctx = FakeContext(http=client)

    assert ctx.http.post("https://example.com/hook").status_code == 204
    ctx.close()
    client.close()


def test_registry_can_be_used_in_tests():
    @handler(topics="alerts", name="isolation-check")
    def handle(message, ctx):
        pass

    assert [spec.name for spec in get_registry().handlers()] == ["isolation-check"]


def test_registry_is_reset_between_tests():
    assert get_registry().handlers() == ()
