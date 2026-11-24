from __future__ import annotations

import json
import logging

import httpx
import pytest

from ntfy_dispatch.config import AuthConfig, NtfyConfig
from ntfy_dispatch.publisher import (
    Publisher,
    PublishError,
    SelfPublishError,
    check_publish_target,
)

OWN_TOPICS = frozenset({"alerts"})
LISTENED_TOPICS = frozenset({"alerts", "other"})


class Recorder:
    def __init__(self, *, status: int = 200, body: dict | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._status = status
        self._body = body if body is not None else {"id": "p1", "topic": "other", "time": 123}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self._status, json=self._body, request=request)


def make_publisher(recorder: Recorder, **config_kwargs) -> Publisher:
    config = NtfyConfig(base_url="https://ntfy.example.com/base", **config_kwargs)
    return Publisher(
        config,
        timeout=1.0,
        transport=httpx.MockTransport(recorder),
    )


def test_publish_posts_json_to_instance_root():
    recorder = Recorder()
    publisher = make_publisher(recorder)

    result = publisher.publish(
        handler="forward",
        topic="other",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
        message="hello",
        title="Title",
        priority=4,
        tags=("warning", "cd"),
    )

    assert len(recorder.requests) == 1
    request = recorder.requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://ntfy.example.com/base/"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"].startswith("ntfy-dispatch/")

    body = json.loads(request.content)
    assert body["topic"] == "other"
    assert body["message"] == "hello"
    assert body["title"] == "Title"
    assert body["priority"] == 4
    assert body["tags"] == ["warning", "cd"]

    assert result.id == "p1"
    assert result.topic == "other"
    assert result.time == 123
    assert result.raw["id"] == "p1"


def test_publish_supports_all_documented_fields():
    recorder = Recorder()
    publisher = make_publisher(recorder)

    publisher.publish(
        handler="forward",
        topic="other",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
        message="body",
        actions=[{"action": "view", "label": "Open", "url": "https://example.com"}],
        click="https://example.com",
        attach="https://example.com/file.jpg",
        filename="file.jpg",
        markdown=True,
        icon="https://example.com/icon.png",
        delay="30min",
        timezone="Asia/Tokyo",
        email="yes",
        call="yes",
        sequence_id="seq-1",
    )

    body = json.loads(recorder.requests[0].content)
    assert body["actions"] == [{"action": "view", "label": "Open", "url": "https://example.com"}]
    assert body["click"] == "https://example.com"
    assert body["attach"] == "https://example.com/file.jpg"
    assert body["filename"] == "file.jpg"
    assert body["markdown"] is True
    assert body["icon"] == "https://example.com/icon.png"
    assert body["delay"] == "30min"
    assert body["timezone"] == "Asia/Tokyo"
    assert body["email"] == "yes"
    assert body["call"] == "yes"
    assert body["sequence_id"] == "seq-1"


def test_publish_uses_token_auth(monkeypatch):
    monkeypatch.setenv("NTFY_TEST_TOKEN", "secret-token")
    recorder = Recorder()
    publisher = make_publisher(
        recorder,
        auth=AuthConfig(type="token", token_env="NTFY_TEST_TOKEN"),
    )

    publisher.publish(
        handler="forward",
        topic="other",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
    )

    assert recorder.requests[0].headers["authorization"] == "Bearer secret-token"


def test_publish_rejects_own_topic_without_request():
    recorder = Recorder()
    publisher = make_publisher(recorder)

    with pytest.raises(SelfPublishError, match="own topic"):
        publisher.publish(
            handler="forward",
            topic="alerts",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
        )

    assert recorder.requests == []


def test_publish_warns_once_for_other_listened_topic(caplog):
    caplog.set_level(logging.WARNING, logger="ntfy_dispatch.publish")
    recorder = Recorder()
    publisher = make_publisher(recorder)

    for _ in range(2):
        publisher.publish(
            handler="forward",
            topic="other",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
        )

    assert len(recorder.requests) == 2
    records = [record for record in caplog.records if record.name == "ntfy_dispatch.publish"]
    assert len(records) == 1
    assert "other" in records[0].getMessage()
    assert "forward" in records[0].getMessage()


def test_publish_to_unrelated_topic_is_quiet(caplog):
    caplog.set_level(logging.WARNING, logger="ntfy_dispatch.publish")
    recorder = Recorder()
    publisher = make_publisher(recorder)

    publisher.publish(
        handler="forward",
        topic="third",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
    )

    assert len(recorder.requests) == 1
    assert [record for record in caplog.records if record.name == "ntfy_dispatch.publish"] == []


def test_publish_validates_fields():
    publisher = make_publisher(Recorder())
    base = {
        "handler": "forward",
        "own_topics": OWN_TOPICS,
        "listened_topics": LISTENED_TOPICS,
    }

    for priority in (0, 6, True, "3"):
        with pytest.raises(PublishError, match="priority"):
            publisher.publish(**base, topic="third", priority=priority)

    with pytest.raises(PublishError, match="tags"):
        publisher.publish(**base, topic="third", tags=(1,))
    with pytest.raises(PublishError, match="markdown"):
        publisher.publish(**base, topic="third", markdown="yes")
    with pytest.raises(PublishError, match="title"):
        publisher.publish(**base, topic="third", title=1)
    with pytest.raises(PublishError, match="topic"):
        publisher.publish(**base, topic="bad topic!")


def test_publish_rejects_extra_reserved_fields():
    publisher = make_publisher(Recorder())

    with pytest.raises(PublishError, match="override"):
        publisher.publish(
            handler="forward",
            topic="third",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
            extra={"topic": "other"},
        )


def test_publish_extra_is_merged():
    recorder = Recorder()
    publisher = make_publisher(recorder)

    publisher.publish(
        handler="forward",
        topic="third",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
        extra={"cache": "no"},
    )

    assert json.loads(recorder.requests[0].content)["cache"] == "no"


def test_publish_reports_http_error():
    recorder = Recorder(status=403, body={"error": "forbidden"})
    publisher = make_publisher(recorder)

    with pytest.raises(PublishError, match="HTTP 403"):
        publisher.publish(
            handler="forward",
            topic="third",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
        )


def test_publish_reports_non_json_response():
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json", request=request)

    publisher = Publisher(
        NtfyConfig(base_url="https://ntfy.example.com"),
        timeout=1.0,
        transport=httpx.MockTransport(respond),
    )

    with pytest.raises(PublishError, match="non-JSON"):
        publisher.publish(
            handler="forward",
            topic="third",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
        )


def test_check_publish_target():
    assert check_publish_target(
        handler="forward",
        topic="other",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
    )
    assert not check_publish_target(
        handler="forward",
        topic="third",
        own_topics=OWN_TOPICS,
        listened_topics=LISTENED_TOPICS,
    )
    with pytest.raises(SelfPublishError):
        check_publish_target(
            handler="forward",
            topic="alerts",
            own_topics=OWN_TOPICS,
            listened_topics=LISTENED_TOPICS,
        )
