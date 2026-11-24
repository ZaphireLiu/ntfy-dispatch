from __future__ import annotations

import threading

import pytest

from ntfy_dispatch.config import AuthConfig, NtfyConfig
from ntfy_dispatch.dedupe import DedupeStore
from ntfy_dispatch.models import Message
from ntfy_dispatch.registry import HandlerSpec
from ntfy_dispatch.stats import Stats
from ntfy_dispatch.stream import NtfyReader


class FakeWorker:
    def __init__(self) -> None:
        self.messages: list[Message] = []

    def submit(self, message: Message) -> bool:
        self.messages.append(message)
        return True


def make_reader(
    tmp_path,
    *,
    resume: bool = True,
    overlap: int = 120,
    base_url: str = "https://ntfy.example.com",
    topics: frozenset[str] | None = None,
):
    topics = topics or frozenset({"t"})
    spec = HandlerSpec(
        name="worker",
        topics=topics,
        func=lambda message, ctx: None,
        queue_size=None,
        registration_index=0,
    )
    handlers_by_topic = {topic: (spec,) for topic in topics}
    worker = FakeWorker()
    stats = Stats()
    dedupe = DedupeStore(tmp_path / "dedupe.sqlite3", memory_cache=100, stats=stats)
    reader = NtfyReader(
        config=NtfyConfig(
            name="i",
            base_url=base_url,
            auth=AuthConfig(type="none"),
            resume=resume,
            resume_overlap_seconds=overlap,
        ),
        topics=topics,
        handlers_by_topic=handlers_by_topic,
        workers={"worker": worker},
        dedupe=dedupe,
        stats=stats,
        stop_event=threading.Event(),
    )
    return reader, worker, dedupe, stats


def message_event(msg_id: str = "abc", event_time: int = 1000) -> dict[str, object]:
    return {
        "event": "message",
        "id": msg_id,
        "time": event_time,
        "topic": "t",
        "message": "hello",
        "title": "Title",
        "priority": 4,
        "tags": ["warning", "disk"],
        "click": "https://example.com",
        "actions": [{"action": "view", "label": "Open", "url": "https://example.com"}],
        "attachment": {"name": "file.txt", "url": "https://example.com/file.txt"},
    }


def test_message_from_event():
    message = Message.from_event("i", message_event())
    assert message.instance == "i"
    assert message.topic == "t"
    assert message.id == "abc"
    assert message.time == 1000
    assert message.message == "hello"
    assert message.title == "Title"
    assert message.priority == 4
    assert message.tags == ("warning", "disk")
    assert message.click == "https://example.com"
    assert message.actions is not None
    assert message.actions[0]["action"] == "view"
    assert message.attachment is not None
    assert message.attachment["name"] == "file.txt"
    assert message.raw["id"] == "abc"


def test_message_payload_is_read_only():
    message = Message.from_event("i", message_event())
    with pytest.raises(TypeError):
        message.raw["id"] = "other"  # type: ignore[index]


def test_message_requires_single_topic():
    event = message_event()
    event["topic"] = "a,b"
    with pytest.raises(ValueError, match="single topic"):
        Message.from_event("i", event)


def test_message_requires_id_and_time():
    event = message_event()
    del event["id"]
    with pytest.raises(ValueError, match="'id'"):
        Message.from_event("i", event)

    event = message_event()
    del event["time"]
    with pytest.raises(ValueError, match="'time'"):
        Message.from_event("i", event)


def test_build_url_uses_sorted_topics_and_base_path(tmp_path):
    reader, _, dedupe, _ = make_reader(
        tmp_path,
        base_url="https://ntfy.example.com/base/",
        topics=frozenset({"b", "a"}),
    )
    try:
        assert reader.build_url() == "https://ntfy.example.com/base/a,b/json"
    finally:
        dedupe.close()


def test_build_params_uses_overlap_window(tmp_path):
    reader, _, dedupe, _ = make_reader(tmp_path, overlap=120)
    try:
        assert reader.build_params() == {}
        reader.handle_message(message_event(event_time=1000))
        assert reader.build_params() == {"since": "880"}
    finally:
        dedupe.close()


def test_build_params_respects_resume_false(tmp_path):
    reader, _, dedupe, _ = make_reader(tmp_path, resume=False)
    try:
        reader.handle_message(message_event(event_time=1000))
        assert reader.build_params() == {}
    finally:
        dedupe.close()


def test_duplicate_message_is_dispatched_once(tmp_path):
    reader, worker, dedupe, stats = make_reader(tmp_path)
    try:
        event = message_event()
        reader.handle_message(event)
        reader.handle_message(event)
        assert [message.id for message in worker.messages] == ["abc"]
        snapshot = stats.snapshot()
        assert snapshot["counters"]["received"] == 2
        assert snapshot["counters"]["deduped"] == 1
    finally:
        dedupe.close()


def test_invalid_event_does_not_dispatch(tmp_path):
    reader, worker, dedupe, stats = make_reader(tmp_path)
    try:
        reader.handle_message({"event": "message", "topic": "t"})
        assert worker.messages == []
        assert stats.snapshot()["counters"]["parse_errors"] == 1
    finally:
        dedupe.close()
