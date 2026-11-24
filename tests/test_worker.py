from __future__ import annotations

import threading
import time

from ntfy_dispatch.models import Message
from ntfy_dispatch.registry import HandlerSpec
from ntfy_dispatch.stats import Stats
from ntfy_dispatch.worker import HandlerWorker


def make_message(msg_id: str) -> Message:
    return Message(
        instance="i",
        topic="t",
        id=msg_id,
        time=1,
        message="body",
    )


def make_spec(func, *, name: str = "worker-test", queue_size: int | None = 10) -> HandlerSpec:
    return HandlerSpec(
        name=name,
        topics=frozenset({"t"}),
        func=func,
        queue_size=queue_size,
        registration_index=0,
    )


def make_worker(func, *, queue_size: int | None = 10, config=None):
    stats = Stats()
    shutdown_event = threading.Event()
    stop_event = threading.Event()
    worker = HandlerWorker(
        make_spec(func, queue_size=queue_size),
        config or {},
        instance="i",
        default_queue_size=10,
        http_timeout=1.0,
        shutdown_event=shutdown_event,
        stop_event=stop_event,
        stats=stats,
    )
    return worker, stats, shutdown_event, stop_event


def test_fifo_and_stats():
    processed: list[str] = []
    done = threading.Event()

    def func(message, ctx):
        processed.append(message.id)
        if len(processed) == 3:
            done.set()

    worker, stats, _, _ = make_worker(func)
    worker.start()
    for index in range(3):
        assert worker.submit(make_message(f"m{index}")) is True
    assert done.wait(2.0)
    worker.request_stop()
    assert worker.join(2.0) is True

    assert processed == ["m0", "m1", "m2"]
    snapshot = stats.snapshot()
    assert snapshot["counters"]["processed"] == 3
    assert snapshot["counters"]["enqueued"] == 3
    assert snapshot["handlers"]["worker-test"]["processed"] == 3


def test_exception_is_isolated():
    processed: list[str] = []
    done = threading.Event()

    def func(message, ctx):
        if message.id == "bad":
            raise RuntimeError("boom")
        processed.append(message.id)
        done.set()

    worker, stats, _, _ = make_worker(func)
    worker.start()
    worker.submit(make_message("bad"))
    worker.submit(make_message("good"))
    assert done.wait(2.0)
    worker.request_stop()
    assert worker.join(2.0) is True

    assert processed == ["good"]
    snapshot = stats.snapshot()
    assert snapshot["counters"]["failed"] == 1
    assert snapshot["counters"]["processed"] == 1
    assert snapshot["handlers"]["worker-test"]["failed"] == 1


def test_queue_full_drops_new_message():
    started = threading.Event()
    release = threading.Event()

    def func(message, ctx):
        started.set()
        release.wait(2.0)

    worker, stats, _, _ = make_worker(func, queue_size=2)
    worker.start()
    assert worker.submit(make_message("m0")) is True
    assert started.wait(2.0)
    assert worker.submit(make_message("m1")) is True
    assert worker.submit(make_message("m2")) is True
    assert worker.submit(make_message("m3")) is False

    release.set()
    worker.request_stop()
    assert worker.join(2.0) is True

    snapshot = stats.snapshot()
    assert snapshot["counters"]["dropped"] == 1
    assert snapshot["handlers"]["worker-test"]["dropped"] == 1


def test_stop_drains_queue():
    processed: list[str] = []

    def func(message, ctx):
        processed.append(message.id)

    worker, _, _, _ = make_worker(func)
    worker.start()
    for index in range(3):
        worker.submit(make_message(f"m{index}"))

    worker.request_stop(time.monotonic() + 2.0)
    assert worker.join(2.0) is True
    assert processed == ["m0", "m1", "m2"]


def test_handler_context(monkeypatch):
    monkeypatch.setenv("TEST_HANDLER_VALUE", "from-env")
    seen: dict[str, object] = {}
    done = threading.Event()

    def func(message, ctx):
        seen["config"] = ctx.config
        seen["instance"] = ctx.instance
        seen["logger"] = ctx.logger.name
        seen["http"] = ctx.http
        seen["env"] = ctx.env("TEST_HANDLER_VALUE")
        done.set()

    worker, _, _, _ = make_worker(func, config={"foo": "bar"})
    worker.start()
    worker.submit(make_message("m0"))
    assert done.wait(2.0)
    worker.request_stop()
    assert worker.join(2.0) is True

    assert seen["config"] == {"foo": "bar"}
    assert seen["instance"] == "i"
    assert str(seen["logger"]).endswith("worker-test")
    assert seen["http"] is not None
    assert seen["env"] == "from-env"
