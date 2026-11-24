from __future__ import annotations

import threading
import time

from ntfy_dispatch.dedupe import DedupePruner, DedupeStore


def make_store(tmp_path, **kwargs) -> DedupeStore:
    return DedupeStore(
        tmp_path / "dedupe.sqlite3",
        retention_days=kwargs.pop("retention_days", 7),
        max_rows=kwargs.pop("max_rows", 200_000),
        memory_cache=kwargs.pop("memory_cache", 10_000),
        **kwargs,
    )


def test_new_and_duplicate(tmp_path):
    store = make_store(tmp_path)
    try:
        assert store.is_new_and_mark("i", "t", "id1", 1000) is True
        assert store.is_new_and_mark("i", "t", "id1", 1000) is False
        assert store.count() == 1
    finally:
        store.close()


def test_topic_is_part_of_key(tmp_path):
    store = make_store(tmp_path)
    try:
        assert store.is_new_and_mark("i", "a", "id1", 1000) is True
        assert store.is_new_and_mark("i", "b", "id1", 1000) is True
        assert store.count() == 2
    finally:
        store.close()


def test_last_time_tracks_new_messages(tmp_path):
    store = make_store(tmp_path)
    try:
        assert store.get_last_time("i") is None
        store.is_new_and_mark("i", "t", "id1", 1000)
        assert store.get_last_time("i") == 1000
        store.is_new_and_mark("i", "t", "id2", 900)
        assert store.get_last_time("i") == 1000
        store.is_new_and_mark("i", "t", "id3", 1200)
        assert store.get_last_time("i") == 1200
    finally:
        store.close()


def test_restart_keeps_dedupe(tmp_path):
    store = make_store(tmp_path)
    try:
        assert store.is_new_and_mark("i", "t", "id1", 1000) is True
    finally:
        store.close()

    reopened = make_store(tmp_path)
    try:
        assert reopened.is_new_and_mark("i", "t", "id1", 1000) is False
        assert reopened.get_last_time("i") == 1000
    finally:
        reopened.close()


def test_prune_by_age(tmp_path):
    store = make_store(tmp_path, retention_days=1, max_rows=0, memory_cache=0)
    try:
        now = 10_000_000
        store.is_new_and_mark("i", "t", "old", 1, seen_at=now - 2 * 86400)
        store.is_new_and_mark("i", "t", "new", 2, seen_at=now - 60)
        by_age, by_rows = store.prune(now=now)
        assert by_age == 1
        assert by_rows == 0
        assert store.count() == 1
    finally:
        store.close()


def test_prune_by_rows(tmp_path):
    store = make_store(tmp_path, retention_days=7, max_rows=5, memory_cache=0)
    try:
        for index in range(10):
            store.is_new_and_mark("i", "t", f"id{index}", index, seen_at=1000 + index)
        by_age, by_rows = store.prune(now=1000)
        assert by_age == 0
        assert by_rows == 5
        assert store.count() == 5
        assert store.is_new_and_mark("i", "t", "id9", 9, seen_at=1009) is False
        assert store.is_new_and_mark("i", "t", "id0", 0, seen_at=1000) is True
    finally:
        store.close()


def test_memory_cache_eviction_falls_back_to_database(tmp_path):
    store = make_store(tmp_path, memory_cache=1)
    try:
        assert store.is_new_and_mark("i", "t", "a", 1) is True
        assert store.is_new_and_mark("i", "t", "b", 2) is True
        assert store.is_new_and_mark("i", "t", "a", 1) is False
    finally:
        store.close()


def test_prune_clears_memory_cache(tmp_path):
    store = make_store(tmp_path, retention_days=1, max_rows=0, memory_cache=10)
    try:
        assert store.is_new_and_mark("i", "t", "old", 1, seen_at=0) is True
        assert store.prune(now=10_000_000)[0] == 1
        assert store.is_new_and_mark("i", "t", "old", 1, seen_at=0) is True
    finally:
        store.close()


def test_pruner_removes_old_rows(tmp_path):
    store = make_store(tmp_path, retention_days=1, max_rows=0, memory_cache=0)
    stop_event = threading.Event()
    pruner: DedupePruner | None = None
    try:
        store.is_new_and_mark("i", "t", "old", 1, seen_at=0)
        pruner = DedupePruner(store, interval=0.01, stop_event=stop_event)
        pruner.start()
        deadline = time.monotonic() + 2.0
        while store.count() > 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert store.count() == 0
    finally:
        stop_event.set()
        if pruner is not None:
            pruner.join(timeout=1.0)
        store.close()
