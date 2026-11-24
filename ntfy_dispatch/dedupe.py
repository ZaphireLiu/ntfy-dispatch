"""Persistent deduplication of ntfy messages."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path

from .stats import Stats

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL,
    topic TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    seen_at INTEGER NOT NULL,
    UNIQUE(instance, topic, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_seen_seen_at ON seen(seen_at);

CREATE TABLE IF NOT EXISTS instance_state (
    instance TEXT PRIMARY KEY,
    last_time INTEGER,
    last_id TEXT,
    updated_at INTEGER
);
"""


class DedupeStore:
    """SQLite-backed message ID store with a small in-memory LRU."""

    def __init__(
        self,
        path: str | Path,
        *,
        retention_days: int = 7,
        max_rows: int = 200_000,
        memory_cache: int = 10_000,
        stats: Stats | None = None,
    ) -> None:
        self.path = Path(path)
        self._retention_days = retention_days
        self._max_rows = max_rows
        self._memory_cache = max(0, memory_cache)
        self._stats = stats
        self._lock = threading.Lock()
        self._closed = False

        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._conn = sqlite3.connect(
            str(self.path),
            timeout=5.0,
            check_same_thread=False,
        )
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

        self._mem: set[tuple[str, str, str]] = set()
        self._mem_order: deque[tuple[str, str, str]] = deque()
        self._last_time: dict[str, int] = {}
        self._load_memory()
        self._load_state()

    def _load_memory(self) -> None:
        if self._memory_cache <= 0:
            return
        rows = self._conn.execute(
            "SELECT instance, topic, msg_id FROM seen ORDER BY id DESC LIMIT ?",
            (self._memory_cache,),
        ).fetchall()
        for row in reversed(rows):
            key = (str(row[0]), str(row[1]), str(row[2]))
            self._mem.add(key)
            self._mem_order.append(key)

    def _load_state(self) -> None:
        rows = self._conn.execute(
            "SELECT instance, last_time FROM instance_state WHERE last_time IS NOT NULL"
        ).fetchall()
        for instance, last_time in rows:
            self._last_time[str(instance)] = int(last_time)

    def _remember(self, key: tuple[str, str, str]) -> None:
        if self._memory_cache <= 0 or key in self._mem:
            return
        self._mem.add(key)
        self._mem_order.append(key)
        while len(self._mem_order) > self._memory_cache:
            old = self._mem_order.popleft()
            self._mem.discard(old)

    def is_new_and_mark(
        self,
        instance: str,
        topic: str,
        msg_id: str,
        msg_time: int,
        *,
        seen_at: int | None = None,
    ) -> bool:
        """Return True if the message is new, and persist the key."""

        key = (instance, topic, msg_id)
        with self._lock:
            if key in self._mem:
                return False

            seen_timestamp = int(time.time() if seen_at is None else seen_at)
            try:
                with self._conn:
                    cursor = self._conn.execute(
                        """
                        INSERT OR IGNORE INTO seen(instance, topic, msg_id, seen_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (instance, topic, msg_id, seen_timestamp),
                    )
                    is_new = cursor.rowcount == 1
                    if is_new:
                        self._conn.execute(
                            """
                            INSERT INTO instance_state(instance, last_time, last_id, updated_at)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(instance) DO UPDATE SET
                                last_id = CASE
                                    WHEN excluded.last_time >= COALESCE(instance_state.last_time, 0)
                                    THEN excluded.last_id
                                    ELSE instance_state.last_id
                                END,
                                last_time = MAX(
                                    COALESCE(instance_state.last_time, 0),
                                    excluded.last_time
                                ),
                                updated_at = excluded.updated_at
                            """,
                            (instance, int(msg_time), msg_id, int(time.time())),
                        )
                        self._last_time[instance] = max(
                            self._last_time.get(instance, 0), int(msg_time)
                        )
                    self._remember(key)
                    return is_new
            except sqlite3.Error:
                logger.exception("dedupe database error; falling back to in-memory dedupe")
                if self._stats is not None:
                    self._stats.inc("db_errors")
                self._last_time[instance] = max(self._last_time.get(instance, 0), int(msg_time))
                self._remember(key)
                return True

    def get_last_time(self, instance: str) -> int | None:
        with self._lock:
            return self._last_time.get(instance)

    def count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM seen").fetchone()
            return int(row[0]) if row else 0

    def prune(self, *, now: float | None = None) -> tuple[int, int]:
        """Delete old/overflow rows and return (by_age, by_rows)."""

        now_ts = int(time.time() if now is None else now)
        cutoff = now_ts - self._retention_days * 86400
        with self._lock:
            with self._conn:
                by_age = self._conn.execute(
                    "DELETE FROM seen WHERE seen_at < ?", (cutoff,)
                ).rowcount
                by_rows = 0
                if self._max_rows > 0:
                    by_rows = self._conn.execute(
                        """
                        DELETE FROM seen
                        WHERE id <= (
                            SELECT id FROM seen ORDER BY id DESC LIMIT 1 OFFSET ?
                        )
                        """,
                        (self._max_rows,),
                    ).rowcount
            if by_age or by_rows:
                # The database is authoritative after a prune; keeping pruned
                # keys in the LRU would suppress legitimate replays.
                self._mem.clear()
                self._mem_order.clear()
        if by_age or by_rows:
            logger.info("dedupe prune removed rows: age=%d overflow=%d", by_age, by_rows)
        return by_age, by_rows

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.close()

    def __enter__(self) -> DedupeStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


class DedupePruner(threading.Thread):
    """Periodically remove old or excess dedupe rows."""

    def __init__(
        self,
        store: DedupeStore,
        interval: float,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="dedupe-prune", daemon=True)
        self._store = store
        self._interval = interval
        self._stop_event = stop_event

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._store.prune()
            except Exception:
                logger.exception("dedupe prune failed")
            if self._stop_event.wait(self._interval):
                break
