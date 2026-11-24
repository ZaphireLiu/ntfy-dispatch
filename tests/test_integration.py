from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ntfy_dispatch.app import App
from ntfy_dispatch.config import (
    AppConfig,
    AuthConfig,
    CoreConfig,
    DedupeConfig,
    NtfyConfig,
)
from ntfy_dispatch.registry import Registry


class FakeNtfyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), FakeNtfyHandler)
        self._paths: list[str] = []
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def record(self, path: str) -> None:
        with self._lock:
            self._paths.append(path)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._paths)


class FakeNtfyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_GET(self) -> None:
        self.server.record(self.path)
        index = len(self.server.snapshot()) - 1
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.end_headers()
        events = [
            {"event": "open", "id": f"open{index}", "time": 999, "topic": "demo"},
            {
                "event": "message",
                "id": f"m{index}",
                "time": 1000 + index,
                "topic": "demo",
                "message": f"hello {index}",
            },
        ]
        try:
            for event in events:
                self.wfile.write((json.dumps(event) + "\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format: str, *args) -> None:
        pass


def test_app_receives_messages_and_resumes_after_disconnect(tmp_path: Path):
    server = FakeNtfyServer()
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    registry = Registry()
    seen: list[str] = []
    done = threading.Event()

    def handle(message, ctx) -> None:
        seen.append(message.id)
        if len(seen) >= 2:
            done.set()

    registry.register(
        name="test",
        topics=frozenset({"demo"}),
        func=handle,
        queue_size=10,
    )

    config = AppConfig(
        core=CoreConfig(
            state_dir=tmp_path / "state",
            queue_size=10,
            stats_interval=60.0,
            shutdown_drain_seconds=2.0,
            http_timeout=1.0,
        ),
        ntfy=NtfyConfig(
            name="test",
            base_url=server.base_url,
            auth=AuthConfig(type="none"),
            read_timeout=2.0,
            connect_timeout=2.0,
            min_backoff=0.01,
            max_backoff=0.05,
            resume=True,
            resume_overlap_seconds=60,
            tls_verify=False,
        ),
        dedupe=DedupeConfig(
            path=tmp_path / "state" / "dedupe.sqlite3",
            retention_days=7,
            max_rows=1000,
            memory_cache=100,
        ),
        handler_configs={},
    )

    try:
        app = App.build(config, registry)
        try:
            app.start()
            assert done.wait(5.0), f"requests={server.snapshot()}"
        finally:
            app.shutdown()
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(2.0)

    assert seen == ["m0", "m1"]
    paths = server.snapshot()
    assert len(paths) >= 2
    assert "since=" in paths[1]
    assert "since=940" in paths[1]
