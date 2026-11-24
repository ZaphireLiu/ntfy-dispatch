"""ntfy JSON stream reader."""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from typing import Protocol
from urllib.parse import quote

import httpx

from .config import AuthConfig, ConfigError, NtfyConfig
from .dedupe import DedupeStore
from .models import Message
from .registry import HandlerSpec
from .stats import Stats
from .version import __version__

STABLE_CONNECTION_SECONDS = 5.0


class WorkerLike(Protocol):
    def submit(self, message: Message) -> bool: ...


class StreamError(Exception):
    """Base class for reader errors."""


class HttpStatusError(StreamError):
    def __init__(self, status_code: int, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class NtfyReader:
    """Maintain one long-lived ntfy subscription and dispatch messages."""

    def __init__(
        self,
        *,
        config: NtfyConfig,
        topics: frozenset[str],
        handlers_by_topic: Mapping[str, tuple[HandlerSpec, ...]],
        workers: Mapping[str, WorkerLike],
        dedupe: DedupeStore,
        stats: Stats,
        stop_event: threading.Event,
    ) -> None:
        if not topics:
            raise ValueError("topics must not be empty")
        self._config = config
        self._topics = topics
        self._handlers_by_topic = handlers_by_topic
        self._workers = workers
        self._dedupe = dedupe
        self._stats = stats
        self._stop_event = stop_event
        self._logger = logging.getLogger("ntfy_dispatch.stream")
        self._thread = threading.Thread(target=self._run, name="ntfy-reader", daemon=True)
        self._lock = threading.Lock()
        self._client: httpx.Client | None = None
        self._response: httpx.Response | None = None
        self._auth = self._build_auth(config.auth)
        self._headers = self._build_headers(config.auth)

    @staticmethod
    def _build_auth(auth: AuthConfig) -> httpx.BasicAuth | None:
        if auth.type != "basic":
            return None
        username_env = auth.username_env
        password_env = auth.password_env
        if not username_env or not password_env:
            raise ConfigError("basic auth requires username_env and password_env")
        username = os.environ.get(username_env)
        password = os.environ.get(password_env)
        if username is None or password is None:
            missing = [
                name
                for name, value in ((username_env, username), (password_env, password))
                if value is None
            ]
            raise ConfigError("missing required environment variable(s): " + ", ".join(missing))
        return httpx.BasicAuth(username, password)

    @staticmethod
    def _build_headers(auth: AuthConfig) -> dict[str, str]:
        headers = {"User-Agent": f"ntfy-dispatch/{__version__}"}
        if auth.type == "token":
            if not auth.token_env:
                raise ConfigError("token auth requires token_env")
            token = os.environ.get(auth.token_env)
            if not token:
                raise ConfigError(f"environment variable {auth.token_env!r} is not set")
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def build_url(self) -> str:
        topics = ",".join(sorted(self._topics))
        return f"{self._config.base_url.rstrip('/')}/{quote(topics, safe=',')}/json"

    def build_params(self) -> dict[str, str]:
        if not self._config.resume:
            return {}
        last_time = self._dedupe.get_last_time(self._config.name)
        if last_time is None:
            return {}
        since = max(0, last_time - self._config.resume_overlap_seconds)
        return {"since": str(since)}

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float | None = None) -> bool:
        if self._thread.ident is None:
            return True
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            response = self._response
            client = self._client
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def _new_client(self) -> httpx.Client:
        verify: bool | str = self._config.tls_verify
        if self._config.ca_file is not None:
            verify = str(self._config.ca_file)
        return httpx.Client(
            verify=verify,
            timeout=httpx.Timeout(
                connect=self._config.connect_timeout,
                read=self._config.read_timeout,
                write=10.0,
                pool=10.0,
            ),
            auth=self._auth,
            follow_redirects=True,
            trust_env=True,
        )

    def _run(self) -> None:
        backoff = self._config.min_backoff
        try:
            while not self._stop_event.is_set():
                opened_at: float | None = None
                retry_after: float | None = None
                try:
                    opened_at = self._connect_once()
                    if self._stop_event.is_set():
                        break
                    self._stats.inc("disconnects")
                    self._logger.warning("ntfy stream closed; reconnecting")
                except HttpStatusError as exc:
                    if self._stop_event.is_set():
                        break
                    self._stats.inc("disconnects")
                    retry_after = exc.retry_after
                    self._log_http_error(exc)
                    if exc.status_code in (401, 403):
                        backoff = max(backoff, 300.0)
                    elif exc.status_code == 404:
                        backoff = max(backoff, 60.0)
                except Exception as exc:
                    if self._stop_event.is_set():
                        break
                    self._stats.inc("disconnects")
                    self._logger.warning("ntfy stream error: %s", exc)

                if (
                    opened_at is not None
                    and time.monotonic() - opened_at >= STABLE_CONNECTION_SECONDS
                ):
                    backoff = self._config.min_backoff

                delay = backoff * random.uniform(0.8, 1.2)
                if retry_after is not None:
                    delay = max(delay, retry_after)
                delay = max(0.0, delay)
                self._logger.debug("reconnecting in %.2fs", delay)
                if self._stop_event.wait(delay):
                    break
                backoff = min(backoff * 2.0, self._config.max_backoff)
        finally:
            self._logger.info("ntfy reader stopped")

    def _log_http_error(self, exc: HttpStatusError) -> None:
        if exc.status_code == 401:
            self._logger.error(
                "ntfy authentication failed (HTTP 401); check ntfy.auth and token_env"
            )
        elif exc.status_code == 403:
            self._logger.error("ntfy access denied (HTTP 403); check topic ACL")
        elif exc.status_code == 429:
            self._logger.warning("ntfy rate limited (HTTP 429); retry_after=%s", exc.retry_after)
        elif exc.status_code == 404:
            self._logger.error("ntfy endpoint not found (HTTP 404); check ntfy.base_url and topics")
        else:
            self._logger.warning("ntfy request failed: %s", exc)

    def _connect_once(self) -> float | None:
        client = self._new_client()
        with self._lock:
            self._client = client
        try:
            with client.stream(
                "GET",
                self.build_url(),
                params=self.build_params(),
                headers=self._headers,
            ) as response:
                with self._lock:
                    self._response = response
                if response.status_code != 200:
                    body = self._read_error_body(response)
                    retry_after = self._parse_retry_after(response)
                    raise HttpStatusError(
                        response.status_code,
                        f"HTTP {response.status_code}: {body[:200]}",
                        retry_after,
                    )
                if response.headers.get("X-Messages-Truncated", "").lower() in {"1", "true"}:
                    self._logger.warning(
                        "ntfy replay was truncated; "
                        "older cached messages were dropped by the server"
                    )
                return self._consume(response)
        finally:
            with self._lock:
                self._response = None
                if self._client is client:
                    self._client = None
            try:
                client.close()
            except Exception:
                pass

    @staticmethod
    def _read_error_body(response: httpx.Response, limit: int = 4096) -> str:
        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in response.iter_bytes():
                chunks.append(chunk)
                total += len(chunk)
                if total >= limit:
                    break
        except Exception:
            return ""
        return b"".join(chunks)[:limit].decode("utf-8", "replace")

    @staticmethod
    def _parse_retry_after(response: httpx.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
        try:
            parsed = parsedate_to_datetime(raw)
            if parsed is None:
                return None
            return max(0.0, parsed.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None

    def _consume(self, response: httpx.Response) -> float | None:
        opened_at: float | None = None
        for line in response.iter_lines():
            if self._stop_event.is_set():
                break
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                self._stats.inc("parse_errors")
                self._logger.warning("invalid JSON from ntfy (length=%d)", len(line))
                continue
            if not isinstance(event, dict):
                self._stats.inc("parse_errors")
                self._logger.warning("ntfy event is not a JSON object")
                continue

            event_type = event.get("event")
            if not isinstance(event_type, str):
                self._stats.inc("parse_errors")
                self._logger.warning("ntfy event has no valid 'event' field")
                continue
            if event_type == "open":
                opened_at = time.monotonic()
                self._stats.inc("connects")
                self._logger.info(
                    "subscribed to %d topic(s) on %s",
                    len(self._topics),
                    self._config.base_url,
                )
            elif event_type == "message":
                self.handle_message(event)
            elif event_type == "keepalive":
                self._logger.debug("keepalive")
            elif event_type in {"message_delete", "message_clear"}:
                self._logger.debug("ignoring %s event", event_type)
            elif event_type == "poll_request":
                self._logger.debug("ignoring poll_request event")
            else:
                self._logger.debug("ignoring unknown event type %r", event_type)
        return opened_at

    def handle_message(self, event: Mapping[str, object]) -> None:
        try:
            message = Message.from_event(self._config.name, event)
        except ValueError as exc:
            self._stats.inc("parse_errors")
            self._logger.warning("invalid message event: %s", exc)
            return

        self._stats.inc("received")
        if not self._dedupe.is_new_and_mark(
            message.instance, message.topic, message.id, message.time
        ):
            self._stats.inc("deduped")
            self._logger.debug("duplicate message id=%s topic=%s", message.id, message.topic)
            return

        specs = self._handlers_by_topic.get(message.topic, ())
        if not specs:
            self._logger.debug("no handler for topic=%s", message.topic)
            return

        for spec in specs:
            worker = self._workers.get(spec.name)
            if worker is None:
                self._logger.error("handler %s is registered but has no worker", spec.name)
                continue
            worker.submit(message)
