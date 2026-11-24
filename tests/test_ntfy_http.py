from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from ntfy_dispatch.config import AuthConfig, ConfigError, NtfyConfig
from ntfy_dispatch.ntfy_http import build_auth, build_client, build_headers, build_verify


def test_build_headers_without_auth():
    headers = build_headers(AuthConfig())

    assert headers["User-Agent"].startswith("ntfy-dispatch/")
    assert "Authorization" not in headers


def test_build_headers_with_token(monkeypatch):
    monkeypatch.setenv("NTFY_TEST_TOKEN", "secret")

    headers = build_headers(AuthConfig(type="token", token_env="NTFY_TEST_TOKEN"))

    assert headers["Authorization"] == "Bearer secret"


def test_build_headers_requires_token_env():
    with pytest.raises(ConfigError, match="token_env"):
        build_headers(AuthConfig(type="token"))


def test_build_headers_requires_token_value(monkeypatch):
    monkeypatch.delenv("NTFY_ABSENT_TOKEN", raising=False)

    with pytest.raises(ConfigError, match="NTFY_ABSENT_TOKEN"):
        build_headers(AuthConfig(type="token", token_env="NTFY_ABSENT_TOKEN"))


def test_build_auth_basic(monkeypatch):
    monkeypatch.setenv("NTFY_TEST_USER", "user")
    monkeypatch.setenv("NTFY_TEST_PASS", "pass")

    auth = build_auth(
        AuthConfig(
            type="basic",
            username_env="NTFY_TEST_USER",
            password_env="NTFY_TEST_PASS",
        )
    )

    assert isinstance(auth, httpx.BasicAuth)


def test_build_auth_requires_both_env_names():
    with pytest.raises(ConfigError, match="username_env"):
        build_auth(AuthConfig(type="basic", username_env="ONLY_USER"))


def test_build_verify(tmp_path: Path):
    ca_file = tmp_path / "ca.pem"

    assert build_verify(NtfyConfig()) is True
    assert build_verify(NtfyConfig(tls_verify=False)) is False
    assert build_verify(NtfyConfig(ca_file=ca_file)) == str(ca_file)


def test_build_client_sets_base_config():
    client = build_client(
        NtfyConfig(base_url="https://ntfy.example.com"),
        timeout=5.0,
        transport=httpx.MockTransport(lambda request: httpx.Response(204, request=request)),
    )
    try:
        response = client.post("https://ntfy.example.com/")
        assert response.status_code == 204
    finally:
        client.close()
