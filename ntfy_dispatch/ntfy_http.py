"""HTTP helpers shared by the ntfy reader and publisher."""

from __future__ import annotations

import os

import httpx

from .config import AuthConfig, ConfigError, NtfyConfig
from .version import __version__


def build_auth(auth: AuthConfig) -> httpx.BasicAuth | None:
    """Resolve basic auth credentials from the environment."""

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


def build_headers(auth: AuthConfig) -> dict[str, str]:
    """Build the request headers required by the configured auth mode."""

    headers = {"User-Agent": f"ntfy-dispatch/{__version__}"}
    if auth.type == "token":
        if not auth.token_env:
            raise ConfigError("token auth requires token_env")
        token = os.environ.get(auth.token_env)
        if not token:
            raise ConfigError(f"environment variable {auth.token_env!r} is not set")
        headers["Authorization"] = f"Bearer {token}"
    return headers


def build_verify(config: NtfyConfig) -> bool | str:
    """Return the ``verify`` value for an httpx client."""

    if config.ca_file is not None:
        return str(config.ca_file)
    return config.tls_verify


def build_client(
    config: NtfyConfig,
    *,
    timeout: httpx.Timeout | float,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """Create an httpx client for talking to the configured ntfy instance."""

    return httpx.Client(
        verify=build_verify(config),
        timeout=timeout,
        auth=build_auth(config.auth),
        headers=build_headers(config.auth),
        follow_redirects=True,
        trust_env=True,
        transport=transport,
    )
