from __future__ import annotations

import pytest

from ntfy_dispatch.config import ConfigError, load_config, missing_secret_envs


def write_config(tmp_path, text: str):
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_load_full_config(tmp_path):
    path = write_config(
        tmp_path,
        """
[core]
log_level = "debug"
state_dir = "state"
handler_packages = ["handlers", "extra"]
queue_size = 10
stats_interval = 1.5
shutdown_drain_seconds = 2
http_timeout = 3

[ntfy]
name = "test"
base_url = "https://ntfy.example.com/base/"
auth = "none"
read_timeout = 30
connect_timeout = 5
min_backoff = 0.5
max_backoff = 10
resume = false
resume_overlap_seconds = 60
tls_verify = true

[dedupe]
retention_days = 3
max_rows = 1000
memory_cache = 100

[handlers.foo]
target = "https://example.com"
""",
    )

    config = load_config(path)

    assert config.core.log_level == "DEBUG"
    assert config.core.state_dir == tmp_path / "state"
    assert config.core.handler_packages == ("handlers", "extra")
    assert config.core.queue_size == 10
    assert config.core.stats_interval == 1.5
    assert config.core.shutdown_drain_seconds == 2
    assert config.core.http_timeout == 3
    assert config.ntfy.name == "test"
    assert config.ntfy.base_url == "https://ntfy.example.com/base"
    assert config.ntfy.auth.type == "none"
    assert config.ntfy.resume is False
    assert config.ntfy.resume_overlap_seconds == 60
    assert config.dedupe.path == tmp_path / "state" / "dedupe.sqlite3"
    assert config.dedupe.retention_days == 3
    assert config.dedupe.max_rows == 1000
    assert config.dedupe.memory_cache == 100
    assert config.handler_configs == {"foo": {"target": "https://example.com"}}


def test_ca_file_is_resolved_relative_to_config(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
auth = "none"
ca_file = "certs/ca.pem"
""",
    )
    config = load_config(path)
    assert config.ntfy.ca_file == tmp_path / "certs" / "ca.pem"


def test_missing_config_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.toml")


def test_invalid_toml(tmp_path):
    path = write_config(tmp_path, "[core\n")
    with pytest.raises(ConfigError, match="invalid TOML"):
        load_config(path)


def test_unknown_top_level_key(tmp_path):
    path = write_config(tmp_path, "unknown = 1\n")
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(path)


def test_invalid_base_url(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
base_url = "ftp://example.com"
""",
    )
    with pytest.raises(ConfigError, match="base_url"):
        load_config(path)


def test_token_auth_requires_env_name(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
auth = "token"
""",
    )
    with pytest.raises(ConfigError, match="token_env"):
        load_config(path)


def test_basic_auth_requires_both_env_names(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
auth = "basic"
username_env = "USER"
""",
    )
    with pytest.raises(ConfigError, match="username_env"):
        load_config(path)


def test_min_backoff_must_not_exceed_max(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
min_backoff = 10
max_backoff = 1
""",
    )
    with pytest.raises(ConfigError, match="min_backoff"):
        load_config(path)


def test_non_finite_number_is_rejected(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
read_timeout = nan
""",
    )
    with pytest.raises(ConfigError, match="finite"):
        load_config(path)


def test_ca_file_requires_tls_verify(tmp_path):
    path = write_config(
        tmp_path,
        """
[ntfy]
tls_verify = false
ca_file = "ca.pem"
""",
    )
    with pytest.raises(ConfigError, match="ca_file"):
        load_config(path)


def test_missing_secret_envs(tmp_path, monkeypatch):
    path = write_config(
        tmp_path,
        """
[ntfy]
auth = "token"
token_env = "NTFY_TEST_TOKEN"
""",
    )
    config = load_config(path)
    monkeypatch.delenv("NTFY_TEST_TOKEN", raising=False)
    assert missing_secret_envs(config.ntfy) == ["NTFY_TEST_TOKEN"]
    monkeypatch.setenv("NTFY_TEST_TOKEN", "secret")
    assert missing_secret_envs(config.ntfy) == []
