from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_cli_check_loads_example_handlers(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[core]
state_dir = "state"
handler_packages = ["handlers"]

[ntfy]
base_url = "https://ntfy.sh"
auth = "none"
""",
        encoding="utf-8",
    )
    project_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ntfy_dispatch",
            "--config",
            str(config_path),
            "--check",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    assert "example" in result.stdout
    assert "dispatch-demo" in result.stdout


def test_cli_check_warns_about_missing_token(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[core]
state_dir = "state"
handler_packages = ["handlers"]

[ntfy]
base_url = "https://ntfy.sh"
auth = "token"
token_env = "NTFY_DISPATCH_MISSING_TEST_TOKEN"
""",
        encoding="utf-8",
    )
    project_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.pop("NTFY_DISPATCH_MISSING_TEST_TOKEN", None)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ntfy_dispatch",
            "--config",
            str(config_path),
            "--check",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    assert "NTFY_DISPATCH_MISSING_TEST_TOKEN" in result.stderr
