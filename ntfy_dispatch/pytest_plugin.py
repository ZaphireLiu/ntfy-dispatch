"""Pytest plugin that isolates the process-wide handler registry.

Registered through the ``pytest11`` entry point, so any project that installs
ntfy-dispatch gets this fixture automatically.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from .registry import reset_registry


@pytest.fixture(autouse=True)
def isolate_handler_registry() -> Iterator[None]:
    reset_registry()
    yield
    reset_registry()
