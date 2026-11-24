from __future__ import annotations

import pytest

from ntfy_dispatch.registry import reset_registry


@pytest.fixture(autouse=True)
def _reset_global_registry():
    reset_registry()
    yield
    reset_registry()
