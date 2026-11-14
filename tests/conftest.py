"""Shared fixtures: an in-memory store and an app with the BEACN consumer disabled."""

from __future__ import annotations

import pytest
from sillo.testclient import TestClient

from logsman.core.app import create_app
from logsman.core.config import AppConfig
from logsman.storage.memory import MemoryEventStore


@pytest.fixture
def store() -> MemoryEventStore:
    """A fresh in-memory store per test."""
    return MemoryEventStore()


@pytest.fixture
def app(store):
    """An app wired to ``store`` with no ``.env`` and no BEACN consumer."""
    settings = AppConfig(
        _env_file=None,
        app_env="testing",
        debug=True,
        beacn_enabled=False,
    )
    return create_app(settings, store=store)


@pytest.fixture
def client(app):
    """A started test client (startup/shutdown hooks run)."""
    with TestClient(app) as test_client:
        yield test_client
