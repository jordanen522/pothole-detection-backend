import os
from collections import defaultdict
from unittest.mock import MagicMock, patch

# Must be set before any app module is imported during collection
os.environ.setdefault("SUPABASE_URL", "https://placeholder.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "placeholder-service-key")
os.environ.setdefault("DEVICE_ID_SALT", "test-salt-1234567890abcdef")

# Patch create_client so no real connection is attempted at import time.
# The returned mock is only used for the initial module-level assignment in
# config.py — individual tests replace pothole_backend.config.supabase with
# a fresh mock via the mock_sb fixture below.
_init_client = MagicMock()
_create_client_patcher = patch("supabase.create_client", return_value=_init_client)
_create_client_patcher.start()

import pytest
import numpy as np
from fastapi.testclient import TestClient
from pothole_backend.main import app


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_sb():
    """Fresh MagicMock wired as pothole_backend.config.supabase for each test."""
    m = MagicMock()
    _SUPABASE_TARGETS = [
        "pothole_backend.config.supabase",
        "pothole_backend.database.supabase",
        "pothole_backend.monitoring.supabase",
        "pothole_backend.worker.supabase",
        "pothole_backend.routes.events.supabase",
        "pothole_backend.routes.potholes.supabase",
    ]
    patchers = [patch(t, m) for t in _SUPABASE_TARGETS]
    for p in patchers:
        p.start()
    yield m
    for p in patchers:
        p.stop()


@pytest.fixture
def client(mock_sb):
    return TestClient(app)


# ---------------------------------------------------------------------------
# Helpers shared across test modules
# ---------------------------------------------------------------------------

def make_result(data=None):
    """MagicMock with a .data attribute."""
    r = MagicMock()
    r.data = data if data is not None else []
    return r


def make_rpc_mock(data=None):
    """Mock for supabase.rpc(...) chains — .execute() returns data."""
    m = MagicMock()
    m.execute.return_value = make_result(data)
    return m


def per_table(mock_sb):
    """
    Installs a side_effect on mock_sb.table so each table name gets its own
    independent MagicMock. Returns a defaultdict so tests can pre-configure
    table mocks before the code under test calls supabase.table().
    """
    tables = defaultdict(MagicMock)
    mock_sb.table.side_effect = lambda name: tables[name]
    return tables


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_burst():
    n = 100
    t = np.linspace(0, n / 200, n)
    z = (4 * 9.81 * np.sin(2 * np.pi * 12 * t)).tolist()
    return {"z_values": z, "timestamps_ms": list(range(0, n * 5, 5))}


@pytest.fixture
def sample_event(sample_burst):
    return {
        "device_id":   "test-device-001",
        "latitude":    47.2529,
        "longitude":   -122.4443,
        "detected_at": "2026-05-26T12:00:00+00:00",
        "accel_burst": sample_burst,
        "app_version": "1.0.0",
    }
