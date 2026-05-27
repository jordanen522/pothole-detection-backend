from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pytest

from pothole_backend.models import AccelBurst
from pothole_backend.worker import process_event_math
from tests.conftest import make_result, per_table

HASH       = "a" * 64
LAT        = 47.2529
LNG        = -122.4443
DETECTED   = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
APP_VER    = "1.0"

NEARBY_LAT = LAT + 0.000001   # ~0.1 m away — inside cluster radius
FAR_LAT    = LAT + 1.0        # ~111 km away — outside cluster radius


def _accel():
    n = 100
    t = np.linspace(0, 0.5, n)
    z = (4 * 9.81 * np.sin(2 * np.pi * 12 * t)).tolist()
    return AccelBurst(z_values=z, timestamps_ms=list(range(0, n * 5, 5)))


# ---------------------------------------------------------------------------
# Duplicate guard
# ---------------------------------------------------------------------------

def test_duplicate_nearby_skips_insert(mock_sb):
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([
        {"latitude": NEARBY_LAT, "longitude": LNG}
    ])

    process_event_math(HASH, LAT, LNG, _accel(), DETECTED, APP_VER)

    assert not tables["events"].insert.called


def test_duplicate_same_location_different_device_hash_not_suppressed(mock_sb):
    # Different hash means different device — should proceed even at same coords
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([])
    mock_sb.rpc.return_value.execute.return_value = make_result([])
    tables["potholes"].insert.return_value.execute.return_value = make_result([{"pothole_id": "new-id"}])
    tables["events"].insert.return_value.execute.return_value = make_result([])

    process_event_math("b" * 64, LAT, LNG, _accel(), DETECTED, APP_VER)

    assert tables["events"].insert.called


def test_far_location_same_device_is_not_suppressed(mock_sb):
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([
        {"latitude": FAR_LAT, "longitude": LNG}
    ])
    mock_sb.rpc.return_value.execute.return_value = make_result([])
    tables["potholes"].insert.return_value.execute.return_value = make_result([{"pothole_id": "far-id"}])
    tables["events"].insert.return_value.execute.return_value = make_result([])

    process_event_math(HASH, LAT, LNG, _accel(), DETECTED, APP_VER)

    assert tables["events"].insert.called


# ---------------------------------------------------------------------------
# Clustering — new vs existing pothole
# ---------------------------------------------------------------------------

def test_no_nearby_pothole_creates_new(mock_sb):
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([])
    mock_sb.rpc.return_value.execute.return_value = make_result([])  # no nearby pothole
    tables["potholes"].insert.return_value.execute.return_value = make_result([{"pothole_id": "brand-new"}])
    tables["events"].insert.return_value.execute.return_value = make_result([])

    process_event_math(HASH, LAT, LNG, _accel(), DETECTED, APP_VER)

    tables["potholes"].insert.assert_called_once()


def test_nearby_pothole_found_updates_existing(mock_sb):
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([])
    mock_sb.rpc.return_value.execute.return_value = make_result([{"pothole_id": "existing-id"}])

    # upsert_pothole needs the current row to compute rolling averages
    current = MagicMock()
    current.data = {"canonical_lat": LAT, "canonical_lng": LNG, "severity_score": 0.5, "hit_count": 2}
    tables["potholes"].select.return_value.eq.return_value.single.return_value.execute.return_value = current
    tables["potholes"].update.return_value.eq.return_value.execute.return_value = make_result([])
    tables["events"].insert.return_value.execute.return_value = make_result([])

    process_event_math(HASH, LAT, LNG, _accel(), DETECTED, APP_VER)

    tables["potholes"].update.assert_called_once()
    assert not tables["potholes"].insert.called


# ---------------------------------------------------------------------------
# Events row uses device_id_hash, not raw device_id
# ---------------------------------------------------------------------------

def test_events_insert_uses_hash_not_raw_id(mock_sb):
    raw_device_id = "my-real-device-uuid"
    tables = per_table(mock_sb)
    tables["events"].select.return_value.eq.return_value.gte.return_value.execute.return_value = make_result([])
    mock_sb.rpc.return_value.execute.return_value = make_result([])
    tables["potholes"].insert.return_value.execute.return_value = make_result([{"pothole_id": "pid"}])
    tables["events"].insert.return_value.execute.return_value = make_result([])

    process_event_math(HASH, LAT, LNG, _accel(), DETECTED, APP_VER)

    call_kwargs = tables["events"].insert.call_args[0][0]
    assert call_kwargs.get("device_id_hash") == HASH
    assert "device_id" not in call_kwargs
    assert raw_device_id not in str(call_kwargs)
