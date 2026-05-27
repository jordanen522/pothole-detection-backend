from datetime import date
from unittest.mock import patch

from pothole_backend.routes.events import _hash_device_id


def test_hash_is_deterministic():
    assert _hash_device_id("device-abc") == _hash_device_id("device-abc")


def test_different_devices_produce_different_hashes():
    assert _hash_device_id("device-001") != _hash_device_id("device-002")


def test_hash_rotates_daily():
    with patch("pothole_backend.routes.events.date") as mock_date:
        mock_date.today.return_value = date(2026, 5, 26)
        h1 = _hash_device_id("device-abc")

        mock_date.today.return_value = date(2026, 5, 27)
        h2 = _hash_device_id("device-abc")

    assert h1 != h2


def test_hash_is_64_char_hex():
    h = _hash_device_id("device-abc")
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_same_device_same_day_same_hash():
    with patch("pothole_backend.routes.events.date") as mock_date:
        mock_date.today.return_value = date(2026, 5, 26)
        assert _hash_device_id("device-xyz") == _hash_device_id("device-xyz")


def test_ingest_payload_strips_device_id(client, mock_sb, sample_event):
    """The JSON stored in nonanon_events must not contain the raw device_id."""
    from tests.conftest import make_result

    stored = {}

    original_insert = mock_sb.table.return_value.insert

    def capture(data):
        stored.update(data)
        return original_insert.return_value

    mock_sb.table.return_value.insert.side_effect = capture
    mock_sb.table.return_value.insert.return_value.execute.return_value = make_result()

    client.post("/events", json=sample_event)

    payload = stored.get("payload", {})
    assert "device_id" not in payload
    assert sample_event["device_id"] not in str(payload)
