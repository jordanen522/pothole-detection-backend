import pytest

from tests.conftest import make_result, make_rpc_mock, per_table


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert "timestamp" in resp.json()


# ---------------------------------------------------------------------------
# POST /events
# ---------------------------------------------------------------------------

def test_ingest_event_accepted(client, mock_sb, sample_event):
    mock_sb.table.return_value.insert.return_value.execute.return_value = make_result()
    resp = client.post("/events", json=sample_event)
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True
    assert "queued_at" in resp.json()


def test_ingest_event_missing_device_id(client, sample_event):
    del sample_event["device_id"]
    resp = client.post("/events", json=sample_event)
    assert resp.status_code == 422


def test_ingest_event_invalid_latitude(client, sample_event):
    sample_event["latitude"] = 999.0
    resp = client.post("/events", json=sample_event)
    assert resp.status_code == 422


def test_ingest_event_invalid_longitude(client, sample_event):
    sample_event["longitude"] = -999.0
    resp = client.post("/events", json=sample_event)
    assert resp.status_code == 422


def test_ingest_event_burst_too_short(client, sample_event):
    sample_event["accel_burst"]["z_values"]      = [0.1] * 10
    sample_event["accel_burst"]["timestamps_ms"] = list(range(10))
    resp = client.post("/events", json=sample_event)
    assert resp.status_code == 422


def test_ingest_event_does_not_store_raw_device_id(client, mock_sb, sample_event):
    captured = {}

    def capture_insert(data):
        captured.update(data)
        return mock_sb.table.return_value.insert.return_value

    mock_sb.table.return_value.insert.side_effect = capture_insert
    mock_sb.table.return_value.insert.return_value.execute.return_value = make_result()

    client.post("/events", json=sample_event)

    assert "device_id" not in captured.get("payload", {})
    assert sample_event["device_id"] not in str(captured.get("payload", {}))


# ---------------------------------------------------------------------------
# GET /potholes
# ---------------------------------------------------------------------------

_POTHOLE_ROW = {
    "pothole_id":     "abc-123",
    "canonical_lat":  47.25,
    "canonical_lng":  -122.44,
    "severity_score": 0.7,
    "hit_count":      3,
    "priority_score": 2.1,
    "first_seen":     "2026-01-01T00:00:00+00:00",
    "last_seen":      "2026-05-26T00:00:00+00:00",
    "traffic_weight": 1.0,
}


def test_get_potholes_returns_list(client, mock_sb):
    tables = per_table(mock_sb)
    tables["potholes"].select.return_value.gte.return_value.order.return_value.range.return_value.execute.return_value = make_result([_POTHOLE_ROW])
    resp = client.get("/potholes")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_get_potholes_with_geo_filter(client, mock_sb):
    mock_sb.rpc.return_value.execute.return_value = make_result([_POTHOLE_ROW])
    resp = client.get("/potholes?lat=47.25&lng=-122.44&radius_miles=10")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_get_potholes_limit_capped_at_500(client, mock_sb):
    tables = per_table(mock_sb)
    tables["potholes"].select.return_value.gte.return_value.order.return_value.range.return_value.execute.return_value = make_result([])
    resp = client.get("/potholes?limit=9999")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /potholes/geojson
# ---------------------------------------------------------------------------

def test_get_potholes_geojson_structure(client, mock_sb):
    tables = per_table(mock_sb)
    tables["potholes"].select.return_value.gte.return_value.execute.return_value = make_result([{
        "pothole_id": "abc", "canonical_lat": 47.25, "canonical_lng": -122.44,
        "severity_score": 0.7, "hit_count": 3, "priority_score": 2.1,
        "last_seen": "2026-05-26T00:00:00+00:00",
    }])
    resp = client.get("/potholes/geojson")
    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1
    feat = body["features"][0]
    assert feat["geometry"]["type"] == "Point"
    assert feat["geometry"]["coordinates"] == [-122.44, 47.25]


def test_get_potholes_geojson_with_geo_filter(client, mock_sb):
    mock_sb.rpc.return_value.execute.return_value = make_result([{
        "pothole_id": "abc", "canonical_lat": 47.25, "canonical_lng": -122.44,
        "severity_score": 0.7, "hit_count": 3, "priority_score": 2.1,
        "last_seen": "2026-05-26T00:00:00+00:00",
    }])
    resp = client.get("/potholes/geojson?lat=47.25&lng=-122.44")
    assert resp.status_code == 200
    assert resp.json()["type"] == "FeatureCollection"


# ---------------------------------------------------------------------------
# GET /monitoring/*
# ---------------------------------------------------------------------------

def _setup_monitoring_rpcs(mock_sb):
    def rpc_side_effect(func_name, params):
        data = {
            "get_db_size_mb":   150.0,
            "get_queue_counts": [{"status": "pending", "cnt": 3}],
        }.get(func_name, [])
        return make_rpc_mock(data)

    mock_sb.rpc.side_effect = rpc_side_effect


def test_monitoring_live_returns_snapshot(client, mock_sb):
    _setup_monitoring_rpcs(mock_sb)
    resp = client.get("/monitoring/live")
    assert resp.status_code == 200
    body = resp.json()
    for field in ("cpu_percent", "memory_used_mb", "memory_percent",
                  "db_size_mb", "queue_pending", "queue_dead_letter",
                  "events_processed", "events_failed", "events_rejected", "pings_per_sec"):
        assert field in body


def test_monitoring_history_default_24h(client, mock_sb):
    tables = per_table(mock_sb)
    tables["monitoring_snapshots"].select.return_value.gte.return_value.order.return_value.execute.return_value = make_result([])
    resp = client.get("/monitoring/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body["period_hours"] == 24
    assert "snapshots" in body


def test_monitoring_history_hours_clamped(client, mock_sb):
    tables = per_table(mock_sb)
    tables["monitoring_snapshots"].select.return_value.gte.return_value.order.return_value.execute.return_value = make_result([])
    resp = client.get("/monitoring/history?hours=999")
    assert resp.status_code == 200
    assert resp.json()["period_hours"] == 24


def test_monitoring_dlq_returns_items(client, mock_sb):
    tables = per_table(mock_sb)
    tables["nonanon_events"].select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = make_result([])
    resp = client.get("/monitoring/dlq")
    assert resp.status_code == 200
    assert "items" in resp.json()
    assert "dead_letter_count" in resp.json()
