# Pothole Detection Backend

FastAPI backend for the Pothole Detection system. Ingests accelerometer events from mobile devices, scores pothole severity via FFT, clusters GPS hits, and exposes a real-time monitoring layer.

---

## Table of Contents

- [Architecture](#architecture)
- [Environment Variables](#environment-variables)
- [Database Schema](#database-schema)
- [API Reference](#api-reference)
- [Deployment](#deployment)
- [Mobile Integration](#mobile-integration)

---

## Architecture

```
Phone App
   │
   │  POST /events  (202 Accepted — instant)
   ▼
event_queue table  (Supabase/Postgres)
   │
   │  Background worker polls every 5 s
   ▼
Queue Worker
   ├── FFT severity scoring  (8–20 Hz suspension band)
   ├── Duplicate guard       (same device + same location within 24 h)
   ├── GPS clustering        (configurable radius via PostGIS)
   ├── Centroid update       (rolling average lat/lng per cluster)
   └── Upsert → potholes + events tables

Monitoring Snapshot Worker  (every 15 min)
   └── Writes CPU / memory / queue / throughput → monitoring_snapshots
```

**Dead-letter queue (DLQ):** jobs that fail 3 times are moved to `status = dead_letter` and visible at `GET /monitoring/dlq`.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SUPABASE_URL` | Yes | — | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Yes | — | Supabase service role key |
| `CLUSTER_RADIUS_M` | No | `5.0` | GPS cluster radius in metres |
| `SAMPLE_RATE_HZ` | No | `200` | Expected accelerometer sample rate |
| `SNAPSHOT_INTERVAL_S` | No | `900` | Monitoring snapshot frequency (seconds) |
| `SNAPSHOT_RETENTION_H` | No | `24` | How long to keep monitoring snapshots |
| `PORT` | No | `8000` | Uvicorn listen port |

Copy `.env.example` to `.env` and fill in the Supabase values before running locally.

---

## Database Schema

Run migrations in order against your Supabase project via the SQL editor.

### Tables

#### `potholes`
Canonical, deduplicated pothole records. One row per unique road location.

| Column | Type | Description |
|---|---|---|
| `pothole_id` | UUID PK | Auto-generated |
| `canonical_lat` | float | Cluster centroid latitude — rolling average, updated on each hit |
| `canonical_lng` | float | Cluster centroid longitude — rolling average, updated on each hit |
| `severity_score` | float | Rolling average of hit severity scores (0–1) |
| `hit_count` | int | Total number of device hits |
| `priority_score` | float | Computed repair priority (severity × traffic weight) |
| `traffic_weight` | float | Road traffic multiplier, default 1.0 |
| `first_seen` | timestamptz | Timestamp of first report |
| `last_seen` | timestamptz | Timestamp of most recent report |

#### `events`
Raw per-device hits, linked to a canonical pothole record.

| Column | Type | Description |
|---|---|---|
| `event_id` | UUID PK | Auto-generated |
| `device_id` | text | Reporting device identifier |
| `pothole_id` | UUID FK | References `potholes` |
| `latitude` | float | Hit latitude |
| `longitude` | float | Hit longitude |
| `severity` | float | FFT severity score for this hit |
| `detected_at` | timestamptz | When the device detected the pothole |
| `app_version` | text | Optional app version string |

#### `event_queue`
Producer-consumer queue. The `/events` endpoint writes here; the background worker reads and deletes.

| Column | Type | Description |
|---|---|---|
| `id` | serial PK | Auto-incremented |
| `device_id` | text | Originating device |
| `payload` | jsonb | Full `PotholeEventIn` payload |
| `status` | text | `pending` \| `dead_letter` |
| `retry_count` | int | Number of failed processing attempts |
| `error_msg` | text | Last error message (DLQ diagnosis) |

#### `monitoring_snapshots`
15-minute ring buffer of system metrics. Rows older than 24 hours are auto-purged.

| Column | Type | Description |
|---|---|---|
| `captured_at` | timestamptz | Snapshot timestamp |
| `cpu_percent` | float | Process CPU % |
| `memory_used_mb` | float | Process RSS in MB |
| `memory_percent` | float | Process memory as % of system total |
| `db_size_mb` | float | Postgres database size in MB |
| `queue_pending` | int | Pending jobs in event_queue |
| `queue_dead_letter` | int | Dead-letter jobs in event_queue |
| `events_processed` | int | Events processed in this window |
| `events_failed` | int | Processing failures in this window |
| `events_rejected` | int | Ingest rejections in this window |
| `avg_pings_per_sec` | float | Average request rate over the window |

### RPC Functions

| Function | Description |
|---|---|
| `find_nearby_pothole(lat, lng, radius)` | Returns the nearest pothole within `radius` metres using PostGIS geography |
| `get_potholes_near(lat, lng, radius)` | Returns all potholes within `radius` metres, ordered by priority score |
| `get_queue_counts()` | Returns row counts grouped by status for the event_queue |
| `get_db_size_mb()` | Returns current database size in MB |
| `get_table_stats()` | Returns estimated row counts and sizes for core tables |
| `purge_old_snapshots()` | Deletes monitoring_snapshots older than 24 hours |

---

## API Reference

### Core

#### `GET /health`
Returns server status and current UTC timestamp.

```json
{ "status": "ok", "timestamp": "2026-05-22T00:00:00Z" }
```

#### `POST /events`
Ingest a pothole event. Returns `202 Accepted` immediately — processing happens asynchronously.

**Request body:**
```json
{
  "device_id": "abc123",
  "latitude": 47.6062,
  "longitude": -122.3321,
  "detected_at": "2026-05-22T10:00:00Z",
  "app_version": "1.0.0",
  "accel_burst": {
    "z_values": [0.1, -0.3, 1.2, ...],
    "timestamps_ms": [1716379200000, ...]
  }
}
```

`z_values` and `timestamps_ms` must each have at least 50 elements.

**Response:**
```json
{ "accepted": true, "queued_at": "2026-05-22T10:00:00Z" }
```

#### `GET /potholes`
Returns pothole records ordered by priority score descending. When `lat` and `lng` are provided, results are filtered to within `radius_miles` of that point using PostGIS — recommended for all client requests to avoid returning unbounded data.

| Query param | Default | Description |
|---|---|---|
| `lat` | — | Centre latitude for geographic filter |
| `lng` | — | Centre longitude for geographic filter |
| `radius_miles` | `80.0` | Search radius in miles (only applies when lat/lng provided) |
| `min_priority` | `0.0` | Filter out potholes below this priority score |
| `limit` | `200` | Max results (capped at 500) |
| `offset` | `0` | Pagination offset |

**Example:**
```
GET /potholes?lat=47.2529&lng=-122.4443&radius_miles=10
```

#### `GET /potholes/geojson`
Returns potholes as a GeoJSON `FeatureCollection`, ready to render on a map. Supports the same geographic filter as `/potholes`.

| Query param | Default | Description |
|---|---|---|
| `lat` | — | Centre latitude for geographic filter |
| `lng` | — | Centre longitude for geographic filter |
| `radius_miles` | `80.0` | Search radius in miles (only applies when lat/lng provided) |
| `min_priority` | `0.0` | Filter threshold |

**Example:**
```
GET /potholes/geojson?lat=47.2529&lng=-122.4443&radius_miles=25
```

### Monitoring

#### `GET /monitoring/live`
Real-time system snapshot. Call every 5–10 seconds from a dashboard. Does not reset any counters.

```json
{
  "captured_at": "2026-05-22T10:00:00Z",
  "cpu_percent": 2.1,
  "memory_used_mb": 84.3,
  "memory_percent": 0.5,
  "db_size_mb": 12.4,
  "queue_pending": 3,
  "queue_dead_letter": 0,
  "events_processed": 142,
  "events_failed": 1,
  "events_rejected": 0,
  "pings_per_sec": 0.8
}
```

#### `GET /monitoring/history`
Returns up to 24 hours of 15-minute snapshots, oldest first.

| Query param | Default | Description |
|---|---|---|
| `hours` | `24` | Lookback window (1–24) |

#### `GET /monitoring/dlq`
Returns dead-letter queue items for failure diagnosis.

| Query param | Default | Description |
|---|---|---|
| `limit` | `50` | Max results (capped at 200) |

---

## Deployment

### Unraid + Cloudflare Tunnel

The Docker image is published at `salixbloom/pothole-backend`.

**Unraid Add Container settings:**

| Field | Value |
|---|---|
| Repository | `salixbloom/pothole-backend` |
| Network Type | `bridge` |
| Port Mapping | Container `8000` → Host `8000`, TCP |
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Your Supabase service role key |
| Restart Policy | `unless-stopped` |

**Cloudflare Tunnel** — add a public hostname in Zero Trust → Networks → Tunnels:

| Field | Value |
|---|---|
| Subdomain | `api` (or whatever you prefer) |
| Domain | your domain |
| Type | HTTP |
| URL | `localhost:8000` |

Cloudflare handles HTTPS termination — the container only speaks plain HTTP.

### Local Development

```bash
cp .env.example .env
# fill in SUPABASE_URL and SUPABASE_SERVICE_KEY

pip install -r requirements.txt
uvicorn pothole_backend:app --reload
```

API docs available at `http://localhost:8000/docs`.

### Rebuilding the Docker Image

```bash
docker build -t salixbloom/pothole-backend .
docker push salixbloom/pothole-backend
```

Then in Unraid, stop the container and click **Force Update** to pull the new image.

---

## Mobile Integration

`orientedBurst.ts` (in this repo) is a TypeScript module for Expo that collects a reoriented accelerometer burst ready to POST to `/events`.

It uses the phone's gyroscope to continuously track orientation during the burst window, rotating each accelerometer sample into world frame so the backend receives world-vertical acceleration regardless of how the phone is mounted.

**Install dependency:**
```bash
npx expo install expo-sensors
```

**Usage:**
```typescript
import { collectOrientedBurst } from './orientedBurst';

const burst = await collectOrientedBurst(); // 250 ms, 200 Hz

await fetch('https://api.yourdomain.com/events', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    device_id: deviceId,
    latitude,
    longitude,
    detected_at: new Date().toISOString(),
    accel_burst: burst,
  }),
});
```

**Options:**
```typescript
await collectOrientedBurst({
  durationMs:   250,  // burst length — must produce ≥50 samples
  sampleRateHz: 200,  // polling rate
  settleMs:     100,  // pre-burst gravity estimation window
});
```

Falls back to initial gravity-only orientation on devices without a gyroscope.
