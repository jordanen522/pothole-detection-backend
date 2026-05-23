"""
=============================================================================
POTHOLE DETECTION BACKEND — FastAPI Application
=============================================================================
REPO PURPOSE:
    This is the backend service for the Pothole Detection mobile app.
    It is intentionally separate from the mobile app (Expo/React Native) and
    the city dashboard (Next.js). This repo handles ONLY:
        1. Receiving pothole event data from mobile clients
        2. Running FFT-based severity scoring on accelerometer bursts
        3. Clustering nearby GPS hits into a single canonical "pothole" record
        4. Storing results in Supabase (Postgres + PostGIS)
        5. Serving aggregated pothole data to the city dashboard

SISTER REPOS:
    - Mobile App  : Expo (React Native) — sensor capture, SQLite queue, sync
    - Dashboard   : Next.js + Mapbox GL JS — city-facing map and leaderboard

HOSTING:
    Deployed on Railway. Push to `main` triggers auto-deploy.
    Environment variables are set in Railway's dashboard (see .env.example).

DATA FLOW REMINDER (for AI agents reading this file):
    Phone sensors
        → peak detection on-device (Z-axis spike pattern)
        → GPS coords + raw accel burst queued in SQLite on phone
        → POST /events  (this backend)
        → FFT severity scoring  (see score_severity())
        → GPS clustering via Shapely / PostGIS  (see cluster_events())
        → Upsert into `potholes` table in Supabase
        → GET /potholes  (served to dashboard)

PRIORITY SCORE FORMULA:
    priority = severity_score × hit_frequency × traffic_weight
    - severity_score  : 0–1 float from FFT peak analysis
    - hit_frequency   : number of unique device hits in last 30 days
    - traffic_weight  : road-segment AADT estimate (future: pull from city GIS)

=============================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Third-party — install via:  pip install fastapi uvicorn numpy scipy shapely
#                               supabase python-dotenv
# ---------------------------------------------------------------------------
import numpy as np                      # numerical ops on accel burst arrays
from scipy.signal import find_peaks     # peak detection used in FFT scoring
from scipy.fft import fft, fftfreq     # frequency domain analysis
from shapely.geometry import Point      # geometry type for GPS clustering
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from supabase import create_client, Client  # Supabase Python SDK
from dotenv import load_dotenv          # loads .env file locally

# ---------------------------------------------------------------------------
# Logging — Railway surfaces these in its log stream
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pothole-backend")

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
load_dotenv()  # no-op in production; Railway injects env vars directly

# Supabase connection — set these in Railway environment variables
SUPABASE_URL: str = os.environ["SUPABASE_URL"]          # e.g. https://xyz.supabase.co
SUPABASE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]  # service role key, NOT anon

# Clustering radius — two GPS hits within this distance (metres) are the same pothole
CLUSTER_RADIUS_M: float = float(os.getenv("CLUSTER_RADIUS_M", "5.0"))

# Accelerometer sample rate the mobile app uses (Hz)
# IMPORTANT: must match the value set in the Expo app (expo-sensors DeviceMotion)
SAMPLE_RATE_HZ: int = int(os.getenv("SAMPLE_RATE_HZ", "200"))

# ---------------------------------------------------------------------------
# Supabase client (singleton — reused across all requests)
# ---------------------------------------------------------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# FastAPI app instance
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Pothole Detection API",
    description=(
        "Ingests pothole events from mobile clients, scores severity via FFT, "
        "clusters GPS hits, and serves aggregated data to the city dashboard."
    ),
    version="1.0.0",
)

# CORS — allow the Next.js dashboard (Vercel) and local dev to call this API.
# In production, tighten `allow_origins` to your Vercel domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # TODO: lock down to dashboard domain in prod
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# =============================================================================
# PYDANTIC MODELS  (request / response shapes — doubles as API documentation)
# =============================================================================

class AccelBurst(BaseModel):
    """
    A short burst of raw Z-axis accelerometer readings captured around the
    moment of pothole impact, sent from the mobile app.

    The mobile app captures ~0.5 s before and ~1.0 s after the spike, giving
    roughly 300 samples at 200 Hz.  More is fine; scoring truncates as needed.

    z_values: list of float
        Raw accelerometer Z readings in m/s².  Gravity (~9.81) should already
        be subtracted on-device so 0.0 = no movement.
    timestamps_ms: list of int
        Millisecond timestamps matching each z_values entry.  Used to verify
        sample rate and detect dropped samples.
    """
    z_values: list[float] = Field(..., min_length=50)
    timestamps_ms: list[int] = Field(..., min_length=50)


class PotholeEventIn(BaseModel):
    """
    A single pothole detection event POSTed by the mobile app.

    device_id: str
        Stable anonymous device identifier (generated once on install, stored
        in Expo SecureStore).  Used for deduplication — we only count one hit
        per device per pothole per 24 h window.
    latitude / longitude: float
        WGS-84 GPS coordinates at moment of detection.
    detected_at: datetime
        ISO-8601 UTC timestamp from the phone.  May differ from server
        received_at if the event was queued offline.
    accel_burst: AccelBurst
        Raw sensor data used for server-side FFT severity scoring.
    app_version: str
        Semver string from the Expo app.  Used to ignore events from outdated
        clients that may have buggy sensor handling.
    """
    device_id: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    detected_at: datetime
    accel_burst: AccelBurst
    app_version: Optional[str] = None


class PotholeRecord(BaseModel):
    """
    A canonical pothole record as stored in Supabase and returned to the
    city dashboard.  One record aggregates many individual PotholeEventIn hits
    that fell within CLUSTER_RADIUS_M of each other.

    pothole_id: str (UUID)
    canonical_lat / canonical_lng: float
        Centroid of all clustered GPS hits.
    severity_score: float  [0.0 – 1.0]
        Average FFT severity across all hits for this pothole.
    hit_count: int
        Total confirmed device hits (deduplicated per device/day).
    priority_score: float
        = severity_score × hit_count × traffic_weight
        Dashboard sorts by this descending.
    first_seen / last_seen: datetime
    traffic_weight: float
        Multiplier representing estimated vehicles/day on this road segment.
        Defaults to 1.0 until city GIS data is integrated.
    """
    pothole_id: str
    canonical_lat: float
    canonical_lng: float
    severity_score: float
    hit_count: int
    priority_score: float
    first_seen: datetime
    last_seen: datetime
    traffic_weight: float = 1.0


# =============================================================================
# CORE LOGIC — FFT SEVERITY SCORING
# =============================================================================

def score_severity(burst: AccelBurst) -> float:
    """
    Converts a raw Z-axis accelerometer burst into a [0, 1] severity score
    using Fast Fourier Transform (FFT) frequency analysis.

    WHY FFT?
    --------
    A pothole produces a characteristic waveform:
        1. Sharp negative spike  (front wheel drops in)
        2. Positive impact spike (wheel hits the far edge / bottom)
        3. Dampened oscillation  (suspension bounce, ~8–15 Hz for most cars)

    Simple peak-to-peak amplitude alone is noisy — speed bumps, kerbs, and
    rough tarmac all create large spikes.  The FFT reveals *how much energy*
    sits in the 8–20 Hz "pothole band" relative to the total signal energy,
    which is a much more reliable discriminator.

    ALGORITHM:
    ----------
    1. Convert z_values to a numpy array and remove DC offset (mean subtraction).
    2. Apply a Hann window to reduce spectral leakage at the array boundaries.
    3. Compute FFT magnitude spectrum.
    4. Sum energy in the pothole band (POTHOLE_FREQ_LOW – POTHOLE_FREQ_HIGH Hz).
    5. Divide by total signal energy → band energy ratio in [0, 1].
    6. Multiply by a peak amplitude factor so large impacts score higher.

    RETURN:
    -------
    float in [0.0, 1.0]
        0.0 = smooth road / false positive
        1.0 = severe pothole

    NOTE FOR AI AGENTS:
    -------------------
    If you change SAMPLE_RATE_HZ here, update the matching constant in the
    Expo mobile app (DeviceMotion subscription interval).  Mismatch will make
    all frequency bins wrong.
    """

    POTHOLE_FREQ_LOW  = 8.0   # Hz — lower bound of pothole oscillation band
    POTHOLE_FREQ_HIGH = 20.0  # Hz — upper bound; above this is tyre noise

    z = np.array(burst.z_values, dtype=np.float64)

    # --- 1. Remove DC offset (gravity residual, phone tilt baseline) ----------
    z -= z.mean()

    # --- 2. Hann window — reduces spectral leakage ----------------------------
    window = np.hanning(len(z))
    z_windowed = z * window

    # --- 3. FFT magnitude spectrum --------------------------------------------
    spectrum = np.abs(fft(z_windowed))
    freqs    = fftfreq(len(z), d=1.0 / SAMPLE_RATE_HZ)

    # Only positive frequencies (spectrum is symmetric for real input)
    pos_mask = freqs > 0
    spectrum = spectrum[pos_mask]
    freqs    = freqs[pos_mask]

    # --- 4. Band energy ratio -------------------------------------------------
    pothole_band = (freqs >= POTHOLE_FREQ_LOW) & (freqs <= POTHOLE_FREQ_HIGH)
    band_energy  = np.sum(spectrum[pothole_band] ** 2)
    total_energy = np.sum(spectrum ** 2)

    if total_energy == 0:
        return 0.0  # flat signal — no movement detected

    band_ratio = band_energy / total_energy  # 0 → 1

    # --- 5. Peak amplitude factor (0 → 1, saturates at 4 × gravity) ----------
    peak_amplitude = np.max(np.abs(z))
    amp_factor = min(peak_amplitude / (4 * 9.81), 1.0)

    # --- 6. Combined score — geometric mean weights both dimensions equally ---
    severity = float(np.sqrt(band_ratio * amp_factor))
    return round(min(severity, 1.0), 4)


# =============================================================================
# CORE LOGIC — GPS CLUSTERING
# =============================================================================

def find_nearby_pothole(lat: float, lng: float) -> Optional[str]:
    """
    Queries Supabase/PostGIS for an existing pothole record within
    CLUSTER_RADIUS_M metres of the given coordinates.

    Uses PostGIS ST_DWithin for efficient spatial lookup.  The `potholes`
    table must have a `location` geography column with a GIST index:

        ALTER TABLE potholes ADD COLUMN location geography(Point, 4326);
        CREATE INDEX idx_potholes_location ON potholes USING GIST(location);

    RETURN:
    -------
    str (UUID) of the matching pothole, or None if no nearby record exists.

    NOTE FOR AI AGENTS:
    -------------------
    ST_DWithin on a geography column works in metres, which is what we want.
    Do NOT use ST_DWithin on a geometry column — that works in degrees and the
    CLUSTER_RADIUS_M value would be meaningless.
    """

    # PostGIS RPC — this SQL function must exist in Supabase (see migrations/)
    # Function signature:  find_nearby_pothole(lat float8, lng float8, radius float8)
    #                      RETURNS TABLE(pothole_id uuid)
    response = supabase.rpc(
        "find_nearby_pothole",
        {"lat": lat, "lng": lng, "radius": CLUSTER_RADIUS_M},
    ).execute()

    if response.data:
        return response.data[0]["pothole_id"]
    return None


def upsert_pothole(
        pothole_id: Optional[str],
        lat: float,
        lng: float,
        severity: float,
        device_id: str,
        detected_at: datetime,
) -> str:
    """
    Either creates a new pothole record or updates an existing one.

    CREATE path (pothole_id is None):
        - Insert a new row into `potholes` with canonical_lat/lng = given coords,
          severity_score = severity, hit_count = 1, traffic_weight = 1.0.

    UPDATE path (pothole_id is not None):
        - Recalculate canonical_lat/lng as the centroid of all hits (via PostGIS
          ST_Centroid — handled by a DB trigger, not here).
        - Increment hit_count.
        - Recalculate severity_score as rolling average:
              new_avg = (old_avg × old_count + new_severity) / (old_count + 1)
        - Update last_seen timestamp.
        - Recompute priority_score = severity_score × hit_count × traffic_weight.

    DEDUPLICATION:
        Before calling this function the caller should check the `events` table
        to ensure this device_id has not already been counted for this pothole
        in the last 24 hours.  See ingest_event() below.

    RETURN:
    -------
    str — UUID of the created or updated pothole record.

    NOTE FOR AI AGENTS:
    -------------------
    The priority_score recomputation intentionally happens in the database via
    a GENERATED ALWAYS column or trigger, not in Python, to keep the value
    consistent even if rows are manually edited for testing.
    """

    now = datetime.now(timezone.utc).isoformat()

    if pothole_id is None:
        # --- CREATE new pothole record ----------------------------------------
        result = supabase.table("potholes").insert({
            "canonical_lat":   lat,
            "canonical_lng":   lng,
            "severity_score":  severity,
            "hit_count":       1,
            "traffic_weight":  1.0,         # default; city GIS integration TBD
            "first_seen":      detected_at.isoformat(),
            "last_seen":       detected_at.isoformat(),
        }).execute()
        return result.data[0]["pothole_id"]

    else:
        # --- UPDATE existing pothole record ------------------------------------
        # Fetch current stats so we can recalculate the rolling average
        current = (
            supabase.table("potholes")
            .select("severity_score, hit_count")
            .eq("pothole_id", pothole_id)
            .single()
            .execute()
        )
        old_severity = current.data["severity_score"]
        old_count    = current.data["hit_count"]

        # Rolling average severity
        new_count    = old_count + 1
        new_severity = round(
            (old_severity * old_count + severity) / new_count, 4
        )

        supabase.table("potholes").update({
            "severity_score": new_severity,
            "hit_count":      new_count,
            "last_seen":      detected_at.isoformat(),
            # priority_score is recomputed by DB trigger (see migrations/)
        }).eq("pothole_id", pothole_id).execute()

        return pothole_id


# =============================================================================
# API ROUTES
# =============================================================================

@app.get("/health")
def health_check():
    """
    Simple liveness probe.
    Railway and any uptime monitors hit this endpoint.
    Returns 200 OK with a timestamp so you can tell the response is fresh.
    """
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/events", status_code=202)
async def ingest_event(event: PotholeEventIn, background_tasks: BackgroundTasks):
    """
    PRIMARY INGEST ENDPOINT — called by the mobile app after it detects a pothole.

    The mobile app POSTs here for each event that was queued in SQLite while
    offline.  A single sync batch may POST many events in rapid succession.

    HTTP 202 (Accepted) is returned immediately; the heavy FFT + DB work runs
    in a FastAPI BackgroundTask so the phone's sync doesn't time out.

    FLOW:
    -----
    1. Validate request body (Pydantic does this automatically).
    2. Schedule process_event() as a background task.
    3. Return 202 immediately.

    The mobile app treats 202 as "received" and removes the event from its
    local SQLite queue.  If we returned 5xx the app would retry later.

    NOTE FOR AI AGENTS:
    -------------------
    If you need to add request authentication (API key, JWT), add it here as a
    FastAPI Dependency before the background task is scheduled.  Do NOT add
    auth inside process_event() — that function has no access to request headers.
    """
    background_tasks.add_task(process_event, event)
    return {"accepted": True, "queued_at": datetime.now(timezone.utc).isoformat()}


def process_event(event: PotholeEventIn) -> None:
    """
    Background worker — runs after ingest_event() returns 202.

    STEPS:
    ------
    1. Score severity via FFT.
    2. Deduplicate — check if this device already reported this pothole today.
    3. Find or create a pothole cluster via PostGIS.
    4. Upsert the pothole record.
    5. Log the raw event to the `events` table for audit / replay purposes.

    ERROR HANDLING:
    ---------------
    Exceptions are logged but not re-raised.  If processing fails, the event
    is NOT re-queued here — the mobile app already removed it from SQLite after
    receiving the 202.  For production you should push failed events to a dead-
    letter queue (e.g. a Supabase `failed_events` table) for manual replay.
    """
    try:
        # --- Step 1: FFT severity score ---------------------------------------
        severity = score_severity(event.accel_burst)
        logger.info(
            f"Event from device {event.device_id[:8]}… "
            f"@ ({event.latitude:.5f}, {event.longitude:.5f}) "
            f"severity={severity}"
        )

        # --- Step 2: Deduplication --------------------------------------------
        # Prevents a single device hitting the same pothole 3× in one commute
        # from inflating hit_count artificially.
        # Check: has this device reported any pothole within CLUSTER_RADIUS_M
        #        in the last 24 hours?
        existing_event = (
            supabase.table("events")
            .select("event_id")
            .eq("device_id", event.device_id)
            .gte("detected_at", _yesterday_iso())
            .execute()
        )
        # NOTE: a proper spatial dedup would also check distance; this is a
        # simplified version that prevents any double-count from same device/day.
        # TODO: add ST_DWithin check against recent events from this device.
        if existing_event.data:
            logger.info(f"Duplicate — device {event.device_id[:8]}… already reported today")
            return

        # --- Step 3: GPS clustering -------------------------------------------
        pothole_id = find_nearby_pothole(event.latitude, event.longitude)

        # --- Step 4: Upsert pothole record ------------------------------------
        pothole_id = upsert_pothole(
            pothole_id=pothole_id,
            lat=event.latitude,
            lng=event.longitude,
            severity=severity,
            device_id=event.device_id,
            detected_at=event.detected_at,
        )

        # --- Step 5: Log raw event for audit trail ----------------------------
        supabase.table("events").insert({
            "device_id":   event.device_id,
            "pothole_id":  pothole_id,
            "latitude":    event.latitude,
            "longitude":   event.longitude,
            "severity":    severity,
            "detected_at": event.detected_at.isoformat(),
            "app_version": event.app_version,
            # Raw accel burst is NOT stored long-term — too large.
            # Severity score is the derived artefact we keep.
        }).execute()

        logger.info(f"Processed event → pothole {pothole_id}")

    except Exception as exc:
        # Log and swallow — background tasks must not crash the worker process
        logger.exception(f"Failed to process event: {exc}")


@app.get("/potholes", response_model=list[PotholeRecord])
def get_potholes(
        min_priority: float = 0.0,
        limit: int = 200,
        offset: int = 0,
):
    """
    DASHBOARD READ ENDPOINT — called by the Next.js city dashboard.

    Returns pothole records sorted by priority_score descending (worst first).
    The dashboard uses this to:
        - Plot priority pins on the Mapbox GL JS map layer
        - Populate the "worst offenders" leaderboard (Recharts)

    QUERY PARAMS:
    -------------
    min_priority : float  — filter out low-priority noise (default 0 = show all)
    limit        : int    — page size (max 500 to avoid huge payloads)
    offset       : int    — pagination offset

    NOTE FOR AI AGENTS:
    -------------------
    For the live dashboard (Supabase Realtime), the Next.js app subscribes
    directly to the `potholes` table via the Supabase JS client.  This REST
    endpoint is used for initial page load and for the leaderboard table only.
    The Mapbox heatmap layer uses the GeoJSON endpoint below.
    """
    limit = min(limit, 500)  # hard cap — protect against scraping

    result = (
        supabase.table("potholes")
        .select("*")
        .gte("priority_score", min_priority)
        .order("priority_score", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )

    return result.data


@app.get("/potholes/geojson")
def get_potholes_geojson(min_priority: float = 0.0):
    """
    Returns all potholes as a GeoJSON FeatureCollection.

    Mapbox GL JS consumes this directly as a vector source for the heatmap
    and priority-pin layers on the city dashboard.

    Each Feature's `properties` include:
        - pothole_id, severity_score, hit_count, priority_score, last_seen

    The `geometry` is a GeoJSON Point [longitude, latitude].

    NOTE FOR AI AGENTS:
    -------------------
    GeoJSON spec requires [longitude, latitude] order (x, y), NOT [lat, lng].
    Mapbox expects this order.  Do not swap them.
    """
    result = (
        supabase.table("potholes")
        .select(
            "pothole_id, canonical_lat, canonical_lng, "
            "severity_score, hit_count, priority_score, last_seen"
        )
        .gte("priority_score", min_priority)
        .execute()
    )

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [row["canonical_lng"], row["canonical_lat"]],  # [lng, lat] — GeoJSON spec
            },
            "properties": {
                "pothole_id":     row["pothole_id"],
                "severity_score": row["severity_score"],
                "hit_count":      row["hit_count"],
                "priority_score": row["priority_score"],
                "last_seen":      row["last_seen"],
            },
        }
        for row in result.data
    ]

    return {"type": "FeatureCollection", "features": features}


# =============================================================================
# HELPERS
# =============================================================================

def _yesterday_iso() -> str:
    """Returns an ISO-8601 UTC timestamp for exactly 24 hours ago."""
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()


# =============================================================================
# ENTRY POINT (local development only)
# =============================================================================
# In production, Railway runs:  uvicorn pothole_backend:app --host 0.0.0.0 --port $PORT
# Locally:  python pothole_backend.py
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "pothole_backend:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,   # hot-reload on file changes during development
    )


# =============================================================================
# DATABASE SCHEMA REFERENCE
# =============================================================================
# The following SQL should be run as a migration in Supabase (Dashboard → SQL editor).
# Keeping it here as a comment means this file is a single source of truth for
# AI agents that need to understand the full system without opening Supabase.
#
# -- Enable PostGIS extension (do this once per project)
# CREATE EXTENSION IF NOT EXISTS postgis;
#
# -- Raw events — one row per mobile detection, kept for audit / replay
# CREATE TABLE events (
#     event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#     device_id   TEXT        NOT NULL,
#     pothole_id  UUID        REFERENCES potholes(pothole_id) ON DELETE SET NULL,
#     latitude    FLOAT8      NOT NULL,
#     longitude   FLOAT8      NOT NULL,
#     severity    FLOAT4      NOT NULL CHECK (severity BETWEEN 0 AND 1),
#     detected_at TIMESTAMPTZ NOT NULL,
#     received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
#     app_version TEXT
# );
# CREATE INDEX idx_events_pothole    ON events(pothole_id);
# CREATE INDEX idx_events_device_day ON events(device_id, detected_at);
#
# -- Canonical potholes — one row per clustered location
# CREATE TABLE potholes (
#     pothole_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#     canonical_lat   FLOAT8      NOT NULL,
#     canonical_lng   FLOAT8      NOT NULL,
#     location        GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (
#                         ST_SetSRID(ST_MakePoint(canonical_lng, canonical_lat), 4326)::geography
#                     ) STORED,
#     severity_score  FLOAT4      NOT NULL DEFAULT 0,
#     hit_count       INT         NOT NULL DEFAULT 0,
#     traffic_weight  FLOAT4      NOT NULL DEFAULT 1.0,
#     priority_score  FLOAT4 GENERATED ALWAYS AS
#                         (severity_score * hit_count * traffic_weight) STORED,
#     first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
#     last_seen       TIMESTAMPTZ NOT NULL DEFAULT now()
# );
# CREATE INDEX idx_potholes_location  ON potholes USING GIST(location);
# CREATE INDEX idx_potholes_priority  ON potholes(priority_score DESC);
#
# -- PostGIS RPC used by find_nearby_pothole()
# CREATE OR REPLACE FUNCTION find_nearby_pothole(lat float8, lng float8, radius float8)
# RETURNS TABLE(pothole_id uuid) AS $$
#     SELECT pothole_id FROM potholes
#     WHERE ST_DWithin(
#         location,
#         ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
#         radius
#     )
#     ORDER BY ST_Distance(location, ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography)
#     LIMIT 1;
# $$ LANGUAGE sql STABLE;
#
# =============================================================================
# ENVIRONMENT VARIABLES REFERENCE (.env.example)
# =============================================================================
# SUPABASE_URL=https://your-project.supabase.co
# SUPABASE_SERVICE_KEY=eyJ...              # service role key from Supabase dashboard
# CLUSTER_RADIUS_M=5.0                     # metres — how close = same pothole
# SAMPLE_RATE_HZ=200                       # must match Expo app sensor rate
# PORT=8000                                # Railway sets this automatically
# =============================================================================
