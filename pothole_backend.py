"""
=============================================================================
POTHOLE DETECTION BACKEND — FastAPI Application
=============================================================================
REPO PURPOSE:
    This is the backend service for the Pothole Detection mobile app.
    It is intentionally separate from the mobile app (Expo/React Native) and
    the city dashboard (Next.js). This repo handles ONLY:
        1. Receiving pothole event data from mobile clients
        2. Queuing events in a Postgres-backed Queue (for resilience)
        3. Running FFT-based severity scoring on accelerometer bursts
        4. Clustering nearby GPS hits into a single canonical "pothole" record
        5. Serving aggregated pothole data to the city dashboard

ARCHITECTURE UPDATE (Producer-Consumer Queue):
    Events are no longer processed in memory via FastAPI BackgroundTasks.
    Instead, POST /events writes the raw JSON payload to Supabase (`event_queue`).
    An asyncio worker loop (`queue_worker`) constantly polls batches of 50,
    processes the FFT math, and either deletes the row (success) or increments
    a retry counter. After 3 failures, it moves to a Dead Letter Queue (DLQ).

SISTER REPOS:
    - Mobile App  : Expo (React Native) — sensor capture, SQLite queue, sync
    - Dashboard   : Next.js + Mapbox GL JS — city-facing map and leaderboard

=============================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import numpy as np
from scipy.signal import find_peaks
from scipy.fft import fft, fftfreq
from shapely.geometry import Point
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from supabase import create_client, Client
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pothole-backend")

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]

CLUSTER_RADIUS_M: float = float(os.getenv("CLUSTER_RADIUS_M", "5.0"))
SAMPLE_RATE_HZ: int = int(os.getenv("SAMPLE_RATE_HZ", "200"))

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# FastAPI app instance
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Pothole Detection API",
    description="Ingests sensor events, queues them, scores severity via FFT, and clusters GPS hits.",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class AccelBurst(BaseModel):
    z_values: list[float] = Field(..., min_length=50)
    timestamps_ms: list[int] = Field(..., min_length=50)


class PotholeEventIn(BaseModel):
    device_id: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    detected_at: datetime
    accel_burst: AccelBurst
    app_version: Optional[str] = None


class PotholeRecord(BaseModel):
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
    AI AGENT INSTRUCTIONS:
    Do not modify this math without testing. This uses an FFT to isolate the
    8-20 Hz frequency band (suspension bounce signature).
    """
    POTHOLE_FREQ_LOW  = 8.0
    POTHOLE_FREQ_HIGH = 20.0

    z = np.array(burst.z_values, dtype=np.float64)
    z -= z.mean()

    window = np.hanning(len(z))
    z_windowed = z * window

    spectrum = np.abs(fft(z_windowed))
    freqs    = fftfreq(len(z), d=1.0 / SAMPLE_RATE_HZ)

    pos_mask = freqs > 0
    spectrum = spectrum[pos_mask]
    freqs    = freqs[pos_mask]

    pothole_band = (freqs >= POTHOLE_FREQ_LOW) & (freqs <= POTHOLE_FREQ_HIGH)
    band_energy  = np.sum(spectrum[pothole_band] ** 2)
    total_energy = np.sum(spectrum ** 2)

    if total_energy == 0:
        return 0.0

    band_ratio = band_energy / total_energy

    peak_amplitude = np.max(np.abs(z))
    amp_factor = min(peak_amplitude / (4 * 9.81), 1.0)

    severity = float(np.sqrt(band_ratio * amp_factor))
    return round(min(severity, 1.0), 4)


# =============================================================================
# CORE LOGIC — GPS CLUSTERING
# =============================================================================

def find_nearby_pothole(lat: float, lng: float) -> Optional[str]:
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
    now = datetime.now(timezone.utc).isoformat()

    if pothole_id is None:
        result = supabase.table("potholes").insert({
            "canonical_lat":   lat,
            "canonical_lng":   lng,
            "severity_score":  severity,
            "hit_count":       1,
            "traffic_weight":  1.0,
            "first_seen":      detected_at.isoformat(),
            "last_seen":       detected_at.isoformat(),
        }).execute()
        return result.data[0]["pothole_id"]

    else:
        current = (
            supabase.table("potholes")
            .select("severity_score, hit_count")
            .eq("pothole_id", pothole_id)
            .single()
            .execute()
        )
        old_severity = current.data["severity_score"]
        old_count    = current.data["hit_count"]

        new_count    = old_count + 1
        new_severity = round(
            (old_severity * old_count + severity) / new_count, 4
        )

        supabase.table("potholes").update({
            "severity_score": new_severity,
            "hit_count":      new_count,
            "last_seen":      detected_at.isoformat(),
        }).eq("pothole_id", pothole_id).execute()

        return pothole_id


# =============================================================================
# API ROUTES
# =============================================================================

@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/events", status_code=202)
async def ingest_event(event: PotholeEventIn):
    """
    PRODUCER ENDPOINT.
    AI AGENT CONTEXT:
    This endpoint does ZERO processing. It instantly writes the Pydantic model
    as JSON into the Postgres `event_queue` table and returns a 202 to the phone.
    This prevents the mobile app's sync from timing out during high load.
    """
    # 1. Dump into Postgres Queue
    supabase.table("event_queue").insert({
        "device_id": event.device_id,
        # Convert Pydantic model to dict, serialize datetimes via jsonable_encoder
        # (Using .model_dump(mode='json') if pydantic v2, or fallback to .json())
        "payload": json.loads(event.json()),
        "status": "pending"
    }).execute()

    # 2. Return instantly
    return {"accepted": True, "queued_at": datetime.now(timezone.utc).isoformat()}


# =============================================================================
# WORKER / CONSUMER LOGIC (DEAD LETTER QUEUE PATTERN)
# =============================================================================

def process_event_math(event: PotholeEventIn) -> None:
    """
    Synchronous processing logic. Runs inside the async queue worker.
    Raises an exception if something goes wrong so the worker can trigger a retry.
    """
    severity = score_severity(event.accel_burst)

    existing_event = (
        supabase.table("events")
        .select("event_id")
        .eq("device_id", event.device_id)
        .gte("detected_at", _yesterday_iso())
        .execute()
    )
    if existing_event.data:
        logger.info(f"Duplicate — device {event.device_id[:8]} reported today. Skipping math.")
        return

    pothole_id = find_nearby_pothole(event.latitude, event.longitude)

    pothole_id = upsert_pothole(
        pothole_id=pothole_id,
        lat=event.latitude,
        lng=event.longitude,
        severity=severity,
        device_id=event.device_id,
        detected_at=event.detected_at,
    )

    supabase.table("events").insert({
        "device_id":   event.device_id,
        "pothole_id":  pothole_id,
        "latitude":    event.latitude,
        "longitude":   event.longitude,
        "severity":    severity,
        "detected_at": event.detected_at.isoformat(),
        "app_version": event.app_version,
    }).execute()


async def queue_worker():
    """
    BACKGROUND CONSUMER.
    AI AGENT CONTEXT:
    This loop polls the Postgres database for `pending` jobs in batches of 50.
    It implements the Dead Letter Queue (DLQ) pattern:
    - Success -> Delete row
    - Failure -> Increment `retry_count`
    - 3 Failures -> Mark status as `dead_letter` for engineers to investigate.
    """
    logger.info("Starting DLQ worker loop...")
    while True:
        try:
            # 1. Fetch a BATCH of pending items
            response = supabase.table("event_queue") \
                .select("*") \
                .eq("status", "pending") \
                .order("created_at") \
                .limit(50) \
                .execute()

            jobs = response.data
            if not jobs:
                await asyncio.sleep(5)  # Wait if queue is empty
                continue

            for job in jobs:
                job_id = job["id"]
                try:
                    # Parse the JSON payload back into our Pydantic model
                    event_data = PotholeEventIn(**job["payload"])

                    # Run the heavy math & DB upserts
                    process_event_math(event_data)

                    # Success: Remove from queue
                    supabase.table("event_queue").delete().eq("id", job_id).execute()

                except Exception as processing_error:
                    # Failure: Handle Retries / DLQ
                    new_retries = job["retry_count"] + 1
                    logger.error(f"Job {job_id} failed on try {new_retries}: {processing_error}")

                    if new_retries >= 3:
                        # Move to DLQ
                        supabase.table("event_queue").update({
                            "status": "dead_letter"
                        }).eq("id", job_id).execute()
                        logger.error(f"Job {job_id} permanently failed. Moved to DLQ.")
                    else:
                        # Re-queue
                        supabase.table("event_queue").update({
                            "retry_count": new_retries
                        }).eq("id", job_id).execute()

        except Exception as queue_error:
            # Catch DB polling errors to prevent the loop from dying
            logger.error(f"Queue worker polling crashed: {queue_error}")
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    """
    Fires when uvicorn starts. Kicks off the background polling loop.
    """
    asyncio.create_task(queue_worker())


# =============================================================================
# DASHBOARD READ ENDPOINTS
# =============================================================================

@app.get("/potholes", response_model=list[PotholeRecord])
def get_potholes(min_priority: float = 0.0, limit: int = 200, offset: int = 0):
    limit = min(limit, 500)
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
    result = (
        supabase.table("potholes")
        .select("pothole_id, canonical_lat, canonical_lng, severity_score, hit_count, priority_score, last_seen")
        .gte("priority_score", min_priority)
        .execute()
    )

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [row["canonical_lng"], row["canonical_lat"]],
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
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("pothole_backend:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)


# =============================================================================
# DATABASE SCHEMA REFERENCE (FOR AI AGENTS)
# =============================================================================
# -- ... (existing postgis, events, potholes, rpc logic) ...
#
# -- DLQ EVENT QUEUE (Added v1.1)
# CREATE TABLE event_queue (
#     id SERIAL PRIMARY KEY,
#     device_id TEXT NOT NULL,
#     payload JSONB NOT NULL,
#     status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'dead_letter')),
#     retry_count INT DEFAULT 0,
#     created_at TIMESTAMPTZ DEFAULT now()
# );
# CREATE INDEX idx_queue_status ON event_queue(status);
# =============================================================================