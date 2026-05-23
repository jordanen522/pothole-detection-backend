"""
=============================================================================
POTHOLE DETECTION BACKEND — FastAPI Application  (v1.2)
=============================================================================
CHANGES IN v1.2:
    - Producer-consumer queue architecture (from v1.1) kept intact.
    - Added a full Monitoring Layer:
        /monitoring/live     – real-time snapshot (call every 5-10 s)
        /monitoring/history  – last 24 h at 15-minute granularity
    - Metrics collected:
        • CPU %  (psutil)
        • RSS memory MB / %  (psutil)
        • Postgres DB size MB  (RPC: get_db_size_mb)
        • event_queue pending + dead-letter counts
        • events processed / failed / rejected per 15-min window
        • pings/sec (rolling 60-second window)
    - Background task: MonitoringSnapshotWorker
        Fires every 15 minutes, writes to monitoring_snapshots, then purges
        rows older than 24 hours.
=============================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import os
import json
import time
import logging
import asyncio
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Optional

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import psutil
import numpy as np
from scipy.fft import fft, fftfreq
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from supabase import create_client, Client
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("pothole-backend")

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]

CLUSTER_RADIUS_M: float     = float(os.getenv("CLUSTER_RADIUS_M", "5.0"))
SAMPLE_RATE_HZ: int         = int(os.getenv("SAMPLE_RATE_HZ", "200"))
SNAPSHOT_INTERVAL_S: int    = int(os.getenv("SNAPSHOT_INTERVAL_S", "900"))   # 15 min
SNAPSHOT_RETENTION_H: int   = int(os.getenv("SNAPSHOT_RETENTION_H", "24"))

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# Process handle (for psutil metrics)
# ---------------------------------------------------------------------------
_proc = psutil.Process(os.getpid())


# =============================================================================
# MONITORING STATE  (in-process counters, no external dependency)
# =============================================================================

class MonitoringState:
    """
    Thread/async-safe counters for the current 15-minute monitoring window.
    All mutations go through asyncio.Lock so they are safe inside the event loop.
    psutil calls are cheap and lock-free.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        # Rolling request timestamps (last 60 s) for pings/sec calculation
        self._request_times: deque[float] = deque()

        # 15-minute window counters (reset after every snapshot)
        self._events_processed: int = 0
        self._events_failed: int    = 0
        self._events_rejected: int  = 0
        self._window_requests: int  = 0   # total requests in current window

    # ------------------------------------------------------------------
    # Mutations (must be awaited)
    # ------------------------------------------------------------------

    async def record_request(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._request_times.append(now)
            self._window_requests += 1
            # Prune timestamps older than 60 s
            cutoff = now - 60.0
            while self._request_times and self._request_times[0] < cutoff:
                self._request_times.popleft()

    async def record_processed(self) -> None:
        async with self._lock:
            self._events_processed += 1

    async def record_failed(self) -> None:
        async with self._lock:
            self._events_failed += 1

    async def record_rejected(self) -> None:
        async with self._lock:
            self._events_rejected += 1

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def pings_per_second(self) -> float:
        """Compute rate over the most recent 60-second sliding window."""
        now = time.monotonic()
        cutoff = now - 60.0
        recent = sum(1 for t in self._request_times if t >= cutoff)
        return round(recent / 60.0, 3)

    async def snapshot_and_reset(self) -> dict:
        """
        Return a dict of the current window's counters and reset them.
        Called by the snapshot worker.
        """
        async with self._lock:
            data = {
                "events_processed": self._events_processed,
                "events_failed":    self._events_failed,
                "events_rejected":  self._events_rejected,
                "avg_pings_per_sec": self.pings_per_second(),
            }
            # Reset window counters
            self._events_processed = 0
            self._events_failed    = 0
            self._events_rejected  = 0
            self._window_requests  = 0
            return data


monitoring = MonitoringState()


# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="Pothole Detection API",
    description=(
        "Ingests sensor events, queues them, scores severity via FFT, "
        "clusters GPS hits, and exposes a real-time monitoring layer."
    ),
    version="1.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------
# Middleware: count every HTTP hit for pings/sec
# ------------------------------------------------------------------
@app.middleware("http")
async def count_requests(request: Request, call_next):
    await monitoring.record_request()
    response = await call_next(request)
    return response


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class AccelBurst(BaseModel):
    z_values:      list[float] = Field(..., min_length=50)
    timestamps_ms: list[int]   = Field(..., min_length=50)


class PotholeEventIn(BaseModel):
    device_id:   str
    latitude:    float    = Field(..., ge=-90,  le=90)
    longitude:   float    = Field(..., ge=-180, le=180)
    detected_at: datetime
    accel_burst: AccelBurst
    app_version: Optional[str] = None


class PotholeRecord(BaseModel):
    pothole_id:    str
    canonical_lat: float
    canonical_lng: float
    severity_score: float
    hit_count:     int
    priority_score: float
    first_seen:    datetime
    last_seen:     datetime
    traffic_weight: float = 1.0


class LiveMonitoringSnapshot(BaseModel):
    captured_at:       str
    # System
    cpu_percent:       float
    memory_used_mb:    float
    memory_percent:    float
    db_size_mb:        float
    # Queue
    queue_pending:     int
    queue_dead_letter: int
    # Throughput (rolling window since last snapshot)
    events_processed:  int
    events_failed:     int
    events_rejected:   int
    # Rate
    pings_per_sec:     float


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

    window     = np.hanning(len(z))
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

    band_ratio    = band_energy / total_energy
    peak_amplitude = np.max(np.abs(z))
    amp_factor    = min(peak_amplitude / (4 * 9.81), 1.0)

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
        pothole_id:  Optional[str],
        lat:         float,
        lng:         float,
        severity:    float,
        device_id:   str,
        detected_at: datetime,
) -> str:
    if pothole_id is None:
        result = supabase.table("potholes").insert({
            "canonical_lat":  lat,
            "canonical_lng":  lng,
            "severity_score": severity,
            "hit_count":      1,
            "traffic_weight": 1.0,
            "first_seen":     detected_at.isoformat(),
            "last_seen":      detected_at.isoformat(),
        }).execute()
        return result.data[0]["pothole_id"]

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
    new_severity = round((old_severity * old_count + severity) / new_count, 4)

    supabase.table("potholes").update({
        "severity_score": new_severity,
        "hit_count":      new_count,
        "last_seen":      detected_at.isoformat(),
    }).eq("pothole_id", pothole_id).execute()

    return pothole_id


# =============================================================================
# MONITORING HELPERS — SYSTEM RESOURCES
# =============================================================================

def _cpu_percent() -> float:
    """Non-blocking; returns cached value from the last interval call."""
    return _proc.cpu_percent(interval=None)


def _memory_stats() -> tuple[float, float]:
    """Returns (rss_mb, percent)."""
    mem = _proc.memory_info()
    rss_mb = mem.rss / (1024 * 1024)
    # System-wide percentage this process consumes
    sys_total = psutil.virtual_memory().total
    pct = (mem.rss / sys_total) * 100.0
    return round(rss_mb, 2), round(pct, 2)


def _db_size_mb() -> float:
    try:
        resp = supabase.rpc("get_db_size_mb", {}).execute()
        return round(resp.data or 0.0, 2)
    except Exception as exc:
        logger.warning(f"Could not fetch DB size: {exc}")
        return 0.0


def _queue_counts() -> tuple[int, int]:
    """Returns (pending, dead_letter)."""
    try:
        resp = supabase.rpc("get_queue_counts", {}).execute()
        counts = {row["status"]: row["cnt"] for row in (resp.data or [])}
        return int(counts.get("pending", 0)), int(counts.get("dead_letter", 0))
    except Exception as exc:
        logger.warning(f"Could not fetch queue counts: {exc}")
        return 0, 0


# =============================================================================
# API ROUTES — CORE
# =============================================================================

@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/events", status_code=202)
async def ingest_event(event: PotholeEventIn):
    """
    PRODUCER ENDPOINT.
    Instantly writes the payload to event_queue. Zero processing.
    Validation failures (Pydantic) are automatically rejected with 422;
    the middleware still counts the ping, and we note the rejection.
    """
    try:
        supabase.table("event_queue").insert({
            "device_id": event.device_id,
            "payload":   json.loads(event.model_dump_json()),
            "status":    "pending",
        }).execute()
    except Exception as exc:
        await monitoring.record_rejected()
        logger.error(f"Failed to queue event from {event.device_id[:8]}: {exc}")
        raise

    return {"accepted": True, "queued_at": datetime.now(timezone.utc).isoformat()}


# =============================================================================
# WORKER / CONSUMER LOGIC
# =============================================================================

def process_event_math(event: PotholeEventIn) -> None:
    """
    Synchronous processing: FFT scoring + GPS clustering + DB upsert.
    Raises on failure so the worker can apply DLQ logic.
    """
    severity = score_severity(event.accel_burst)

    # Duplicate guard: same device within the last 24 h
    existing = (
        supabase.table("events")
        .select("event_id")
        .eq("device_id", event.device_id)
        .gte("detected_at", _yesterday_iso())
        .execute()
    )
    if existing.data:
        logger.info(f"Duplicate — device {event.device_id[:8]} already reported today.")
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
    BACKGROUND CONSUMER — DLQ pattern.
    Polls pending jobs in batches of 50.
    Success  → delete row.
    Failure  → increment retry_count; store error_msg.
    3 fails  → mark as dead_letter.
    """
    logger.info("Queue worker started.")
    # Warm up psutil CPU measurement (first call always returns 0.0)
    _proc.cpu_percent(interval=None)

    while True:
        try:
            response = (
                supabase.table("event_queue")
                .select("*")
                .eq("status", "pending")
                .order("created_at")
                .limit(50)
                .execute()
            )
            jobs = response.data
            if not jobs:
                await asyncio.sleep(5)
                continue

            for job in jobs:
                job_id = job["id"]
                try:
                    event_data = PotholeEventIn(**job["payload"])
                    process_event_math(event_data)

                    supabase.table("event_queue").delete().eq("id", job_id).execute()
                    await monitoring.record_processed()

                except Exception as err:
                    new_retries = job["retry_count"] + 1
                    logger.error(f"Job {job_id} failed (attempt {new_retries}): {err}")
                    await monitoring.record_failed()

                    if new_retries >= 3:
                        supabase.table("event_queue").update({
                            "status":    "dead_letter",
                            "error_msg": str(err)[:500],
                        }).eq("id", job_id).execute()
                        logger.error(f"Job {job_id} → DLQ.")
                    else:
                        supabase.table("event_queue").update({
                            "retry_count": new_retries,
                            "error_msg":   str(err)[:500],
                        }).eq("id", job_id).execute()

        except Exception as poll_err:
            logger.error(f"Queue worker polling error: {poll_err}")
            await asyncio.sleep(5)


# =============================================================================
# MONITORING SNAPSHOT WORKER
# =============================================================================

async def monitoring_snapshot_worker():
    """
    Fires every SNAPSHOT_INTERVAL_S seconds (default 15 min).
    Collects system + queue + throughput metrics, writes to
    monitoring_snapshots, then purges rows older than 24 h.
    """
    logger.info(f"Monitoring snapshot worker started (interval={SNAPSHOT_INTERVAL_S}s).")
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL_S)
        try:
            rss_mb, mem_pct = _memory_stats()
            cpu              = _cpu_percent()
            db_mb            = _db_size_mb()
            pending, dlq     = _queue_counts()
            window           = await monitoring.snapshot_and_reset()

            supabase.table("monitoring_snapshots").insert({
                "cpu_percent":       cpu,
                "memory_used_mb":    rss_mb,
                "memory_percent":    mem_pct,
                "db_size_mb":        db_mb,
                "queue_pending":     pending,
                "queue_dead_letter": dlq,
                "events_processed":  window["events_processed"],
                "events_failed":     window["events_failed"],
                "events_rejected":   window["events_rejected"],
                "avg_pings_per_sec": window["avg_pings_per_sec"],
            }).execute()

            # Purge snapshots older than 24 h
            supabase.rpc("purge_old_snapshots", {}).execute()

            logger.info(
                f"[Monitoring] CPU={cpu}%  MEM={rss_mb}MB  "
                f"DB={db_mb}MB  pending={pending}  dlq={dlq}  "
                f"processed={window['events_processed']}  "
                f"failed={window['events_failed']}  "
                f"rejected={window['events_rejected']}  "
                f"pings/s={window['avg_pings_per_sec']}"
            )
        except Exception as exc:
            logger.error(f"Monitoring snapshot failed: {exc}")


# =============================================================================
# MONITORING API ENDPOINTS
# =============================================================================

@app.get("/monitoring/live", response_model=LiveMonitoringSnapshot)
async def live_monitoring():
    """
    Returns a real-time snapshot of system health.
    Intended to be called every 5-10 seconds by the dashboard.
    Does NOT reset any counters — use /monitoring/history for trend data.
    """
    rss_mb, mem_pct   = _memory_stats()
    cpu                = _cpu_percent()
    db_mb              = _db_size_mb()
    pending, dlq       = _queue_counts()

    async with monitoring._lock:
        processed = monitoring._events_processed
        failed    = monitoring._events_failed
        rejected  = monitoring._events_rejected
        pps       = monitoring.pings_per_second()

    return LiveMonitoringSnapshot(
        captured_at        = datetime.now(timezone.utc).isoformat(),
        cpu_percent        = cpu,
        memory_used_mb     = rss_mb,
        memory_percent     = mem_pct,
        db_size_mb         = db_mb,
        queue_pending      = pending,
        queue_dead_letter  = dlq,
        events_processed   = processed,
        events_failed      = failed,
        events_rejected    = rejected,
        pings_per_sec      = pps,
    )


@app.get("/monitoring/history")
def monitoring_history(hours: int = 24):
    """
    Returns up to `hours` hours of 15-minute monitoring snapshots (default 24 h).
    Ordered oldest → newest so charts can be rendered directly.
    """
    hours = min(max(hours, 1), 24)
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    result = (
        supabase.table("monitoring_snapshots")
        .select(
            "captured_at, cpu_percent, memory_used_mb, memory_percent, "
            "db_size_mb, queue_pending, queue_dead_letter, "
            "events_processed, events_failed, events_rejected, avg_pings_per_sec"
        )
        .gte("captured_at", since)
        .order("captured_at", desc=False)
        .execute()
    )

    return {
        "period_hours": hours,
        "snapshot_count": len(result.data),
        "snapshots": result.data,
    }


@app.get("/monitoring/dlq")
def get_dlq(limit: int = 50):
    """
    Returns dead-letter queue items so engineers can inspect failures.
    """
    limit = min(limit, 200)
    result = (
        supabase.table("event_queue")
        .select("id, device_id, retry_count, error_msg, created_at, updated_at")
        .eq("status", "dead_letter")
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return {"dead_letter_count": len(result.data), "items": result.data}


# =============================================================================
# DASHBOARD READ ENDPOINTS
# =============================================================================

@app.get("/potholes", response_model=list[PotholeRecord])
def get_potholes(min_priority: float = 0.0, limit: int = 200, offset: int = 0):
    limit  = min(limit, 500)
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
# STARTUP
# =============================================================================

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(queue_worker())
    asyncio.create_task(monitoring_snapshot_worker())
    logger.info("Pothole backend v1.2 started — queue worker + monitoring snapshot worker running.")


# =============================================================================
# HELPERS
# =============================================================================

def _yesterday_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "pothole_backend:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )
