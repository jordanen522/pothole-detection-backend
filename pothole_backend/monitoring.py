import asyncio
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import psutil
from fastapi import APIRouter

from .config import supabase, SNAPSHOT_INTERVAL_S, logger
from .models import LiveMonitoringSnapshot

_proc = psutil.Process()

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


# ---------------------------------------------------------------------------
# In-process counters
# ---------------------------------------------------------------------------

class MonitoringState:
    def __init__(self) -> None:
        self._lock             = asyncio.Lock()
        self._request_times:   deque[float] = deque()
        self._events_processed = 0
        self._events_failed    = 0
        self._events_rejected  = 0
        self._window_requests  = 0

    async def record_request(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._request_times.append(now)
            self._window_requests += 1
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

    def pings_per_second(self) -> float:
        now    = time.monotonic()
        cutoff = now - 60.0
        recent = sum(1 for t in self._request_times if t >= cutoff)
        return round(recent / 60.0, 3)

    async def snapshot_and_reset(self) -> dict:
        async with self._lock:
            data = {
                "events_processed":  self._events_processed,
                "events_failed":     self._events_failed,
                "events_rejected":   self._events_rejected,
                "avg_pings_per_sec": self.pings_per_second(),
            }
            self._events_processed = 0
            self._events_failed    = 0
            self._events_rejected  = 0
            self._window_requests  = 0
            return data


monitoring = MonitoringState()


# ---------------------------------------------------------------------------
# System resource helpers
# ---------------------------------------------------------------------------

def cpu_percent() -> float:
    return _proc.cpu_percent(interval=None)


def memory_stats() -> tuple[float, float]:
    mem       = _proc.memory_info()
    rss_mb    = mem.rss / (1024 * 1024)
    sys_total = psutil.virtual_memory().total
    pct       = (mem.rss / sys_total) * 100.0
    return round(rss_mb, 2), round(pct, 2)


def db_size_mb() -> float:
    try:
        resp = supabase.rpc("get_db_size_mb", {}).execute()
        return round(resp.data or 0.0, 2)
    except Exception as exc:
        logger.warning(f"Could not fetch DB size: {exc}")
        return 0.0


def queue_counts() -> tuple[int, int]:
    try:
        resp   = supabase.rpc("get_queue_counts", {}).execute()
        counts = {row["status"]: row["cnt"] for row in (resp.data or [])}
        return int(counts.get("pending", 0)), int(counts.get("dead_letter", 0))
    except Exception as exc:
        logger.warning(f"Could not fetch queue counts: {exc}")
        return 0, 0


# ---------------------------------------------------------------------------
# Snapshot background worker
# ---------------------------------------------------------------------------

async def monitoring_snapshot_worker() -> None:
    logger.info(f"Monitoring snapshot worker started (interval={SNAPSHOT_INTERVAL_S}s).")
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL_S)
        try:
            rss_mb, mem_pct = memory_stats()
            cpu             = cpu_percent()
            db_mb           = db_size_mb()
            pending, dlq    = queue_counts()
            window          = await monitoring.snapshot_and_reset()

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

            supabase.rpc("purge_old_snapshots", {}).execute()

            logger.info(
                f"[Monitoring] CPU={cpu}%  MEM={rss_mb}MB  DB={db_mb}MB  "
                f"pending={pending}  dlq={dlq}  "
                f"processed={window['events_processed']}  "
                f"failed={window['events_failed']}  "
                f"rejected={window['events_rejected']}  "
                f"pings/s={window['avg_pings_per_sec']}"
            )
        except Exception as exc:
            logger.error(f"Monitoring snapshot failed: {exc}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/live", response_model=LiveMonitoringSnapshot)
async def live_monitoring():
    rss_mb, mem_pct = memory_stats()
    cpu             = cpu_percent()
    db_mb           = db_size_mb()
    pending, dlq    = queue_counts()

    async with monitoring._lock:
        processed = monitoring._events_processed
        failed    = monitoring._events_failed
        rejected  = monitoring._events_rejected
        pps       = monitoring.pings_per_second()

    return LiveMonitoringSnapshot(
        captured_at       = datetime.now(timezone.utc).isoformat(),
        cpu_percent       = cpu,
        memory_used_mb    = rss_mb,
        memory_percent    = mem_pct,
        db_size_mb        = db_mb,
        queue_pending     = pending,
        queue_dead_letter = dlq,
        events_processed  = processed,
        events_failed     = failed,
        events_rejected   = rejected,
        pings_per_sec     = pps,
    )


@router.get("/history")
def monitoring_history(hours: int = 24):
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
        "period_hours":   hours,
        "snapshot_count": len(result.data),
        "snapshots":      result.data,
    }


@router.get("/dlq")
def get_dlq(limit: int = 50):
    limit  = min(limit, 200)
    result = (
        supabase.table("event_queue")
        .select("id, device_id, retry_count, error_msg, created_at, updated_at")
        .eq("status", "dead_letter")
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return {"dead_letter_count": len(result.data), "items": result.data}
