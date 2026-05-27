import asyncio
import json

from .config import supabase, CLUSTER_RADIUS_M, logger
from .database import find_nearby_pothole, upsert_pothole, yesterday_iso
from .models import PotholeEventIn
from .monitoring import monitoring, _proc
from .scoring import score_severity, haversine_m


def process_event_math(event: PotholeEventIn) -> None:
    severity = score_severity(event.accel_burst)

    recent = (
        supabase.table("events")
        .select("latitude, longitude")
        .eq("device_id", event.device_id)
        .gte("detected_at", yesterday_iso())
        .execute()
    )
    for prev in recent.data:
        if haversine_m(event.latitude, event.longitude, prev["latitude"], prev["longitude"]) < CLUSTER_RADIUS_M:
            logger.info(f"Duplicate — device {event.device_id[:8]} already reported this location today.")
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


async def queue_worker() -> None:
    logger.info("Queue worker started.")
    _proc.cpu_percent(interval=None)  # warm up psutil; first call always returns 0.0

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
