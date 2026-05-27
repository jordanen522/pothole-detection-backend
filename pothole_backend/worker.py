import asyncio
from datetime import datetime

from .config import supabase, CLUSTER_RADIUS_M, logger
from .database import find_nearby_pothole, upsert_pothole, yesterday_iso
from .models import AccelBurst
from .monitoring import monitoring, _proc
from .scoring import score_severity, haversine_m


def process_event_math(
    device_id_hash: str,
    lat:            float,
    lng:            float,
    accel_burst:    AccelBurst,
    detected_at:    datetime,
    app_version:    str | None,
) -> None:
    severity = score_severity(accel_burst)

    recent = (
        supabase.table("events")
        .select("latitude, longitude")
        .eq("device_id_hash", device_id_hash)
        .gte("detected_at", yesterday_iso())
        .execute()
    )
    for prev in recent.data:
        if haversine_m(lat, lng, prev["latitude"], prev["longitude"]) < CLUSTER_RADIUS_M:
            logger.info(f"Duplicate — hash {device_id_hash[:8]} already reported this location today.")
            return

    pothole_id = find_nearby_pothole(lat, lng)
    pothole_id = upsert_pothole(
        pothole_id=pothole_id,
        lat=lat,
        lng=lng,
        severity=severity,
        device_id=device_id_hash,
        detected_at=detected_at,
    )

    supabase.table("events").insert({
        "device_id_hash": device_id_hash,
        "pothole_id":     pothole_id,
        "latitude":       lat,
        "longitude":      lng,
        "severity":       severity,
        "detected_at":    detected_at.isoformat(),
        "app_version":    app_version,
    }).execute()


async def queue_worker() -> None:
    logger.info("Queue worker started.")
    _proc.cpu_percent(interval=None)  # warm up psutil; first call always returns 0.0

    while True:
        try:
            response = (
                supabase.table("nonanon_events")
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
                job_id         = job["id"]
                device_id_hash = job["device_id_hash"]
                payload        = job["payload"]

                try:
                    process_event_math(
                        device_id_hash=device_id_hash,
                        lat=payload["latitude"],
                        lng=payload["longitude"],
                        accel_burst=AccelBurst(**payload["accel_burst"]),
                        detected_at=datetime.fromisoformat(payload["detected_at"]),
                        app_version=payload.get("app_version"),
                    )

                    supabase.table("nonanon_events").delete().eq("id", job_id).execute()
                    await monitoring.record_processed()

                except Exception as err:
                    new_retries = job["retry_count"] + 1
                    logger.error(f"Job {job_id} failed (attempt {new_retries}): {err}")
                    await monitoring.record_failed()

                    if new_retries >= 3:
                        supabase.table("nonanon_events").update({
                            "status":    "dead_letter",
                            "error_msg": str(err)[:500],
                        }).eq("id", job_id).execute()
                        logger.error(f"Job {job_id} → DLQ.")
                    else:
                        supabase.table("nonanon_events").update({
                            "retry_count": new_retries,
                            "error_msg":   str(err)[:500],
                        }).eq("id", job_id).execute()

        except Exception as poll_err:
            logger.error(f"Queue worker polling error: {poll_err}")
            await asyncio.sleep(5)
