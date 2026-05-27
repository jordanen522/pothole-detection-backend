import json
from datetime import datetime, timezone

from fastapi import APIRouter

from ..config import supabase, logger
from ..models import PotholeEventIn
from ..monitoring import monitoring

router = APIRouter(tags=["events"])


@router.post("/events", status_code=202)
async def ingest_event(event: PotholeEventIn):
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
