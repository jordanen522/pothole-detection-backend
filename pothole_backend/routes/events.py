import hashlib
import hmac
import json
from datetime import date, datetime, timezone

from fastapi import APIRouter

from ..config import supabase, logger, DEVICE_ID_SALT
from ..models import PotholeEventIn
from ..monitoring import monitoring

router = APIRouter(tags=["events"])


def _hash_device_id(device_id: str) -> str:
    today = date.today().isoformat()
    return hmac.new(
        DEVICE_ID_SALT.encode("utf-8"),
        f"{device_id}:{today}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


@router.post("/events", status_code=202)
async def ingest_event(event: PotholeEventIn):
    device_id_hash = _hash_device_id(event.device_id)
    payload = json.loads(event.model_dump_json(exclude={"device_id"}))

    try:
        supabase.table("nonanon_events").insert({
            "device_id_hash": device_id_hash,
            "payload":        payload,
            "status":         "pending",
        }).execute()
    except Exception as exc:
        await monitoring.record_rejected()
        logger.error(f"Failed to queue event: {exc}")
        raise

    return {"accepted": True, "queued_at": datetime.now(timezone.utc).isoformat()}
