from datetime import datetime, timezone, timedelta
from typing import Optional

from .config import supabase, CLUSTER_RADIUS_M


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
        .select("canonical_lat, canonical_lng, severity_score, hit_count")
        .eq("pothole_id", pothole_id)
        .single()
        .execute()
    )
    old_lat      = current.data["canonical_lat"]
    old_lng      = current.data["canonical_lng"]
    old_severity = current.data["severity_score"]
    old_count    = current.data["hit_count"]
    new_count    = old_count + 1

    new_lat      = round((old_lat * old_count + lat) / new_count, 7)
    new_lng      = round((old_lng * old_count + lng) / new_count, 7)
    new_severity = round((old_severity * old_count + severity) / new_count, 4)

    supabase.table("potholes").update({
        "canonical_lat":  new_lat,
        "canonical_lng":  new_lng,
        "severity_score": new_severity,
        "hit_count":      new_count,
        "last_seen":      detected_at.isoformat(),
    }).eq("pothole_id", pothole_id).execute()

    return pothole_id


def yesterday_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
