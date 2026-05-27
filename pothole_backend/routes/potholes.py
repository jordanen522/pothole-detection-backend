from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter

from ..config import supabase, MILES_TO_METRES
from ..models import PotholeRecord

router = APIRouter(tags=["potholes"])


@router.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get("/potholes", response_model=list[PotholeRecord])
def get_potholes(
    lat:          Optional[float] = None,
    lng:          Optional[float] = None,
    radius_miles: float           = 80.0,
    min_priority: float           = 0.0,
    limit:        int             = 200,
    offset:       int             = 0,
):
    limit = min(limit, 500)

    if lat is not None and lng is not None:
        radius_m = radius_miles * MILES_TO_METRES
        result   = supabase.rpc(
            "get_potholes_near",
            {"lat": lat, "lng": lng, "radius": radius_m},
        ).execute()
        data = [r for r in result.data if r["priority_score"] >= min_priority]
        return data[offset: offset + limit]

    result = (
        supabase.table("potholes")
        .select("*")
        .gte("priority_score", min_priority)
        .order("priority_score", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return result.data


@router.get("/potholes/geojson")
def get_potholes_geojson(
    lat:          Optional[float] = None,
    lng:          Optional[float] = None,
    radius_miles: float           = 80.0,
    min_priority: float           = 0.0,
):
    if lat is not None and lng is not None:
        radius_m = radius_miles * MILES_TO_METRES
        result   = supabase.rpc(
            "get_potholes_near",
            {"lat": lat, "lng": lng, "radius": radius_m},
        ).execute()
        rows = [r for r in result.data if r["priority_score"] >= min_priority]
    else:
        result = (
            supabase.table("potholes")
            .select(
                "pothole_id, canonical_lat, canonical_lng, "
                "severity_score, hit_count, priority_score, last_seen"
            )
            .gte("priority_score", min_priority)
            .execute()
        )
        rows = result.data

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
        for row in rows
    ]

    return {"type": "FeatureCollection", "features": features}
