from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class AccelBurst(BaseModel):
    z_values:      list[float] = Field(..., min_length=50)
    timestamps_ms: list[int]   = Field(..., min_length=50)


class PotholeEventIn(BaseModel):
    device_id:   str
    latitude:    float          = Field(..., ge=-90,  le=90)
    longitude:   float          = Field(..., ge=-180, le=180)
    detected_at: datetime
    accel_burst: AccelBurst
    app_version: Optional[str] = None


class PotholeRecord(BaseModel):
    pothole_id:     str
    canonical_lat:  float
    canonical_lng:  float
    severity_score: float
    hit_count:      int
    priority_score: float
    first_seen:     datetime
    last_seen:      datetime
    traffic_weight: float = 1.0


class LiveMonitoringSnapshot(BaseModel):
    captured_at:       str
    cpu_percent:       float
    memory_used_mb:    float
    memory_percent:    float
    db_size_mb:        float
    queue_pending:     int
    queue_dead_letter: int
    events_processed:  int
    events_failed:     int
    events_rejected:   int
    pings_per_sec:     float
