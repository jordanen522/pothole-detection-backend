import asyncio

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .config import logger
from .monitoring import monitoring, monitoring_snapshot_worker
from .worker import queue_worker
from .routes.events import router as events_router
from .routes.potholes import router as potholes_router
from .monitoring import router as monitoring_router

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


@app.middleware("http")
async def count_requests(request: Request, call_next):
    await monitoring.record_request()
    return await call_next(request)


app.include_router(events_router)
app.include_router(potholes_router)
app.include_router(monitoring_router)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(queue_worker())
    asyncio.create_task(monitoring_snapshot_worker())
    logger.info("Pothole backend v1.2 started — queue worker + monitoring snapshot worker running.")
