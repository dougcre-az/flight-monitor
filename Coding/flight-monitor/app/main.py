from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import Depends, FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.config import get_settings, suggest_hubs
from app.db import init_db
from app.models import (
    BucketAnalysis, BucketRequest, CabinClass, CompareRequest,
    CompareResult, ErrorResponse, HealthResponse, SearchRequest,
    SearchResponse,
)
from services.compare import CompareEngine
from services.duffel import DuffelClient, DuffelError, get_duffel_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init DB
    init_db()

    # Start Duffel client
    client = get_duffel_client()
    await client.start()
    log.info("Duffel client started")

    # Start scheduler — runs daily at 8am UTC
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from services.scheduler import run_daily_check
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_daily_check,
        trigger="cron",
        hour=8,
        minute=0,
        kwargs={"duffel_client": client},
        id="daily_check",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started — daily check at 08:00 UTC")

    yield

    scheduler.shutdown()
    await client.stop()
    log.info("Shutdown complete")


settings = get_settings()

app = FastAPI(
    title       = "Flight Price Monitor",
    description = "Real-time flight price comparison via Duffel API.",
    version     = "0.2.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = settings.cors_origins.split(","),
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


def get_engine(duffel: DuffelClient = Depends(get_duffel_client)) -> CompareEngine:
    return CompareEngine(duffel)


def require_duffel(duffel: DuffelClient = Depends(get_duffel_client)) -> DuffelClient:
    if not settings.duffel_configured:
        raise HTTPException(
            status_code = 503,
            detail      = "Duffel API key not configured.",
        )
    return duffel


@app.exception_handler(DuffelError)
async def duffel_error_handler(request, exc: DuffelError):
    return JSONResponse(
        status_code = exc.status_code or 502,
        content     = ErrorResponse(
            error  = "Duffel API error",
            detail = str(exc),
            code   = exc.code or "duffel_error",
        ).model_dump(),
    )


@app.exception_handler(ValueError)
async def value_error_handler(request, exc: ValueError):
    return JSONResponse(
        status_code = 422,
        content     = ErrorResponse(error="Validation error", detail=str(exc)).model_dump(),
    )


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health(duffel: DuffelClient = Depends(get_duffel_client)) -> HealthResponse:
    if not settings.duffel_configured:
        return HealthResponse(duffel="unconfigured")
    reachable = await duffel.ping()
    return HealthResponse(duffel="ok" if reachable else "error")


@app.post("/run", tags=["meta"])
async def manual_run(background_tasks: BackgroundTasks, duffel: DuffelClient = Depends(require_duffel)):
    """Manually trigger the daily price check — useful for testing."""
    from services.scheduler import run_daily_check
    background_tasks.add_task(run_daily_check, duffel)
    return {"status": "started", "message": "Daily check running in background — check Sheets in ~2 minutes"}


@app.get("/routes", tags=["routes"])
async def list_routes():
    """List all monitored routes."""
    from services.scheduler import MONITORED_ROUTES
    return {"routes": MONITORED_ROUTES}


@app.get("/history/{route_id}", tags=["routes"])
async def price_history(route_id: str, limit: int = Query(default=30, ge=1, le=90)):
    """Get price history for a monitored route."""
    from app.db import get_price_history
    history = get_price_history(route_id, limit)
    return {"route_id": route_id, "history": history}


@app.post("/search", response_model=SearchResponse, tags=["search"])
async def search(
    request: SearchRequest,
    max_results: int = Query(default=10, ge=1, le=50),
    duffel: DuffelClient = Depends(require_duffel),
) -> SearchResponse:
    from datetime import datetime
    offers = await duffel.create_offer_request(request)
    offers = offers[:max_results]
    return SearchResponse(
        offers        = offers,
        total_results = len(offers),
        searched_at   = datetime.utcnow().isoformat(),
        cabin         = request.cabin,
        trip_type     = request.trip_type,
    )


@app.post("/compare", response_model=CompareResult, tags=["compare"])
async def compare(
    request: CompareRequest,
    engine:  CompareEngine = Depends(get_engine),
    _duffel: DuffelClient  = Depends(require_duffel),
) -> CompareResult:
    return await engine.compare(request)


@app.post("/compare/buckets", response_model=BucketAnalysis, tags=["compare"])
async def bucket_analysis(
    request: BucketRequest,
    engine:  CompareEngine = Depends(get_engine),
    _duffel: DuffelClient  = Depends(require_duffel),
) -> BucketAnalysis:
    return await engine.bucket_analysis(request)


@app.get("/hubs/suggest", tags=["hubs"])
async def suggest_hub_airports(
    origin:      str           = Query(..., min_length=3, max_length=3),
    destination: str           = Query(..., min_length=3, max_length=3),
    dest_flag:   Optional[str] = Query(default=None),
    max_hubs:    int           = Query(default=8, ge=1, le=12),
) -> dict:
    hubs = suggest_hubs(
        origin      = origin.upper(),
        destination = destination.upper(),
        dest_flag   = dest_flag,
        max_hubs    = max_hubs,
    )
    return {"origin": origin.upper(), "destination": destination.upper(), "hubs": hubs}
