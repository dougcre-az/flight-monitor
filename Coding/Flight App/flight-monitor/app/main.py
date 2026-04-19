from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.config import get_settings, suggest_hubs
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
    client = get_duffel_client()
    await client.start()
    log.info("Duffel client started")
    yield
    await client.stop()
    log.info("Duffel client stopped")


settings = get_settings()

app = FastAPI(
    title       = "Flight Price Monitor",
    description = "Real-time flight price comparison via Duffel API.",
    version     = "0.1.0",
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
            detail      = "Duffel API key not configured. Set DUFFEL_API_KEY in environment.",
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
