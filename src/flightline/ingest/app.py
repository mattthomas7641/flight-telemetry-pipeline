"""Ingest API: validate -> govern -> enqueue.

Deliberately stateless and CPU-bound so it scales horizontally on CPU (HPA). One
uvicorn worker per pod keeps Prometheus metrics single-process; scale with replicas.
"""

from __future__ import annotations

import hmac
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import orjson
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from pydantic import ValidationError
from redis.asyncio import Redis

from flightline.governance import (
    Catalog,
    GovernedFrame,
    Quarantined,
    QuarantineReason,
    Tagger,
)
from flightline.models import TelemetryFrame
from flightline.observability import (
    INGEST_FRAMES,
    INGEST_QUARANTINED,
    INGEST_REJECTED,
    INGEST_REQUEST_SECONDS,
)
from flightline.settings import Settings
from flightline.streams import WRITER_GROUP, CachedLag, TelemetryBus

log = logging.getLogger(__name__)

MAX_REPORTED_REJECTIONS = 20


def create_app(settings: Settings, redis: Redis | None = None) -> FastAPI:
    catalog = Catalog.load(settings.catalog_path)
    tagger = Tagger(catalog, max_clock_skew_s=settings.max_clock_skew_s)
    api_keys = [k.get_secret_value().encode() for k in settings.api_keys]

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        r = redis or Redis.from_url(settings.redis_url)
        bus = TelemetryBus(r, settings.shards)
        await bus.ensure_group(WRITER_GROUP, bus.streams_for())
        app.state.redis = r
        app.state.bus = bus
        app.state.lag = CachedLag(bus, WRITER_GROUP)
        log.info("ingest ready", extra={"catalog_version": catalog.version})
        yield
        if redis is None:
            await r.aclose()

    app = FastAPI(
        title="Flightline Ingest",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.mount("/metrics", make_asgi_app())

    def _authorized(request: Request) -> bool:
        if not api_keys:
            return True
        header = request.headers.get("authorization", "")
        token = header.removeprefix("Bearer ").encode()
        # Compare against every key so timing does not reveal which (if any) matched.
        return any([hmac.compare_digest(token, k) for k in api_keys])

    def _error(status: int, reason: str, detail: str, **headers: str) -> JSONResponse:
        INGEST_REJECTED.labels(reason).inc()
        return JSONResponse(
            {"error": reason, "detail": detail}, status_code=status, headers=headers
        )

    @app.post("/v1/frames", status_code=202)
    async def ingest(request: Request) -> JSONResponse:
        start = time.perf_counter()
        if not _authorized(request):
            return _error(401, "unauthorized", "missing or invalid bearer token")

        lag = await request.app.state.lag.get()
        if lag > settings.backpressure_max_lag:
            # Shedding here keeps producers' retry loops in charge of buffering, rather
            # than growing Redis memory until it evicts or falls over.
            return _error(503, "backpressure", f"writer lag {lag}", **{"Retry-After": "2"})

        try:
            body = orjson.loads(await request.body())
            raw_frames = body["frames"]
            if not isinstance(raw_frames, list):
                raise TypeError("frames must be a list")
        except (orjson.JSONDecodeError, KeyError, TypeError) as e:
            return _error(400, "bad_request", f"expected {{'frames': [...]}}: {e}")
        if len(raw_frames) > settings.max_frames_per_request:
            return _error(413, "too_many_frames", f"max {settings.max_frames_per_request}")

        ingest_id = uuid.uuid4().hex
        now_ns = time.time_ns()
        governed: list[GovernedFrame] = []
        quarantined: list[Quarantined] = []
        rejections: list[dict[str, Any]] = []

        # Validate frame by frame so one bad frame quarantines itself, not the batch.
        for i, raw in enumerate(raw_frames):
            try:
                frame = TelemetryFrame.model_validate(raw)
            except ValidationError as e:
                q = Quarantined(
                    QuarantineReason.SCHEMA_INVALID,
                    _summarize(e),
                    raw if isinstance(raw, dict) else {"raw": raw},
                    ingest_id,
                    now_ns,
                )
            else:
                result = tagger.tag(frame, ingest_id, now_ns)
                if isinstance(result, GovernedFrame):
                    governed.append(result)
                    continue
                q = result
            quarantined.append(q)
            INGEST_QUARANTINED.labels(q.reason.value).inc()
            if len(rejections) < MAX_REPORTED_REJECTIONS:
                rejections.append({"index": i, "reason": q.reason.value, "detail": q.detail})

        await request.app.state.bus.publish(governed, quarantined)

        INGEST_FRAMES.labels("accepted").inc(len(governed))
        INGEST_FRAMES.labels("quarantined").inc(len(quarantined))
        INGEST_REQUEST_SECONDS.observe(time.perf_counter() - start)
        return JSONResponse(
            {
                "ingest_id": ingest_id,
                "accepted": len(governed),
                "quarantined": len(quarantined),
                "rejections": rejections,
            },
            status_code=202,
        )

    @app.get("/v1/catalog")
    async def get_catalog() -> dict[str, Any]:
        """Lets DAU configuration tooling check channels against policy before a flight."""
        return {"catalog_version": catalog.version, **catalog.spec.model_dump()}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        try:
            await request.app.state.redis.ping()
        except Exception as e:
            return JSONResponse({"status": "unavailable", "detail": str(e)}, status_code=503)
        return JSONResponse({"status": "ready"})

    return app


def _summarize(e: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()[:3]
    )
