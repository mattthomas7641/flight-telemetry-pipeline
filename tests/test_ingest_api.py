from __future__ import annotations

from collections.abc import Callable
from typing import Any

import fakeredis
import httpx
import orjson
import pytest
from pydantic import SecretStr

from flightline.ingest.app import create_app
from flightline.settings import Settings
from flightline.streams import QUARANTINE_STREAM, WRITER_GROUP, TelemetryBus
from tests.conftest import raw_frame


@pytest.fixture
def client_for(redis: fakeredis.FakeAsyncRedis) -> Callable[[Settings], Any]:
    def make(settings: Settings) -> Any:
        app = create_app(settings, redis=redis)

        class _Ctx:
            async def __aenter__(self) -> httpx.AsyncClient:
                self.lifespan = app.router.lifespan_context(app)
                await self.lifespan.__aenter__()
                self.client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                )
                return self.client

            async def __aexit__(self, *exc: object) -> None:
                await self.client.aclose()
                await self.lifespan.__aexit__(None, None, None)

        return _Ctx()

    return make


async def test_accepts_governs_and_enqueues(
    settings: Settings, client_for: Any, bus: TelemetryBus
) -> None:
    async with client_for(settings) as c:
        r = await c.post("/v1/frames", json={"frames": [raw_frame(seq=i) for i in range(3)]})
    assert r.status_code == 202
    body = r.json()
    assert (body["accepted"], body["quarantined"]) == (3, 0)

    entries = await bus.read(WRITER_GROUP, "t", bus.streams_for(), block_ms=1)
    frames = [f for e in entries for f in e.frames]
    assert len(frames) == 3
    assert all(s.classification for f in frames for s in f.samples)
    assert {f.lineage.ingest_id for f in frames} == {body["ingest_id"]}


async def test_bad_frames_are_quarantined_individually(
    settings: Settings, client_for: Any, redis: fakeredis.FakeAsyncRedis
) -> None:
    frames = [
        raw_frame(seq=1),
        raw_frame({"not.in.catalog": 1.0}, seq=2),
        {**raw_frame(seq=3), "seq": "three"},
        "not even an object",
    ]
    async with client_for(settings) as c:
        r = await c.post("/v1/frames", json={"frames": frames})
    body = r.json()
    assert (r.status_code, body["accepted"], body["quarantined"]) == (202, 1, 3)
    assert [x["reason"] for x in body["rejections"]] == [
        "unregistered_channel",
        "schema_invalid",
        "schema_invalid",
    ]
    assert [x["index"] for x in body["rejections"]] == [1, 2, 3]

    stored = await redis.xrange(QUARANTINE_STREAM)
    assert len(stored) == 3
    assert orjson.loads(stored[2][1][b"d"])["frame"] == {"raw": "not even an object"}


@pytest.mark.parametrize(
    ("body", "status"),
    [
        (b"not json", 400),
        (b'{"nope": []}', 400),
        (b'{"frames": {}}', 400),
    ],
)
async def test_malformed_envelope_is_rejected(
    settings: Settings, client_for: Any, body: bytes, status: int
) -> None:
    async with client_for(settings) as c:
        r = await c.post("/v1/frames", content=body)
    assert r.status_code == status
    assert r.json()["error"] == "bad_request"


async def test_oversized_batch_rejected(settings: Settings, client_for: Any) -> None:
    settings.max_frames_per_request = 2
    async with client_for(settings) as c:
        r = await c.post("/v1/frames", json={"frames": [raw_frame(seq=i) for i in range(3)]})
    assert r.status_code == 413


async def test_backpressure_sheds_load_with_retry_after(
    settings: Settings, client_for: Any
) -> None:
    settings.backpressure_max_lag = 2
    async with client_for(settings) as c:
        for i in range(3):  # 3 aircraft -> 3 entries of lag
            await c.post("/v1/frames", json={"frames": [raw_frame(aircraft_id=f"N{i}FL")]})
            c._transport.app.state.lag._at = 0  # type: ignore[attr-defined]  # expire cache
        r = await c.post("/v1/frames", json={"frames": [raw_frame()]})
    assert r.status_code == 503
    assert r.headers["retry-after"] == "2"


async def test_api_key_required_when_configured(settings: Settings, client_for: Any) -> None:
    settings.api_keys = [SecretStr("k1"), SecretStr("k2")]
    payload = {"frames": [raw_frame()]}
    async with client_for(settings) as c:
        assert (await c.post("/v1/frames", json=payload)).status_code == 401
        bad = {"authorization": "Bearer nope"}
        assert (await c.post("/v1/frames", json=payload, headers=bad)).status_code == 401
        ok = {"authorization": "Bearer k2"}
        assert (await c.post("/v1/frames", json=payload, headers=ok)).status_code == 202


async def test_health_ready_catalog_and_metrics(settings: Settings, client_for: Any) -> None:
    async with client_for(settings) as c:
        assert (await c.get("/healthz")).json() == {"status": "ok"}
        assert (await c.get("/readyz")).json() == {"status": "ready"}
        cat = (await c.get("/v1/catalog")).json()
        assert cat["catalog_version"].startswith("v3-")
        await c.post("/v1/frames", json={"frames": [raw_frame()]})
        metrics = (await c.get("/metrics/")).text
    assert "flightline_ingest_frames_total" in metrics


async def test_not_ready_when_redis_is_down(settings: Settings, client_for: Any) -> None:
    async with client_for(settings) as c:
        app = c._transport.app  # type: ignore[attr-defined]

        class Down:
            async def ping(self) -> None:
                raise ConnectionError("redis unreachable")

        app.state.redis = Down()
        r = await c.get("/readyz")
    assert r.status_code == 503
