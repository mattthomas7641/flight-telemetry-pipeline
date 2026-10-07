from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import fakeredis
import pytest
from moto import mock_aws

from flightline.alerting.rules import RuleSet
from flightline.governance import Catalog, GovernedFrame, Tagger
from flightline.models import TelemetryFrame
from flightline.settings import Settings
from flightline.streams import TelemetryBus

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config" / "catalog.yaml"
RULES = ROOT / "config" / "rules.yaml"


@pytest.fixture
def catalog() -> Catalog:
    return Catalog.load(CATALOG)


@pytest.fixture
def rules() -> RuleSet:
    return RuleSet.load(RULES)


@pytest.fixture
def tagger(catalog: Catalog) -> Tagger:
    return Tagger(catalog)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        catalog_path=CATALOG,
        rules_path=RULES,
        local_sink_path=tmp_path / "lake",
        shards=4,
    )


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.aclose()


@pytest.fixture
def bus(redis: fakeredis.FakeAsyncRedis) -> TelemetryBus:
    return TelemetryBus(redis, shards=4)


def raw_frame(
    samples: dict[str, float] | None = None,
    *,
    aircraft_id: str = "N301FL",
    flight_id: str = "F1",
    seq: int = 1,
    ts_ns: int | None = None,
    phase: str | None = "cruise",
) -> dict[str, Any]:
    return {
        "aircraft_id": aircraft_id,
        "flight_id": flight_id,
        "campaign": "FT-TEST",
        "source_id": f"{aircraft_id}-dau1",
        "seq": seq,
        "ts_ns": ts_ns or time.time_ns(),
        "phase": phase,
        "samples": samples or {"motor.1.winding_temp_c": 80.0, "battery.soc_pct": 90.0},
    }


@pytest.fixture
def make_frame(tagger: Tagger) -> Callable[..., GovernedFrame]:
    """Build a governed frame the way ingest would."""

    def _make(samples: dict[str, float] | None = None, **kw: Any) -> GovernedFrame:
        frame = TelemetryFrame.model_validate(raw_frame(samples, **kw))
        out = tagger.tag(frame, ingest_id="test")
        assert isinstance(out, GovernedFrame), out
        return out

    return _make


@pytest.fixture
def s3() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lake")
        yield client
