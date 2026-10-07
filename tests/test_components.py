"""S3 sink, simulator physics, health endpoint and pager mock."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import boto3
import httpx
import orjson

from flightline.governance import GovernedFrame, Tagger
from flightline.health import Heartbeat, serve
from flightline.models import TelemetryFrame
from flightline.observability import JsonFormatter
from flightline.pager_mock import create_app as create_pager
from flightline.simulator import AircraftModel, Scenario, phase_at, synthetic_frames
from flightline.writer.sinks import S3Sink


async def test_s3_sink_sets_object_tags_atomically(s3: Any) -> None:
    sink = S3Sink("lake", client=s3)
    await sink.put("t/a.parquet", b"x", {"classification": "proprietary"}, "application/x")
    tags = s3.get_object_tagging(Bucket="lake", Key="t/a.parquet")["TagSet"]
    assert tags == [{"Key": "classification", "Value": "proprietary"}]


async def test_s3_sink_requests_kms_encryption(s3: Any) -> None:
    kms = boto3.client("kms", region_name="us-east-1")
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    await S3Sink("lake", client=s3, kms_key_id=key_id).put("k", b"x", {}, "application/x")
    assert s3.head_object(Bucket="lake", Key="k")["ServerSideEncryption"] == "aws:kms"


def fly(model: AircraftModel, seconds: float, hz: int = 20) -> list[dict[str, Any]]:
    dt = 1 / hz
    return [model.frame(i * dt, dt, 1) for i in range(int(seconds * hz) + 1)]


def peak(frames: list[dict[str, Any]], channel: str) -> float:
    return max(f["samples"].get(channel, float("-inf")) for f in frames)


def test_nominal_flight_stays_inside_every_threshold() -> None:
    frames = fly(AircraftModel("N1", "F", "C", duration_s=120, seed=1), 120)
    assert peak(frames, "motor.7.winding_temp_c") < 140
    assert peak(frames, "battery.1.cell_temp_max_c") < 55
    assert min(f["samples"]["battery.pack_voltage_v"] for f in frames) > 640
    assert peak(frames, "struct.vibration_g_rms") < 3
    assert frames[-1]["phase"] == "shutdown"
    assert [f["seq"] for f in frames[:3]] == [1, 2, 3]


def test_overtemp_scenario_crosses_critical() -> None:
    frames = fly(AircraftModel("N1", "F", "C", 120, Scenario.MOTOR_OVERTEMP, seed=1), 120)
    assert peak(frames, "motor.7.winding_temp_c") > 165
    assert peak(frames, "motor.8.winding_temp_c") < 140  # only the failing motor


def test_sim_output_passes_governance(tagger: Tagger) -> None:
    for scenario, expect_governed in [
        (Scenario.NOMINAL, True),
        (Scenario.SENSOR_FAULT, True),
        (Scenario.ROGUE_CHANNEL, False),
    ]:
        m = AircraftModel("N1", "F", "C", 100, scenario, seed=2)
        frame = m.frame(45, 0.02, 1)  # t=45 s is inside the scenario window
        out = tagger.tag(TelemetryFrame.model_validate(frame), "i")
        assert isinstance(out, GovernedFrame) is expect_governed, scenario


def test_dropout_window_is_eight_seconds() -> None:
    m = AircraftModel("N1", "F", "C", 100, Scenario.DROPOUT)
    silent = [t for t in range(100) if m.in_dropout(t / 100)]
    assert silent == list(range(45, 54))
    assert not AircraftModel("N1", "F", "C", 100).in_dropout(0.5)


def test_flight_profile_order() -> None:
    assert [phase_at(x).value for x in (0, 0.1, 0.5, 0.9, 0.99)] == [
        "preflight",
        "hover",
        "cruise",
        "hover",
        "landed",
    ]


def test_synthetic_frames_are_valid_and_interleaved() -> None:
    frames = list(synthetic_frames(8, aircraft=4))
    assert [f["aircraft_id"] for f in frames[:4]] == ["N301FL", "N302FL", "N303FL", "N304FL"]
    for f in frames:
        TelemetryFrame.model_validate(f)


async def test_worker_health_endpoint_reflects_heartbeat() -> None:
    hb = Heartbeat(max_age_s=60)
    server = await serve(hb, 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as c:
            assert (await c.get("/healthz")).status_code == 200
            assert "python_info" in (await c.get("/metrics")).text
            assert (await c.get("/nope")).status_code == 404
            hb.max_age_s = 0
            await asyncio.sleep(0.01)
            assert (await c.get("/healthz")).status_code == 503
    finally:
        server.close()


async def test_pager_mock_dedups_and_reopens() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_pager()), base_url="http://p"
    ) as c:
        trig = {"event_action": "trigger", "dedup_key": "k", "payload": {"severity": "warning"}}
        await c.post("/v2/enqueue", json=trig)
        await c.post("/v2/enqueue", json=trig)
        assert len((await c.get("/incidents")).json()) == 1
        await c.post("/v2/enqueue", json={"event_action": "resolve", "dedup_key": "k"})
        await c.post("/v2/enqueue", json=trig)
        [inc] = (await c.get("/incidents")).json()
        assert (inc["status"], inc["triggers"]) == ("triggered", 1)
        bad = await c.post("/v2/enqueue", json={"event_action": "explode"})
        assert bad.status_code == 400
        assert (await c.get("/healthz")).status_code == 200


def test_json_log_formatter_includes_extras() -> None:
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    rec.aircraft = "N301FL"
    out = orjson.loads(JsonFormatter().format(rec))
    assert (out["msg"], out["aircraft"], out["level"]) == ("hello world", "N301FL", "INFO")
