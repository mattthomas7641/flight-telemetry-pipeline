"""End-to-end against the running docker compose stack: `make up && make e2e`.

Flies real simulated aircraft through ingest -> Redis -> writer -> S3 and
alerter -> pager, then checks the observable outcomes of each pipeline path."""

from __future__ import annotations

import asyncio
import os
import time

import boto3
import httpx
import pytest

from flightline.simulator import Scenario, fly

pytestmark = pytest.mark.e2e

INGEST = os.environ.get("E2E_INGEST_URL", "http://localhost:8080")
PAGER = os.environ.get("E2E_PAGER_URL", "http://localhost:8090")
S3 = os.environ.get("E2E_S3_URL", "http://localhost:4566")
BUCKET = "flightline-telemetry"


async def test_full_pipeline() -> None:
    started = time.time()
    overtemp, rogue = await asyncio.gather(
        fly(
            INGEST,
            aircraft=2,
            duration_s=60,
            scenario=Scenario.MOTOR_OVERTEMP,
            seed=11,
            first_tail=401,
        ),
        fly(
            INGEST,
            aircraft=1,
            duration_s=60,
            scenario=Scenario.ROGUE_CHANNEL,
            seed=12,
            first_tail=501,
        ),
    )
    await asyncio.sleep(8)  # one flush interval + alerter tick

    # Paging: the overheating motor on N401FL paged and auto-resolved; nothing else did.
    async with httpx.AsyncClient(base_url=PAGER) as c:
        incidents = [
            i
            for i in (await c.get("/incidents")).json()
            if i["opened_at"] >= started and i["dedup_key"].startswith("motor_winding_overtemp")
        ]
    assert [i["dedup_key"].split("/")[1] for i in incidents] == ["N401FL"]
    assert incidents[0]["status"] == "resolved"

    # Governance: the rogue channel was quarantined at the door, the rest accepted.
    assert overtemp.frames_quarantined == 0
    assert rogue.frames_quarantined > 0
    assert rogue.frames_accepted > 0

    # Storage: every aircraft landed, split by classification, with policy tags.
    s3 = boto3.client(
        "s3",
        endpoint_url=S3,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    def keys(prefix: str) -> list[str]:
        # Prefix-scoped like real lake queries (LocalStack 4.0 also mis-paginates
        # bucket-wide listings past 1000 keys).
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"telemetry/{prefix}")
        return [o["Key"] for o in resp.get("Contents", [])]

    for tail in ("N401FL", "N402FL", "N501FL"):
        for cls in ("internal", "proprietary", "export_controlled"):
            assert keys(f"classification={cls}/campaign=FT-2026-TRANSITION/aircraft={tail}/"), (
                tail,
                cls,
            )
    assert keys("_quarantine/")
    sample = keys("classification=export_controlled/campaign=FT-2026-TRANSITION/aircraft=N401FL/")[
        0
    ]
    tags = {
        t["Key"]: t["Value"] for t in s3.get_object_tagging(Bucket=BUCKET, Key=sample)["TagSet"]
    }
    assert tags == {"classification": "export_controlled", "retention_class": "flight_test"}
