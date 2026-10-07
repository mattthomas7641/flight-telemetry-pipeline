from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx
import orjson
import pytest

from flightline.alerting.engine import AlertEngine
from flightline.alerting.notifiers import (
    Dispatcher,
    LogNotifier,
    PagerDutyNotifier,
    SlackNotifier,
)
from flightline.alerting.rules import RuleSet
from flightline.alerting.worker import Alerter, ordinal_from_hostname, owned_shards
from flightline.governance import GovernedFrame
from flightline.models import Alert, AlertAction, Severity
from flightline.pager_mock import create_app as create_pager
from flightline.streams import ALERTER_GROUP, ALERTS_STREAM, TelemetryBus

MS = 1_000_000


def alert(
    action: AlertAction = AlertAction.TRIGGER, severity: Severity = Severity.CRITICAL
) -> Alert:
    return Alert(
        rule_id="motor_winding_overtemp",
        action=action,
        severity=severity,
        aircraft_id="N301FL",
        flight_id="F1",
        campaign="FT",
        channel="motor.7.winding_temp_c",
        value=171.2,
        threshold=165,
        event_ts_ns=1_700_000_000 * 10**9,
        ingested_at_ns=None,
        runbook="docs/RUNBOOK.md#motor-winding-overtemp",
        summary="[CRITICAL] N301FL motor.7.winding_temp_c = 171.2 degC",
    )


class Recorder:
    def __init__(self, statuses: list[int] | None = None) -> None:
        self.statuses = statuses or []
        self.bodies: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(orjson.loads(request.content))
        status = self.statuses.pop(0) if self.statuses else 202
        return httpx.Response(status, json={})


def client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_pagerduty_trigger_payload() -> None:
    rec = Recorder()
    async with client(rec) as c:
        await PagerDutyNotifier("https://pd/v2/enqueue", "rk", c).send(alert())
    [body] = rec.bodies
    assert body["routing_key"] == "rk"
    assert body["event_action"] == "trigger"
    assert body["dedup_key"] == "motor_winding_overtemp/N301FL/F1/motor.7.winding_temp_c"
    assert body["payload"]["severity"] == "critical"
    assert body["payload"]["source"] == "N301FL"
    assert body["payload"]["timestamp"].startswith("2023-11-14T22:13:20")
    assert body["links"][0]["text"] == "Runbook"


async def test_pagerduty_resolve_is_minimal() -> None:
    rec = Recorder()
    async with client(rec) as c:
        await PagerDutyNotifier("https://pd", "rk", c).send(alert(AlertAction.RESOLVE, Severity.OK))
    assert set(rec.bodies[0]) == {"routing_key", "event_action", "dedup_key"}


async def test_dispatcher_retries_transient_failures() -> None:
    rec = Recorder([503, 429, 202])
    async with client(rec) as c:
        d = Dispatcher([PagerDutyNotifier("https://pd", "rk", c)], base_delay_s=0)
        assert await d.dispatch(alert()) == {"pagerduty": True}
    assert len(rec.bodies) == 3


async def test_dispatcher_gives_up_after_max_attempts() -> None:
    rec = Recorder([500] * 10)
    async with client(rec) as c:
        d = Dispatcher([PagerDutyNotifier("https://pd", "rk", c)], max_attempts=3, base_delay_s=0)
        assert await d.dispatch(alert()) == {"pagerduty": False}
    assert len(rec.bodies) == 3


async def test_dispatcher_does_not_retry_permanent_rejection() -> None:
    rec = Recorder([400, 202])
    async with client(rec) as c:
        d = Dispatcher([PagerDutyNotifier("https://pd", "rk", c)], base_delay_s=0)
        assert await d.dispatch(alert()) == {"pagerduty": False}
    assert len(rec.bodies) == 1


async def test_one_failing_notifier_does_not_block_another() -> None:
    def slack_down(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("slack down")

    pd = Recorder()
    async with client(pd) as pd_client, client(slack_down) as slack_client:
        d = Dispatcher(
            [
                PagerDutyNotifier("https://pd", "rk", pd_client),
                SlackNotifier("https://slack", slack_client),
                LogNotifier(),
            ],
            max_attempts=2,
            base_delay_s=0,
        )
        assert await d.dispatch(alert()) == {"pagerduty": True, "slack": False, "log": True}


async def test_slack_message_links_runbook() -> None:
    rec = Recorder([200])
    async with client(rec) as c:
        await SlackNotifier("https://slack", c).send(alert())
    assert rec.bodies[0]["text"].startswith(":rotating_light:")
    assert "|runbook>" in rec.bodies[0]["text"]


def test_shard_ownership_partitions_all_shards() -> None:
    owned = [owned_shards(8, 3, i) for i in range(3)]
    assert sorted(s for o in owned for s in o) == list(range(8))
    assert owned[0] == [0, 3, 6]
    with pytest.raises(ValueError, match="out of range"):
        owned_shards(8, 3, 3)


@pytest.mark.parametrize(
    ("host", "ordinal"), [("flightline-alerter-2", 2), ("alerter-0", 0), ("laptop", 0)]
)
def test_ordinal_from_statefulset_hostname(host: str, ordinal: int) -> None:
    assert ordinal_from_hostname(host) == ordinal


async def test_alerter_end_to_end_against_pager_mock(
    bus: TelemetryBus, rules: RuleSet, make_frame: Callable[..., GovernedFrame]
) -> None:
    """Frames on the bus -> engine -> PagerDuty-format events -> one incident that
    escalates, then resolves; entries acked; audit trail written."""
    pager = create_pager()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=pager), base_url="http://pager"
    ) as pc:
        alerter = Alerter(
            bus,
            AlertEngine(rules),
            Dispatcher([PagerDutyNotifier("http://pager/v2/enqueue", "rk", pc)], base_delay_s=0),
            consumer="alerter-0",
            shards=list(range(4)),
            block_ms=1,
        )
        await alerter.start()
        t0 = 1_700_000_000 * 10**9
        temps = [100] + [150] * 5 + [175] * 5 + [100]
        await bus.publish(
            [
                make_frame({"motor.7.winding_temp_c": v}, seq=i, ts_ns=t0 + i * 100 * MS)
                for i, v in enumerate(temps)
            ]
        )
        await alerter.poll_once()

        [incident] = (await pc.get("/incidents")).json()
        events = (await pc.get("/events")).json()

    assert [(e["event_action"], e.get("payload", {}).get("severity")) for e in events] == [
        ("trigger", "warning"),
        ("trigger", "critical"),
        ("resolve", None),
    ]
    assert incident["status"] == "resolved"
    assert incident["triggers"] == 2
    assert await bus.lag(ALERTER_GROUP) == 0
    assert await bus.redis.xlen(ALERTS_STREAM) == 3


async def test_alerter_run_loop_emits_telemetry_loss(
    bus: TelemetryBus, rules: RuleSet, make_frame: Callable[..., GovernedFrame]
) -> None:
    sent: list[Alert] = []

    class Capture:
        name = "capture"

        async def send(self, a: Alert) -> None:
            sent.append(a)

    fast = rules.model_copy(
        update={"telemetry_loss": rules.telemetry_loss.model_copy(update={"timeout_s": 0.05})}
    )
    alerter = Alerter(
        bus,
        AlertEngine(fast),
        Dispatcher([Capture()]),
        consumer="alerter-0",
        shards=list(range(4)),
        block_ms=10,
        liveness_interval_s=0.02,
    )
    await bus.publish([make_frame()])
    stop = asyncio.Event()
    task = asyncio.create_task(alerter.run(stop))
    for _ in range(100):
        if sent:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert [a.rule_id for a in sent] == ["telemetry_loss"]


async def test_alerter_survives_poison_in_its_pending_list(
    bus: TelemetryBus, rules: RuleSet, make_frame: Callable[..., GovernedFrame]
) -> None:
    """Regression for the crash loop: poison delivered-but-unacked before a restart."""
    from flightline.streams import telemetry_stream

    streams = bus.streams_for()
    await bus.ensure_group(ALERTER_GROUP, streams)
    await bus.redis.xadd(telemetry_stream(0), {"d": b"stale codec"})
    await bus.read_raw(ALERTER_GROUP, "alerter-0", streams, block_ms=1)  # delivered, then "crash"

    alerter = Alerter(
        bus,
        AlertEngine(rules),
        Dispatcher([LogNotifier()]),
        "alerter-0",
        list(range(4)),
        block_ms=1,
    )
    await alerter.start()  # previously raised here, every restart
    await bus.publish([make_frame()])
    await alerter.poll_once()
    assert await bus.lag(ALERTER_GROUP) == 0


async def test_runbook_links_are_made_absolute() -> None:
    rec = Recorder()
    async with client(rec) as c:
        n = PagerDutyNotifier(
            "https://pd", "rk", c, runbook_base_url="https://git.example/fl/blob/main/"
        )
        await n.send(alert())
    assert rec.bodies[0]["links"][0]["href"] == (
        "https://git.example/fl/blob/main/docs/RUNBOOK.md#motor-winding-overtemp"
    )
