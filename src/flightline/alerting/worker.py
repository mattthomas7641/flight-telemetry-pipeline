"""Alerter: consumes its own consumer group, independent of the writer.

Two groups on the same streams means paging never waits on storage: an S3 slowdown
backs up the writer's lag, while alert latency is unaffected.

Rule evaluation is stateful per aircraft, so unlike the writer the alerter cannot let
any replica take any shard. It runs as a StatefulSet; replica N owns the shards with
`shard % replicas == N`, which gives each aircraft exactly one evaluator.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import defaultdict

import orjson

from flightline.alerting.engine import AlertEngine
from flightline.alerting.notifiers import Dispatcher
from flightline.health import Heartbeat
from flightline.models import Alert, AlertAction
from flightline.observability import (
    ACTIVE_ALERTS,
    ALERT_LATENCY_SECONDS,
    ALERTER_SAMPLES,
    ALERTS,
    STREAM_LAG,
)
from flightline.streams import (
    ALERTER_GROUP,
    ALERTS_STREAM,
    AUDIT_MAXLEN,
    Entry,
    TelemetryBus,
)

log = logging.getLogger(__name__)


def owned_shards(shards: int, replicas: int, ordinal: int) -> list[int]:
    if not 0 <= ordinal < replicas:
        raise ValueError(f"ordinal {ordinal} out of range for {replicas} replicas")
    return [s for s in range(shards) if s % replicas == ordinal]


def ordinal_from_hostname(hostname: str) -> int:
    """StatefulSet pods are named <set>-<ordinal>."""
    m = re.search(r"-(\d+)$", hostname)
    return int(m.group(1)) if m else 0


class Alerter:
    def __init__(
        self,
        bus: TelemetryBus,
        engine: AlertEngine,
        dispatcher: Dispatcher,
        consumer: str,
        shards: list[int],
        *,
        read_count: int = 64,
        block_ms: int = 200,
        liveness_interval_s: float = 1.0,
    ) -> None:
        self.bus = bus
        self.engine = engine
        self.dispatcher = dispatcher
        self.consumer = consumer
        self.streams = bus.streams_for(shards)
        self.read_count = read_count
        self.block_ms = block_ms
        self.liveness_interval_s = liveness_interval_s
        self._last_liveness = time.monotonic()
        self._watched: dict[str, bool] = {}

    def _keep(self, channel: str) -> bool:
        hit = self._watched.get(channel)
        if hit is None:
            hit = self._watched[channel] = bool(self.engine.match(channel))
        return hit

    async def start(self) -> None:
        await self.bus.ensure_group(ALERTER_GROUP, self.streams)
        # Replay this pod's unacked entries first. The StatefulSet gives a restarted pod
        # the same consumer name, so it picks up exactly where it died.
        pending = await self.bus.read(
            ALERTER_GROUP, self.consumer, self.streams, pending=True, keep=self._keep
        )
        if pending:
            log.info("replaying pending entries", extra={"count": len(pending)})
            await self.handle(pending)
        log.info("alerter ready", extra={"streams": self.streams})

    async def run(self, stop: asyncio.Event, heartbeat: Heartbeat | None = None) -> None:
        await self.start()
        while not stop.is_set():
            try:
                await self.poll_once()
                if heartbeat:
                    heartbeat.beat()
            except Exception:
                log.exception("alerter iteration failed")
                await asyncio.sleep(1)

    async def poll_once(self) -> None:
        entries = await self.bus.read(
            ALERTER_GROUP,
            self.consumer,
            self.streams,
            count=self.read_count,
            block_ms=self.block_ms,
            keep=self._keep,
        )
        if entries:
            await self.handle(entries)
        if time.monotonic() - self._last_liveness >= self.liveness_interval_s:
            self._last_liveness = time.monotonic()
            await self.emit(self.engine.check_liveness())
            for sev, n in self.engine.active_counts().items():
                ACTIVE_ALERTS.labels(sev.value).set(n)
            STREAM_LAG.labels(ALERTER_GROUP).set(await self.bus.lag(ALERTER_GROUP, self.streams))

    async def handle(self, entries: list[Entry]) -> None:
        alerts: list[Alert] = []
        for entry in entries:
            for frame in entry.frames:
                alerts.extend(self.engine.process(frame))
                ALERTER_SAMPLES.labels("evaluated").inc(len(frame.samples))
        await self.emit(alerts)
        # Ack after dispatch: a crash between the two means a duplicate page attempt,
        # which PagerDuty dedups. Acking first could mean a missed page.
        await self.bus.ack(ALERTER_GROUP, entries)

    async def emit(self, alerts: list[Alert]) -> None:
        if not alerts:
            return
        async with self.bus.redis.pipeline(transaction=False) as pipe:
            for a in alerts:
                pipe.xadd(
                    ALERTS_STREAM,
                    {"d": orjson.dumps(a.model_dump(mode="json"))},
                    maxlen=AUDIT_MAXLEN,
                    approximate=True,
                )
            await pipe.execute()
        # Concurrent across incidents, strictly ordered within one: a trigger and its
        # resolve in the same batch must not race each other to PagerDuty.
        by_incident: dict[str, list[Alert]] = defaultdict(list)
        for a in alerts:
            by_incident[a.dedup_key].append(a)
        await asyncio.gather(*(self._dispatch_in_order(group) for group in by_incident.values()))

    async def _dispatch_in_order(self, alerts: list[Alert]) -> None:
        for a in alerts:
            await self._dispatch(a)

    async def _dispatch(self, alert: Alert) -> None:
        await self.dispatcher.dispatch(alert)
        ALERTS.labels(alert.rule_id, alert.severity.value, alert.action.value).inc()
        if alert.action is AlertAction.TRIGGER and alert.ingested_at_ns:
            ALERT_LATENCY_SECONDS.observe((time.time_ns() - alert.ingested_at_ns) / 1e9)
