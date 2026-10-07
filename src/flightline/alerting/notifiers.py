"""Alert delivery. PagerDuty Events API v2 is the paging path; Slack and logs are
secondary. Every alert is also appended to an audit stream regardless of delivery.

PagerDuty dedups on `dedup_key`, which is what makes at-least-once processing safe
here: a redelivered entry that re-fires the same trigger updates the existing
incident instead of opening a second one.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from flightline.models import Alert, AlertAction, Severity
from flightline.observability import NOTIFY_FAILURES

log = logging.getLogger(__name__)


class Notifier(Protocol):
    name: str

    async def send(self, alert: Alert) -> None: ...


class PermanentDeliveryError(Exception):
    """The receiver rejected the request; retrying will not help."""


class LogNotifier:
    name = "log"

    async def send(self, alert: Alert) -> None:
        level = logging.ERROR if alert.severity is Severity.CRITICAL else logging.WARNING
        if alert.action is AlertAction.RESOLVE:
            level = logging.INFO
        log.log(level, alert.summary, extra={"alert": alert.model_dump(mode="json")})


class _HttpNotifier:
    name = "http"

    def __init__(self, url: str, client: httpx.AsyncClient, runbook_base_url: str = "") -> None:
        self.url = url
        self.client = client
        self.runbook_base_url = runbook_base_url

    def runbook(self, alert: Alert) -> str | None:
        if not alert.runbook:
            return None
        if alert.runbook.startswith(("http://", "https://")) or not self.runbook_base_url:
            return alert.runbook
        return self.runbook_base_url.rstrip("/") + "/" + alert.runbook.lstrip("/")

    async def _post(self, body: dict[str, Any]) -> None:
        resp = await self.client.post(self.url, json=body)
        if resp.status_code == 429 or resp.status_code >= 500:
            resp.raise_for_status()  # retryable
        if resp.status_code >= 400:
            raise PermanentDeliveryError(f"{self.name}: HTTP {resp.status_code}: {resp.text[:200]}")


class PagerDutyNotifier(_HttpNotifier):
    name = "pagerduty"

    def __init__(
        self, url: str, routing_key: str, client: httpx.AsyncClient, runbook_base_url: str = ""
    ) -> None:
        super().__init__(url, client, runbook_base_url)
        self.routing_key = routing_key

    def payload(self, alert: Alert) -> dict[str, Any]:
        body: dict[str, Any] = {
            "routing_key": self.routing_key,
            "event_action": alert.action.value,
            "dedup_key": alert.dedup_key,
        }
        if alert.action is AlertAction.RESOLVE:
            return body
        body["payload"] = {
            "summary": alert.summary[:1024],
            "source": alert.aircraft_id,
            "severity": alert.severity.value,  # PD accepts critical|error|warning|info
            "timestamp": datetime.fromtimestamp(alert.event_ts_ns / 1e9, tz=UTC).isoformat(),
            "component": alert.channel or "telemetry",
            "group": alert.campaign,
            "class": alert.rule_id,
            "custom_details": {
                "flight_id": alert.flight_id,
                "value": alert.value,
                "threshold": alert.threshold,
            },
        }
        if runbook := self.runbook(alert):
            body["links"] = [{"href": runbook, "text": "Runbook"}]
        return body

    async def send(self, alert: Alert) -> None:
        await self._post(self.payload(alert))


class SlackNotifier(_HttpNotifier):
    name = "slack"

    async def send(self, alert: Alert) -> None:
        icon = {"critical": ":rotating_light:", "warning": ":warning:", "ok": ":white_check_mark:"}
        text = f"{icon[alert.severity.value]} {alert.summary}"
        if runbook := self.runbook(alert):
            text += f"  <{runbook}|runbook>"
        await self._post({"text": text})


class Dispatcher:
    """Fans an alert out to every notifier concurrently, each with its own retries,
    so a Slack outage cannot delay a page."""

    def __init__(
        self, notifiers: list[Notifier], max_attempts: int = 4, base_delay_s: float = 0.25
    ) -> None:
        self.notifiers = notifiers
        self.max_attempts = max_attempts
        self.base_delay_s = base_delay_s

    async def dispatch(self, alert: Alert) -> dict[str, bool]:
        results = await asyncio.gather(*(self._deliver(n, alert) for n in self.notifiers))
        return {n.name: ok for n, ok in zip(self.notifiers, results, strict=True)}

    async def _deliver(self, notifier: Notifier, alert: Alert) -> bool:
        for attempt in range(1, self.max_attempts + 1):
            try:
                await notifier.send(alert)
            except PermanentDeliveryError:
                log.exception("alert rejected", extra={"notifier": notifier.name})
                break
            except (httpx.HTTPError, OSError) as e:
                if attempt == self.max_attempts:
                    log.error(
                        "alert delivery failed",
                        extra={"notifier": notifier.name, "error": str(e), "attempts": attempt},
                    )
                    break
                # Full jitter: spreads retries from many replicas after a receiver blip.
                await asyncio.sleep(random.uniform(0, self.base_delay_s * 2**attempt))
            else:
                return True
        NOTIFY_FAILURES.labels(notifier.name).inc()
        return False
