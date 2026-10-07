"""A stand-in for the PagerDuty Events API v2, for local demos and e2e tests.

It applies PagerDuty's dedup semantics (one incident per dedup_key, trigger updates,
resolve closes), so you can watch real incident lifecycles without an account.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException

log = logging.getLogger("pager")


def create_app() -> FastAPI:
    app = FastAPI(title="Pager mock (PagerDuty Events v2)")
    incidents: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []

    @app.post("/v2/enqueue", status_code=202)
    async def enqueue(event: dict[str, Any]) -> dict[str, str]:
        action, key = event.get("event_action"), event.get("dedup_key")
        if action not in {"trigger", "resolve", "acknowledge"} or not key:
            raise HTTPException(400, "invalid event")
        events.append({**event, "received_at": time.time()})
        inc = incidents.get(key)
        if action == "trigger":
            payload = event.get("payload", {})
            if inc is None or inc["status"] == "resolved":
                inc = incidents[key] = {"dedup_key": key, "triggers": 0, "opened_at": time.time()}
            inc.update(
                status="triggered",
                severity=payload.get("severity"),
                summary=payload.get("summary"),
                triggers=inc["triggers"] + 1,
            )
            log.warning("PAGE %s", payload.get("summary"))
        elif inc is not None:
            inc["status"] = "resolved" if action == "resolve" else "acknowledged"
            log.info("%s %s", action.upper(), key)
        return {"status": "success", "dedup_key": key}

    @app.get("/incidents")
    async def list_incidents() -> list[dict[str, Any]]:
        return list(incidents.values())

    @app.get("/events")
    async def list_events() -> list[dict[str, Any]]:
        return events

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
