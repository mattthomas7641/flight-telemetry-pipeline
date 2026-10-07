"""Wire and domain models.

A *frame* is one timestamped snapshot from one data acquisition unit (DAU): many
channels sampled at the same instant. Frames are the unit of ingest, ordering and
quarantine. A *row* is one (frame, channel) sample, the unit of storage.
"""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")
]
ChannelName = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")
]


class FlightPhase(StrEnum):
    PREFLIGHT = "preflight"
    HOVER = "hover"
    TRANSITION = "transition"
    CRUISE = "cruise"
    LANDED = "landed"
    SHUTDOWN = "shutdown"


class TelemetryFrame(BaseModel):
    """What a DAU sends us."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    aircraft_id: Identifier
    flight_id: Identifier
    campaign: Identifier
    source_id: Identifier
    seq: int = Field(ge=0, description="Monotonic per source; (source_id, seq) is the frame key.")
    ts_ns: int = Field(gt=0, description="Sample time, UTC nanoseconds since epoch.")
    phase: FlightPhase | None = None
    samples: dict[ChannelName, float] = Field(min_length=1, max_length=4096)

    @field_validator("samples")
    @classmethod
    def _finite(cls, v: dict[str, float]) -> dict[str, float]:
        bad = [k for k, x in v.items() if not math.isfinite(x)]
        if bad:
            raise ValueError(f"non-finite sample values for channels: {sorted(bad)[:5]}")
        return v

    @property
    def frame_id(self) -> str:
        return f"{self.source_id}:{self.seq}"


class Quality(StrEnum):
    GOOD = "good"
    # Outside what the sensor can physically report: a sensor or wiring fault,
    # not a real excursion. Stored (engineers need to see it) but never paged on.
    OUT_OF_PHYSICAL_RANGE = "out_of_physical_range"


class Severity(StrEnum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return _SEVERITY_RANK[self]


_SEVERITY_RANK = {Severity.OK: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}


class AlertAction(StrEnum):
    TRIGGER = "trigger"
    RESOLVE = "resolve"


class Alert(BaseModel):
    model_config = ConfigDict(frozen=True)

    rule_id: str
    action: AlertAction
    severity: Severity
    aircraft_id: str
    flight_id: str
    campaign: str
    channel: str | None
    value: float | None
    threshold: float | None
    event_ts_ns: int
    ingested_at_ns: int | None
    runbook: str | None
    summary: str

    @property
    def dedup_key(self) -> str:
        """Stable across severities so warn -> critical escalates the same incident."""
        return f"{self.rule_id}/{self.aircraft_id}/{self.flight_id}/{self.channel or '-'}"
