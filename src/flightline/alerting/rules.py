"""Threshold rule definitions, validated at load time so a typo fails the deploy, not
the flight."""

from __future__ import annotations

import fnmatch
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from flightline.models import Severity


class Direction(StrEnum):
    ABOVE = "above"
    BELOW = "below"


class ThresholdRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^[a-z0-9_]+$")
    channels: str
    direction: Direction
    warn: float | None = None
    critical: float | None = None
    sustain_ms: int = Field(default=0, ge=0)
    hysteresis: float = Field(default=0.0, ge=0)
    runbook: str | None = None
    _above: bool = PrivateAttr()
    _ok_bound: float = PrivateAttr()

    @model_validator(mode="after")
    def _thresholds_consistent(self) -> ThresholdRule:
        if self.warn is None and self.critical is None:
            raise ValueError(f"{self.id}: needs at least one of warn/critical")
        if self.warn is not None and self.critical is not None:
            ordered = (
                self.warn < self.critical
                if self.direction is Direction.ABOVE
                else self.warn > self.critical
            )
            if not ordered:
                raise ValueError(
                    f"{self.id}: warn must be less severe than critical for '{self.direction}'"
                )
        bounds = [t for t in (self.warn, self.critical) if t is not None]
        self._above = self.direction is Direction.ABOVE
        self._ok_bound = min(bounds) if self._above else max(bounds)
        return self

    @property
    def ok_bound(self) -> tuple[bool, float]:
        """(is_above, bound): a value strictly inside `bound` is OK with no hysteresis in
        play, so the engine can skip the state machine for nearly every sample."""
        return self._above, self._ok_bound

    def threshold(self, severity: Severity) -> float | None:
        return self.critical if severity is Severity.CRITICAL else self.warn

    def breaches(self, value: float, severity: Severity, active: Severity) -> bool:
        """Is `value` at/over the `severity` threshold? Once a level is active the bar
        to stay there is lowered by `hysteresis`, so the alert does not flap."""
        thr = self.threshold(severity)
        if thr is None:
            return False
        slack = self.hysteresis if active.rank >= severity.rank else 0.0
        if self.direction is Direction.ABOVE:
            return value >= thr - slack
        return value <= thr + slack

    def level(self, value: float, active: Severity) -> Severity:
        if self.breaches(value, Severity.CRITICAL, active):
            return Severity.CRITICAL
        if self.breaches(value, Severity.WARNING, active):
            return Severity.WARNING
        return Severity.OK


class TelemetryLossRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    timeout_s: float = Field(default=5.0, gt=0)
    severity: Severity = Severity.CRITICAL
    runbook: str | None = None


class RuleSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: list[ThresholdRule]
    telemetry_loss: TelemetryLossRule = TelemetryLossRule()

    @model_validator(mode="after")
    def _unique_ids(self) -> RuleSet:
        ids = [r.id for r in self.rules]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate rule ids: {sorted(dupes)}")
        return self

    @classmethod
    def load(cls, path: Path) -> RuleSet:
        return cls.model_validate(yaml.safe_load(path.read_text()))

    def matcher(self) -> RuleMatcher:
        return RuleMatcher(self.rules)


class RuleMatcher:
    def __init__(self, rules: list[ThresholdRule]) -> None:
        self.rules = rules
        self._cache: dict[str, tuple[ThresholdRule, ...]] = {}

    def __call__(self, channel: str) -> tuple[ThresholdRule, ...]:
        hit = self._cache.get(channel)
        if hit is None:
            hit = tuple(r for r in self.rules if fnmatch.fnmatchcase(channel, r.channels))
            self._cache[channel] = hit
        return hit
