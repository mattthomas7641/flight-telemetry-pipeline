"""Governance at ingest: every sample is classified, owned, retained and given lineage
*before* it is enqueued, so there is no window in which untagged data exists.

The catalog is versioned by content hash, and that version is stamped on every row,
so for any stored value you can answer "under which policy was this classified?".
"""

from __future__ import annotations

import fnmatch
import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from flightline import __version__
from flightline.models import Quality, TelemetryFrame

_GOOD = Quality.GOOD.value
_OUT_OF_RANGE = Quality.OUT_OF_PHYSICAL_RANGE.value


class QuarantineReason(StrEnum):
    SCHEMA_INVALID = "schema_invalid"
    UNREGISTERED_CHANNEL = "unregistered_channel"
    CLOCK_SKEW = "clock_skew"


class ChannelPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    pattern: str
    unit: str
    classification: str
    owner: str
    retention: str
    physical_range: tuple[float, float]

    @model_validator(mode="after")
    def _range_ordered(self) -> ChannelPolicy:
        lo, hi = self.physical_range
        if lo >= hi:
            raise ValueError(f"{self.pattern}: physical_range must be [low, high] with low < high")
        return self


class CatalogSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int
    retention_classes: dict[str, int] = Field(min_length=1)
    classifications: list[str] = Field(min_length=1)
    channels: list[ChannelPolicy] = Field(min_length=1)

    @model_validator(mode="after")
    def _references_resolve(self) -> CatalogSpec:
        for ch in self.channels:
            if ch.classification not in self.classifications:
                raise ValueError(f"{ch.pattern}: unknown classification {ch.classification!r}")
            if ch.retention not in self.retention_classes:
                raise ValueError(f"{ch.pattern}: unknown retention class {ch.retention!r}")
        return self


class Catalog:
    """Resolves channel names to policies. Resolution is memoised: the channel set of a
    flight-test program is small and stable, so after warm-up this is a dict lookup."""

    def __init__(self, spec: CatalogSpec, version: str) -> None:
        self.spec = spec
        self.version = version
        self._cache: dict[str, ChannelPolicy | None] = {}

    @classmethod
    def load(cls, path: Path) -> Catalog:
        raw = path.read_bytes()
        spec = CatalogSpec.model_validate(yaml.safe_load(raw))
        digest = hashlib.sha256(raw).hexdigest()[:10]
        return cls(spec, version=f"v{spec.version}-{digest}")

    def resolve(self, channel: str) -> ChannelPolicy | None:
        try:
            return self._cache[channel]
        except KeyError:
            match = next(
                (p for p in self.spec.channels if fnmatch.fnmatchcase(channel, p.pattern)), None
            )
            self._cache[channel] = match
            return match

    def retention_days(self, retention_class: str) -> int:
        return self.spec.retention_classes[retention_class]


@dataclass(slots=True)
class Lineage:
    ingest_id: str
    ingested_at_ns: int
    catalog_version: str
    pipeline_version: str = __version__


@dataclass(slots=True)
class GovernedSample:
    channel: str
    value: float
    unit: str
    quality: str
    classification: str
    retention: str
    owner: str


@dataclass(slots=True)
class GovernedFrame:
    aircraft_id: str
    flight_id: str
    campaign: str
    source_id: str
    seq: int
    ts_ns: int
    phase: str | None
    lineage: Lineage
    samples: list[GovernedSample] = field(default_factory=list)

    @property
    def frame_id(self) -> str:
        return f"{self.source_id}:{self.seq}"

    # Samples travel as [channel, value] (+ quality when not good). Their policy tags
    # are dictionary-encoded once per stream entry by streams.encode_entry: every frame
    # in an entry comes from one ingest request, so one catalog version, so one policy
    # per channel.
    def to_wire(self) -> dict[str, Any]:
        lin = self.lineage
        return {
            "a": self.aircraft_id,
            "f": self.flight_id,
            "c": self.campaign,
            "src": self.source_id,
            "q": self.seq,
            "t": self.ts_ns,
            "p": self.phase,
            "l": [lin.ingest_id, lin.ingested_at_ns, lin.catalog_version, lin.pipeline_version],
            "s": [
                [s.channel, s.value] if s.quality == _GOOD else [s.channel, s.value, s.quality]
                for s in self.samples
            ],
        }

    @classmethod
    def from_wire(
        cls,
        d: dict[str, Any],
        policies: dict[str, list[str]],
        keep: Callable[[str], bool] | None = None,
    ) -> GovernedFrame:
        """`keep` lets a consumer that only needs some channels (the alerter) skip
        materialising the rest, which is most of the decode cost."""
        samples = []
        for s in d["s"]:
            if keep is not None and not keep(s[0]):
                continue
            unit, classification, retention, owner = policies[s[0]]
            quality = s[2] if len(s) > 2 else _GOOD
            samples.append(
                GovernedSample(s[0], s[1], unit, quality, classification, retention, owner)
            )
        return cls(
            aircraft_id=d["a"],
            flight_id=d["f"],
            campaign=d["c"],
            source_id=d["src"],
            seq=d["q"],
            ts_ns=d["t"],
            phase=d["p"],
            lineage=Lineage(*d["l"]),
            samples=samples,
        )

    def policies(self) -> dict[str, list[str]]:
        return {s.channel: [s.unit, s.classification, s.retention, s.owner] for s in self.samples}


@dataclass(slots=True)
class Quarantined:
    reason: QuarantineReason
    detail: str
    frame: dict[str, Any]
    ingest_id: str
    received_at_ns: int

    def to_wire(self) -> dict[str, Any]:
        return {
            "reason": self.reason.value,
            "detail": self.detail,
            "frame": self.frame,
            "ingest_id": self.ingest_id,
            "received_at_ns": self.received_at_ns,
        }


class Tagger:
    def __init__(self, catalog: Catalog, max_clock_skew_s: float = 30.0) -> None:
        self.catalog = catalog
        self.max_clock_skew_ns = int(max_clock_skew_s * 1e9)

    def tag(
        self, frame: TelemetryFrame, ingest_id: str, now_ns: int | None = None
    ) -> GovernedFrame | Quarantined:
        now_ns = now_ns or time.time_ns()

        # Future-dated data is a clock fault on the DAU. Past data is allowed:
        # backfilling from onboard recorders after a flight is normal.
        if frame.ts_ns - now_ns > self.max_clock_skew_ns:
            skew_s = (frame.ts_ns - now_ns) / 1e9
            return self._quarantine(
                frame, QuarantineReason.CLOCK_SKEW, f"{skew_s:.1f}s ahead", ingest_id, now_ns
            )

        resolve = self.catalog.resolve
        samples: list[GovernedSample] = []
        unregistered: list[str] = []
        for channel, value in frame.samples.items():
            policy = resolve(channel)
            if policy is None:
                unregistered.append(channel)
                continue
            lo, hi = policy.physical_range
            # Hot path (~40 samples x thousands of frames/s): positional construction and
            # pre-resolved enum strings measurably beat keyword args and Enum.value.
            samples.append(
                GovernedSample(
                    channel,
                    value,
                    policy.unit,
                    _GOOD if lo <= value <= hi else _OUT_OF_RANGE,
                    policy.classification,
                    policy.retention,
                    policy.owner,
                )
            )

        # Whole-frame quarantine (not per-channel drop): a DAU emitting an unknown
        # channel is misconfigured, and silently keeping the rest hides that.
        if unregistered:
            return self._quarantine(
                frame,
                QuarantineReason.UNREGISTERED_CHANNEL,
                ",".join(sorted(unregistered)[:10]),
                ingest_id,
                now_ns,
            )

        return GovernedFrame(
            aircraft_id=frame.aircraft_id,
            flight_id=frame.flight_id,
            campaign=frame.campaign,
            source_id=frame.source_id,
            seq=frame.seq,
            ts_ns=frame.ts_ns,
            phase=frame.phase.value if frame.phase else None,
            lineage=Lineage(ingest_id, now_ns, self.catalog.version),
            samples=samples,
        )

    @staticmethod
    def _quarantine(
        frame: TelemetryFrame, reason: QuarantineReason, detail: str, ingest_id: str, now_ns: int
    ) -> Quarantined:
        return Quarantined(reason, detail, frame.model_dump(mode="json"), ingest_id, now_ns)
