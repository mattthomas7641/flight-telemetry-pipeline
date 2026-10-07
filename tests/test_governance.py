from __future__ import annotations

import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from flightline.governance import (
    Catalog,
    GovernedFrame,
    Quarantined,
    QuarantineReason,
    Tagger,
)
from flightline.models import Quality, TelemetryFrame
from flightline.streams import decode_entry, encode_entry
from tests.conftest import raw_frame


def tag(tagger: Tagger, samples: dict[str, float], **kw: object) -> GovernedFrame | Quarantined:
    return tagger.tag(TelemetryFrame.model_validate(raw_frame(samples, **kw)), ingest_id="i1")


def test_every_sample_is_tagged_with_policy_and_lineage(tagger: Tagger, catalog: Catalog) -> None:
    out = tag(tagger, {"motor.4.winding_temp_c": 90.0, "nav.altitude_ft_agl": 1200.0})
    assert isinstance(out, GovernedFrame)

    by_channel = {s.channel: s for s in out.samples}
    motor = by_channel["motor.4.winding_temp_c"]
    assert (motor.classification, motor.owner, motor.retention, motor.unit) == (
        "proprietary",
        "propulsion",
        "flight_test",
        "degC",
    )
    assert by_channel["nav.altitude_ft_agl"].classification == "export_controlled"
    assert out.lineage.ingest_id == "i1"
    assert out.lineage.catalog_version == catalog.version


def test_unregistered_channel_quarantines_the_whole_frame(tagger: Tagger) -> None:
    out = tag(tagger, {"motor.1.rpm": 4000.0, "motor.1.secret_debug": 1.0})
    assert isinstance(out, Quarantined)
    assert out.reason is QuarantineReason.UNREGISTERED_CHANNEL
    assert "motor.1.secret_debug" in out.detail
    assert out.frame["samples"]["motor.1.rpm"] == 4000.0  # raw frame kept for triage


def test_physically_impossible_reading_is_kept_but_flagged(tagger: Tagger) -> None:
    out = tag(tagger, {"motor.3.winding_temp_c": 9999.0})
    assert isinstance(out, GovernedFrame)
    assert out.samples[0].quality == Quality.OUT_OF_PHYSICAL_RANGE


def test_future_timestamp_is_quarantined_as_clock_skew(tagger: Tagger) -> None:
    out = tag(tagger, {"motor.1.rpm": 1.0}, ts_ns=time.time_ns() + 120 * 10**9)
    assert isinstance(out, Quarantined)
    assert out.reason is QuarantineReason.CLOCK_SKEW


def test_old_timestamp_is_accepted_for_backfill(tagger: Tagger) -> None:
    week_ago = time.time_ns() - 7 * 86_400 * 10**9
    assert isinstance(tag(tagger, {"motor.1.rpm": 1.0}, ts_ns=week_ago), GovernedFrame)


def test_wire_round_trip_is_lossless(tagger: Tagger) -> None:
    out = tag(tagger, {"motor.1.rpm": 4000.0, "battery.soc_pct": 55.5})
    assert isinstance(out, GovernedFrame)
    assert decode_entry(encode_entry([out])) == [out]


def test_wire_round_trip_preserves_bad_quality(tagger: Tagger) -> None:
    out = tag(tagger, {"motor.3.winding_temp_c": 9999.0, "motor.1.rpm": 1.0})
    assert isinstance(out, GovernedFrame)
    assert decode_entry(encode_entry([out])) == [out]


def test_unknown_codec_version_is_rejected() -> None:
    import zlib

    with pytest.raises(ValueError, match="codec"):
        decode_entry(zlib.compress(b'{"v": 99, "pol": {}, "f": []}'))


def test_catalog_version_changes_with_content(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "config" / "catalog.yaml"
    a = tmp_path / "a.yaml"
    a.write_text(src.read_text())
    b = tmp_path / "b.yaml"
    b.write_text(src.read_text().replace("owner: structures", "owner: loads"))
    assert Catalog.load(a).version != Catalog.load(b).version
    assert Catalog.load(a).version == Catalog.load(src).version


def test_first_matching_pattern_wins(catalog: Catalog) -> None:
    policy = catalog.resolve("battery.pack_voltage_v")
    assert policy is not None
    assert policy.pattern == "battery.pack_voltage_v"
    assert catalog.resolve("unknown.channel") is None


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("classification: proprietary", "classification: top_secret"),
        ("retention: engineering", "retention: forever"),
    ],
)
def test_catalog_rejects_dangling_references(tmp_path: Path, mutation: str, message: str) -> None:
    src = Path(__file__).resolve().parents[1] / "config" / "catalog.yaml"
    bad = tmp_path / "bad.yaml"
    bad.write_text(src.read_text().replace(mutation, message, 1))
    with pytest.raises(ValidationError, match="unknown"):
        Catalog.load(bad)


@pytest.mark.parametrize(
    "bad",
    [
        {"samples": {}},
        {"samples": {"motor.1.rpm": float("nan")}},
        {"samples": {"Motor 1 RPM": 1.0}},
        {"aircraft_id": "../../etc"},
        {"seq": -1},
    ],
)
def test_frame_schema_rejects_malformed_input(bad: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TelemetryFrame.model_validate({**raw_frame(), **bad})
