from __future__ import annotations

from collections.abc import Callable

import pytest

from flightline.alerting.engine import TELEMETRY_LOSS_RULE_ID, AlertEngine
from flightline.alerting.rules import RuleSet, ThresholdRule
from flightline.governance import GovernedFrame
from flightline.models import Alert, AlertAction, Severity

MS = 1_000_000
T0 = 1_700_000_000_000_000_000  # fixed event-time origin (2023), never "future"
MOTOR = "motor.7.winding_temp_c"  # warn 140, critical 165, sustain 400 ms, hysteresis 5


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def engine(rules: RuleSet, clock: Clock) -> AlertEngine:
    return AlertEngine(rules, clock=clock)


def feed(
    engine: AlertEngine,
    make_frame: Callable[..., GovernedFrame],
    values: list[float],
    *,
    channel: str = MOTOR,
    step_ms: int = 100,
    start_ms: int = 0,
) -> list[Alert]:
    out: list[Alert] = []
    for i, v in enumerate(values):
        out += engine.process(make_frame({channel: v}, ts_ns=T0 + (start_ms + i * step_ms) * MS))
    return [a for a in out if a.rule_id != TELEMETRY_LOSS_RULE_ID]


def test_nominal_values_never_alert(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    assert feed(engine, make_frame, [90, 110, 120, 139.9] * 10) == []


def test_single_spike_is_filtered_by_sustain(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    assert feed(engine, make_frame, [100, 200, 100, 100]) == []


def test_fires_once_level_is_held_for_sustain_window(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    # 100 ms cadence: first breach at t=100, sustained 400 ms at t=500.
    alerts = feed(engine, make_frame, [100, 145, 145, 145, 145, 145, 145])
    assert len(alerts) == 1
    a = alerts[0]
    assert (a.action, a.severity, a.threshold) == (AlertAction.TRIGGER, Severity.WARNING, 140)
    assert a.event_ts_ns == T0 + 500 * MS
    assert a.runbook and "motor-winding-overtemp" in a.runbook


def test_escalates_warning_to_critical_on_same_incident(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    alerts = feed(engine, make_frame, [145] * 5 + [170] * 5)
    assert [a.severity for a in alerts] == [Severity.WARNING, Severity.CRITICAL]
    assert alerts[0].dedup_key == alerts[1].dedup_key


def test_warning_sustain_counts_from_first_breach_even_when_jumping_to_critical(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    # Straight to critical: both levels have been held since t=0, so after 400 ms the
    # highest sustained level fires directly, with no intermediate warning page.
    alerts = feed(engine, make_frame, [170] * 6)
    assert [a.severity for a in alerts] == [Severity.CRITICAL]


def test_hysteresis_prevents_flapping_at_threshold(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    alerts = feed(engine, make_frame, [145] * 5 + [139, 141, 138, 141, 136] * 4)
    assert [a.action for a in alerts] == [AlertAction.TRIGGER]


def test_resolves_once_back_below_hysteresis_band(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    alerts = feed(engine, make_frame, [145] * 5 + [134])
    assert [a.action for a in alerts] == [AlertAction.TRIGGER, AlertAction.RESOLVE]
    assert alerts[-1].severity is Severity.OK


def test_deescalates_critical_to_warning_immediately(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    alerts = feed(engine, make_frame, [170] * 5 + [150])
    assert [a.severity for a in alerts] == [Severity.CRITICAL, Severity.WARNING]


def test_below_direction_rule(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    alerts = feed(engine, make_frame, [700] + [630] * 6 + [655], channel="battery.pack_voltage_v")
    assert [(a.action, a.severity) for a in alerts] == [
        (AlertAction.TRIGGER, Severity.WARNING),
        (AlertAction.RESOLVE, Severity.OK),
    ]


def test_sensor_fault_readings_never_page(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    assert feed(engine, make_frame, [9999.0] * 20) == []
    assert engine.skipped_bad_quality == 20


def test_state_is_isolated_per_aircraft(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    out: list[Alert] = []
    for i in range(6):
        ts = T0 + i * 100 * MS
        out += engine.process(make_frame({MOTOR: 150.0}, aircraft_id="N301FL", ts_ns=ts))
        out += engine.process(make_frame({MOTOR: 100.0}, aircraft_id="N302FL", ts_ns=ts))
    assert {a.aircraft_id for a in out} == {"N301FL"}


def test_shutdown_resolves_open_alerts_and_clears_state(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame]
) -> None:
    feed(engine, make_frame, [170] * 6)
    alerts = engine.process(make_frame({MOTOR: 170.0}, ts_ns=T0 + 10**9, phase="shutdown"))
    assert [a.action for a in alerts] == [AlertAction.RESOLVE]
    assert engine.channels == {}
    assert engine.flights == {}


def test_telemetry_loss_fires_after_timeout_and_resolves_on_return(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame], clock: Clock
) -> None:
    engine.process(make_frame({MOTOR: 90.0}))
    clock.t = 3.0
    assert engine.check_liveness() == []
    clock.t = 6.0
    [lost] = engine.check_liveness()
    assert (lost.rule_id, lost.action, lost.severity) == (
        TELEMETRY_LOSS_RULE_ID,
        AlertAction.TRIGGER,
        Severity.CRITICAL,
    )
    assert engine.check_liveness() == []  # fires once, not every tick
    assert engine.active_counts()[Severity.CRITICAL] == 1

    clock.t = 7.0
    [restored] = engine.process(make_frame({MOTOR: 90.0}))
    assert (restored.action, restored.dedup_key) == (AlertAction.RESOLVE, lost.dedup_key)


def test_silent_flights_are_eventually_forgotten(
    engine: AlertEngine, make_frame: Callable[..., GovernedFrame], clock: Clock
) -> None:
    engine.process(make_frame({MOTOR: 90.0}))
    clock.t = 10.0
    engine.check_liveness()
    clock.t = 31 * 60
    engine.check_liveness()
    assert engine.flights == {}


def test_replaying_events_is_deterministic(
    rules: RuleSet, make_frame: Callable[..., GovernedFrame]
) -> None:
    """Event-time semantics: the same input always yields the same alerts, which is what
    makes reprocessing after a crash or a backfill safe."""
    values = [100, 150, 150, 150, 150, 150, 170, 170, 170, 170, 170, 120, 100]
    runs = [
        [
            (a.action, a.severity, a.event_ts_ns)
            for a in feed(AlertEngine(rules), make_frame, values)
        ]
        for _ in range(2)
    ]
    assert runs[0] == runs[1] and runs[0]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"direction": "above"}, "at least one"),
        ({"direction": "above", "warn": 10, "critical": 5}, "less severe"),
        ({"direction": "below", "warn": 5, "critical": 10}, "less severe"),
    ],
)
def test_rule_validation(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ThresholdRule.model_validate({"id": "r", "channels": "x", **kwargs})


def test_duplicate_rule_ids_rejected() -> None:
    rule = {"id": "r", "channels": "x", "direction": "above", "warn": 1}
    with pytest.raises(ValueError, match="duplicate"):
        RuleSet.model_validate({"rules": [rule, rule]})
