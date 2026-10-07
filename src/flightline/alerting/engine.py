"""Stateful threshold evaluation.

Per (rule, aircraft, flight, channel) we track the active severity and, for each
level, the event time at which the signal first continuously reached it. A level
fires once it has been held for `sustain_ms` of *event* time. Using event time
rather than wall clock means a backfill or a slow consumer produces exactly the
same alerts as live processing, which also makes the engine deterministic to test.

De-escalation is immediate (hysteresis already guards against flapping): when a
reading drops back, on-call should know now, not after another debounce window.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from flightline.alerting.rules import RuleMatcher, RuleSet, ThresholdRule
from flightline.governance import GovernedFrame, GovernedSample
from flightline.models import Alert, AlertAction, FlightPhase, Quality, Severity

ESCALATION_LEVELS = (Severity.WARNING, Severity.CRITICAL)
TELEMETRY_LOSS_RULE_ID = "telemetry_loss"
# Forget flights silent this long, so a crashed DAU cannot leak state forever.
FLIGHT_STATE_TTL_S = 30 * 60


@dataclass(slots=True)
class ChannelState:
    active: Severity = Severity.OK
    since_ns: dict[Severity, int | None] = field(
        default_factory=lambda: dict.fromkeys(ESCALATION_LEVELS)
    )


@dataclass(slots=True)
class FlightState:
    campaign: str
    last_seen: float
    last_event_ns: int
    last_ingested_ns: int
    lost: bool = False


FlightKey = tuple[str, str]  # (aircraft_id, flight_id)


class AlertEngine:
    def __init__(self, rules: RuleSet, clock: Callable[[], float] = time.monotonic) -> None:
        self.rules = rules
        self.match: RuleMatcher = rules.matcher()
        self.loss = rules.telemetry_loss
        self.clock = clock
        self.channels: dict[tuple[str, str, str, str], ChannelState] = {}
        self.flights: dict[FlightKey, FlightState] = {}
        self.skipped_bad_quality = 0
        # Plain-tuple copy of each rule's fast-path bound: read per sample, and attribute
        # access on a pydantic model is several times slower than a dict lookup.
        self._ok_bounds = {r.id: r.ok_bound for r in rules.rules}

    def _clearly_ok(self, rule: ThresholdRule, value: float) -> bool:
        above, bound = self._ok_bounds[rule.id]
        return value < bound if above else value > bound

    # ---- thresholds ----------------------------------------------------------------

    def process(self, frame: GovernedFrame) -> list[Alert]:
        alerts: list[Alert] = []
        fkey = (frame.aircraft_id, frame.flight_id)
        alerts.extend(self._observe_flight(fkey, frame))

        for sample in frame.samples:
            rules = self.match(sample.channel)
            if not rules:
                continue
            if sample.quality != Quality.GOOD:
                # A physically impossible reading is a sensor fault. Paging on it would
                # train on-call to ignore pages; it is stored and visible in the lake.
                self.skipped_bad_quality += 1
                continue
            for rule in rules:
                alert = self._evaluate(rule, frame, sample)
                if alert:
                    alerts.append(alert)

        if frame.phase == FlightPhase.SHUTDOWN:
            alerts.extend(self._end_flight(fkey, frame))
        return alerts

    def _evaluate(
        self, rule: ThresholdRule, frame: GovernedFrame, sample: GovernedSample
    ) -> Alert | None:
        key = (rule.id, frame.aircraft_id, frame.flight_id, sample.channel)
        st = self.channels.get(key)
        if st is None:
            if self._clearly_ok(rule, sample.value):
                return None  # no state to keep for a channel that has never breached
            st = self.channels[key] = ChannelState()
        elif (
            st.active is Severity.OK
            and st.since_ns[Severity.WARNING] is None
            and st.since_ns[Severity.CRITICAL] is None
            and self._clearly_ok(rule, sample.value)
        ):
            return None

        ts = frame.ts_ns
        new = self._transition(rule, st, sample.value, ts)
        if new is None:
            return None
        st.active = new
        action = AlertAction.RESOLVE if new is Severity.OK else AlertAction.TRIGGER
        # On resolve, report the least severe threshold: the line the value came back under.
        threshold = rule.threshold(new if new is not Severity.OK else Severity.WARNING)
        if threshold is None:
            threshold = rule.critical
        verb = "cleared" if new is Severity.OK else f"{rule.direction.value} {threshold:g}"
        return Alert(
            rule_id=rule.id,
            action=action,
            severity=new,
            aircraft_id=frame.aircraft_id,
            flight_id=frame.flight_id,
            campaign=frame.campaign,
            channel=sample.channel,
            value=sample.value,
            threshold=threshold,
            event_ts_ns=ts,
            ingested_at_ns=frame.lineage.ingested_at_ns,
            runbook=rule.runbook,
            summary=(
                f"[{new.value.upper()}] {frame.aircraft_id} {sample.channel} = "
                f"{sample.value:.1f} {sample.unit} ({verb}) on flight {frame.flight_id}"
            ),
        )

    @staticmethod
    def _transition(
        rule: ThresholdRule, st: ChannelState, value: float, ts: int
    ) -> Severity | None:
        """Advance per-level sustain timers; return the new active level, if it changed."""
        raw = rule.level(value, st.active)
        for lvl in ESCALATION_LEVELS:
            if raw.rank >= lvl.rank:
                if st.since_ns[lvl] is None:
                    st.since_ns[lvl] = ts
            else:
                st.since_ns[lvl] = None

        sustain_ns = rule.sustain_ms * 1_000_000
        sustained = Severity.OK
        for lvl in ESCALATION_LEVELS:
            since = st.since_ns[lvl]
            if since is not None and ts - since >= sustain_ns:
                sustained = lvl

        if raw.rank < st.active.rank:
            return raw
        if sustained.rank > st.active.rank:
            return sustained
        return None

    # ---- telemetry loss ------------------------------------------------------------

    def _observe_flight(self, fkey: FlightKey, frame: GovernedFrame) -> list[Alert]:
        st = self.flights.get(fkey)
        now = self.clock()
        if st is None:
            self.flights[fkey] = FlightState(
                frame.campaign, now, frame.ts_ns, frame.lineage.ingested_at_ns
            )
            return []
        st.last_seen = now
        st.last_event_ns = max(st.last_event_ns, frame.ts_ns)
        st.last_ingested_ns = frame.lineage.ingested_at_ns
        if st.lost:
            st.lost = False
            return [self._loss_alert(fkey, st, AlertAction.RESOLVE)]
        return []

    def check_liveness(self) -> list[Alert]:
        """Called on a timer. Fires once per flight when frames stop arriving."""
        now = self.clock()
        alerts: list[Alert] = []
        for fkey, st in list(self.flights.items()):
            silent = now - st.last_seen
            if silent > FLIGHT_STATE_TTL_S:
                self._forget(fkey)
            elif not st.lost and silent > self.loss.timeout_s:
                st.lost = True
                alerts.append(self._loss_alert(fkey, st, AlertAction.TRIGGER))
        return alerts

    def _end_flight(self, fkey: FlightKey, frame: GovernedFrame) -> list[Alert]:
        """A clean shutdown: resolve anything still open and drop the flight's state."""
        alerts: list[Alert] = []
        for key, st in list(self.channels.items()):
            if (key[1], key[2]) == fkey and st.active is not Severity.OK:
                rule_id, _, _, channel = key
                alerts.append(
                    Alert(
                        rule_id=rule_id,
                        action=AlertAction.RESOLVE,
                        severity=Severity.OK,
                        aircraft_id=fkey[0],
                        flight_id=fkey[1],
                        campaign=frame.campaign,
                        channel=channel,
                        value=None,
                        threshold=None,
                        event_ts_ns=frame.ts_ns,
                        ingested_at_ns=frame.lineage.ingested_at_ns,
                        runbook=None,
                        summary=f"{fkey[0]} flight {fkey[1]} shut down; auto-resolving",
                    )
                )
        self._forget(fkey)
        return alerts

    def _forget(self, fkey: FlightKey) -> None:
        self.flights.pop(fkey, None)
        for key in [k for k in self.channels if (k[1], k[2]) == fkey]:
            del self.channels[key]

    def _loss_alert(self, fkey: FlightKey, st: FlightState, action: AlertAction) -> Alert:
        severity = self.loss.severity if action is AlertAction.TRIGGER else Severity.OK
        what = (
            f"no telemetry for >{self.loss.timeout_s:g}s"
            if action is AlertAction.TRIGGER
            else "telemetry restored"
        )
        return Alert(
            rule_id=TELEMETRY_LOSS_RULE_ID,
            action=action,
            severity=severity,
            aircraft_id=fkey[0],
            flight_id=fkey[1],
            campaign=st.campaign,
            channel=None,
            value=None,
            threshold=self.loss.timeout_s,
            event_ts_ns=st.last_event_ns,
            ingested_at_ns=st.last_ingested_ns if action is AlertAction.RESOLVE else None,
            runbook=self.loss.runbook,
            summary=f"[{severity.value.upper()}] {fkey[0]} flight {fkey[1]}: {what}",
        )

    def active_counts(self) -> dict[Severity, int]:
        counts = dict.fromkeys(ESCALATION_LEVELS, 0)
        for st in self.channels.values():
            if st.active is not Severity.OK:
                counts[st.active] += 1
        for f in self.flights.values():
            if f.lost:
                counts[self.loss.severity] += 1
        return counts
