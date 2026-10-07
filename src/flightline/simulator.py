"""Flight-test DAU simulator for a 12-rotor tilt-wing eVTOL.

Produces physically plausible telemetry through a full flight profile (hover ->
transition -> wing-borne cruise with lift rotors stowed -> back), plus scripted
failure scenarios that exercise every path in the pipeline:

    nominal         nothing fires
    motor-overtemp  motor 7 winding heats past warn, then critical  -> pages
    sensor-fault    motor 3 reports an impossible temperature       -> stored, not paged
    dropout         the DAU goes silent for 8 s                     -> telemetry_loss
    rogue-channel   an unregistered channel appears                 -> quarantined
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import httpx

from flightline.models import FlightPhase

log = logging.getLogger(__name__)

TILT_MOTORS = range(1, 7)  # forward, tilt with the wing: thrust in hover and cruise
LIFT_MOTORS = range(7, 13)  # aft, fixed: hover only, stowed in cruise
BATTERY_PACKS = range(1, 7)
AMBIENT_C = 24.0


class Scenario(StrEnum):
    NOMINAL = "nominal"
    MOTOR_OVERTEMP = "motor-overtemp"
    SENSOR_FAULT = "sensor-fault"
    DROPOUT = "dropout"
    ROGUE_CHANNEL = "rogue-channel"


# (phase, start fraction of flight). Short hover segments, long cruise.
PROFILE: list[tuple[FlightPhase, float]] = [
    (FlightPhase.PREFLIGHT, 0.00),
    (FlightPhase.HOVER, 0.05),
    (FlightPhase.TRANSITION, 0.15),
    (FlightPhase.CRUISE, 0.22),
    (FlightPhase.TRANSITION, 0.80),
    (FlightPhase.HOVER, 0.87),
    (FlightPhase.LANDED, 0.97),
]


def phase_at(frac: float) -> FlightPhase:
    current = PROFILE[0][0]
    for phase, start in PROFILE:
        if frac >= start:
            current = phase
    return current


@dataclass
class AircraftModel:
    aircraft_id: str
    flight_id: str
    campaign: str
    duration_s: float
    scenario: Scenario = Scenario.NOMINAL
    seed: int | None = None
    rng: random.Random = field(init=False)
    winding_c: dict[int, float] = field(init=False)
    cell_c: dict[int, float] = field(init=False)
    soc: float = 96.0
    altitude_ft: float = 0.0
    airspeed_kt: float = 0.0
    seq: int = 0

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.winding_c = {m: AMBIENT_C for m in (*TILT_MOTORS, *LIFT_MOTORS)}
        self.cell_c = dict.fromkeys(BATTERY_PACKS, AMBIENT_C + 2)

    @property
    def source_id(self) -> str:
        return f"{self.aircraft_id}-dau1"

    def scenario_active(self, frac: float) -> bool:
        """Scenarios trigger mid-cruise, when the aircraft is furthest from a pad."""
        return 0.35 <= frac <= 0.60

    def in_dropout(self, frac: float) -> bool:
        if self.scenario is not Scenario.DROPOUT:
            return False
        start = 0.45 * self.duration_s
        return start <= frac * self.duration_s <= start + 8.0

    def step(self, t: float, dt: float) -> tuple[FlightPhase, dict[str, float]]:
        frac = min(t / self.duration_s, 1.0)
        phase = phase_at(frac)
        n = self.rng.gauss

        # Commanded state per phase.
        if phase is FlightPhase.HOVER:
            tilt_load, lift_load, alt_target, spd_target = 0.85, 0.85, 400.0, 0.0
        elif phase is FlightPhase.TRANSITION:
            tilt_load, lift_load, alt_target, spd_target = 0.90, 0.55, 900.0, 80.0
        elif phase is FlightPhase.CRUISE:
            tilt_load, lift_load, alt_target, spd_target = 0.60, 0.0, 2000.0, 130.0
        else:
            tilt_load, lift_load, alt_target, spd_target = 0.0, 0.0, 0.0, 0.0

        self.altitude_ft += (alt_target - self.altitude_ft) * min(1.0, 0.08 * dt)
        self.airspeed_kt += (spd_target - self.airspeed_kt) * min(1.0, 0.15 * dt)

        s: dict[str, float] = {}
        total_load = 0.0
        for m, temp in self.winding_c.items():
            load = tilt_load if m in TILT_MOTORS else lift_load
            total_load += load
            target = AMBIENT_C + 95.0 * load
            if self.scenario is Scenario.MOTOR_OVERTEMP and m == 7 and self.scenario_active(frac):
                # A failing bearing in a lift motor: heats even though it should be stowed.
                target = 195.0
            # First-order thermal lag (tau ~= 12 s); fast enough to see in a short demo.
            self.winding_c[m] = temp + (target - temp) * min(1.0, dt / 12.0)
            s[f"motor.{m}.winding_temp_c"] = round(self.winding_c[m] + n(0, 0.4), 2)
            s[f"motor.{m}.rpm"] = round(max(0.0, 6200 * load + n(0, 25) * (load > 0)), 1)

        current_a = 120 * total_load + n(0, 5) * (total_load > 0)
        self.soc = max(5.0, self.soc - current_a * dt * 0.00009)
        voltage = 610 + 2.0 * self.soc - 0.012 * current_a
        for p, temp in self.cell_c.items():
            target = AMBIENT_C + 4 + current_a * 0.018
            self.cell_c[p] = temp + (target - temp) * min(1.0, dt / 40.0)
            s[f"battery.{p}.cell_temp_max_c"] = round(self.cell_c[p] + n(0, 0.2), 2)
        s["battery.pack_voltage_v"] = round(voltage + n(0, 0.8), 1)
        s["battery.pack_current_a"] = round(current_a, 1)
        s["battery.soc_pct"] = round(self.soc, 2)

        s["nav.altitude_ft_agl"] = round(self.altitude_ft + n(0, 1.5), 1)
        s["nav.airspeed_kt"] = round(max(0.0, self.airspeed_kt + n(0, 0.6)), 1)
        s["fcs.pitch_deg"] = round(2.0 * math.sin(t / 7) + n(0, 0.2), 2)
        s["fcs.roll_deg"] = round(1.5 * math.sin(t / 5) + n(0, 0.2), 2)
        vib_base = 0.4 + 1.1 * (phase is FlightPhase.TRANSITION) + 0.6 * total_load / 12
        s["struct.vibration_g_rms"] = round(abs(vib_base + n(0, 0.08)), 3)

        if self.scenario is Scenario.SENSOR_FAULT and self.scenario_active(frac):
            s["motor.3.winding_temp_c"] = 9999.0  # open-circuit thermocouple
        if self.scenario is Scenario.ROGUE_CHANNEL and self.scenario_active(frac):
            s["motor.3.esc_firmware_dbg"] = float(self.rng.randint(0, 255))
        return phase, s

    def frame(self, t: float, dt: float, ts_ns: int) -> dict[str, Any]:
        phase, samples = self.step(t, dt)
        if t >= self.duration_s:
            phase = FlightPhase.SHUTDOWN
        self.seq += 1
        return {
            "aircraft_id": self.aircraft_id,
            "flight_id": self.flight_id,
            "campaign": self.campaign,
            "source_id": self.source_id,
            "seq": self.seq,
            "ts_ns": ts_ns,
            "phase": phase.value,
            "samples": samples,
        }


def synthetic_frames(
    n: int, aircraft: int = 4, hz: int = 50, seed: int = 7
) -> Iterator[dict[str, Any]]:
    """Deterministic frames for tests and benchmarks (no wall-clock dependency)."""
    models = [
        AircraftModel(f"N{301 + i}FL", f"F{1000 + i}", "FT-BENCH", duration_s=600, seed=seed + i)
        for i in range(aircraft)
    ]
    t0 = time.time_ns()
    dt = 1 / hz
    for k in range(n):
        m = models[k % aircraft]
        tick = k // aircraft
        yield m.frame(t=60 + tick * dt, dt=dt, ts_ns=t0 + int(tick * dt * 1e9))


@dataclass
class SimulationResult:
    frames_sent: int = 0
    frames_accepted: int = 0
    frames_quarantined: int = 0
    retries: int = 0


async def fly(
    url: str,
    *,
    aircraft: int = 2,
    duration_s: float = 120.0,
    hz: int = 50,
    batch_ms: int = 200,
    scenario: Scenario = Scenario.NOMINAL,
    campaign: str = "FT-2026-TRANSITION",
    api_key: str | None = None,
    seed: int | None = None,
    first_tail: int = 301,
) -> SimulationResult:
    """Fly `aircraft` simulated aircraft in real time against the ingest API. The
    scenario is applied to the first aircraft only, so the others act as controls."""
    run = int(time.time()) % 100_000
    models = [
        AircraftModel(
            aircraft_id=f"N{first_tail + i}FL",
            flight_id=f"F{run}-{first_tail + i}",
            campaign=campaign,
            duration_s=duration_s,
            scenario=scenario if i == 0 else Scenario.NOMINAL,
            seed=None if seed is None else seed + i,
        )
        for i in range(aircraft)
    ]
    headers = {"authorization": f"Bearer {api_key}"} if api_key else {}
    result = SimulationResult()
    async with httpx.AsyncClient(base_url=url, headers=headers, timeout=10) as client:
        await asyncio.gather(*(_fly_one(client, m, hz, batch_ms, result) for m in models))
    return result


async def _fly_one(
    client: httpx.AsyncClient, m: AircraftModel, hz: int, batch_ms: int, result: SimulationResult
) -> None:
    dt = 1 / hz
    start = time.monotonic()
    t = 0.0
    pending: list[dict[str, Any]] = []
    log.info(
        "takeoff", extra={"aircraft": m.aircraft_id, "flight": m.flight_id, "scenario": m.scenario}
    )
    while True:
        frac = t / m.duration_s
        frame = m.frame(t, dt, time.time_ns())
        if not m.in_dropout(frac):
            pending.append(frame)
        done = frame["phase"] == FlightPhase.SHUTDOWN
        if pending and (len(pending) * dt * 1000 >= batch_ms or done):
            await _send(client, pending, result)
            pending = []
        if done:
            break
        t += dt
        # Pace to real time; frames carry wall-clock timestamps.
        await asyncio.sleep(max(0.0, start + t - time.monotonic()))
    log.info("shutdown", extra={"aircraft": m.aircraft_id, "frames": m.seq})


async def _send(
    client: httpx.AsyncClient, frames: list[dict[str, Any]], result: SimulationResult
) -> None:
    for attempt in range(6):
        try:
            resp = await client.post("/v1/frames", json={"frames": frames})
        except httpx.TransportError:
            resp = None
        if resp is not None and resp.status_code == 202:
            body = resp.json()
            result.frames_sent += len(frames)
            result.frames_accepted += body["accepted"]
            result.frames_quarantined += body["quarantined"]
            return
        # A real DAU buffers on board and retries; honour server backpressure.
        result.retries += 1
        retry_after = float(resp.headers.get("retry-after", 0.5)) if resp is not None else 0.5
        await asyncio.sleep(retry_after * (attempt + 1))
    log.error("dropping batch after retries", extra={"frames": len(frames)})
