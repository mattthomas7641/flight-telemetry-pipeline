"""`flightline <component>`: one image, one entrypoint, role chosen by subcommand."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import statistics
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

import httpx
import orjson
from redis.asyncio import Redis

from flightline.settings import Settings, get_settings

if TYPE_CHECKING:
    from flightline.writer.sinks import Sink

log = logging.getLogger("flightline")


def _stop_event() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    return stop


def cmd_ingest(s: Settings, args: argparse.Namespace) -> None:
    import uvicorn

    from flightline.ingest.app import create_app

    uvicorn.run(create_app(s), host="0.0.0.0", port=args.port, log_config=None, access_log=False)


async def cmd_writer(s: Settings, _: argparse.Namespace) -> None:
    from flightline.governance import Catalog
    from flightline.health import Heartbeat, serve
    from flightline.streams import TelemetryBus
    from flightline.writer.worker import LakeWriter

    redis = Redis.from_url(s.redis_url)
    writer = LakeWriter(
        TelemetryBus(redis, s.shards),
        _sink(s),
        Catalog.load(s.catalog_path),
        s.consumer_name,
        prefix=s.s3_prefix,
        flush_max_rows=s.flush_max_rows,
        flush_interval_s=s.flush_interval_s,
        reclaim_idle_ms=s.reclaim_idle_ms,
    )
    hb = Heartbeat(max_age_s=max(30.0, 4 * s.flush_interval_s))
    server = await serve(hb, s.metrics_port)
    log.info("writer starting", extra={"consumer": s.consumer_name, "sink": s.sink})
    try:
        await writer.run(_stop_event(), hb)
    finally:
        server.close()
        await redis.aclose()
    log.info("writer stopped cleanly")


def _sink(s: Settings) -> Sink:
    from flightline.writer.sinks import LocalSink, S3Sink

    if s.sink == "s3":
        return S3Sink(s.s3_bucket, endpoint_url=s.s3_endpoint_url)
    return LocalSink(s.local_sink_path)


async def cmd_compact(s: Settings, args: argparse.Namespace) -> None:
    from flightline.writer.compaction import Compactor, default_target

    date, hour = (args.date, args.hour) if args.date else default_target()
    stats = await Compactor(_sink(s), s.s3_prefix).compact_hour(date, f"{int(hour):02d}")
    print(orjson.dumps({**stats.__dict__, "duplicates_removed": stats.duplicates_removed}).decode())


async def cmd_alerter(s: Settings, _: argparse.Namespace) -> None:
    from flightline.alerting.engine import AlertEngine
    from flightline.alerting.notifiers import (
        Dispatcher,
        LogNotifier,
        Notifier,
        PagerDutyNotifier,
        SlackNotifier,
    )
    from flightline.alerting.rules import RuleSet
    from flightline.alerting.worker import Alerter, ordinal_from_hostname, owned_shards
    from flightline.health import Heartbeat, serve
    from flightline.streams import TelemetryBus

    ordinal = (
        s.alerter_ordinal
        if s.alerter_ordinal is not None
        else ordinal_from_hostname(s.consumer_name)
    )
    shards = owned_shards(s.shards, s.alerter_replicas, ordinal)
    redis = Redis.from_url(s.redis_url)
    async with httpx.AsyncClient(timeout=s.notify_timeout_s) as client:
        notifiers: list[Notifier] = [LogNotifier()]
        if s.pagerduty_routing_key:
            notifiers.append(
                PagerDutyNotifier(
                    s.pagerduty_url,
                    s.pagerduty_routing_key.get_secret_value(),
                    client,
                    s.runbook_base_url,
                )
            )
        if s.slack_webhook_url:
            notifiers.append(
                SlackNotifier(s.slack_webhook_url.get_secret_value(), client, s.runbook_base_url)
            )
        alerter = Alerter(
            TelemetryBus(redis, s.shards),
            AlertEngine(RuleSet.load(s.rules_path)),
            Dispatcher(notifiers, max_attempts=s.notify_max_attempts),
            s.consumer_name,
            shards,
        )
        hb = Heartbeat()
        server = await serve(hb, s.metrics_port)
        log.info(
            "alerter starting",
            extra={"ordinal": ordinal, "shards": shards, "notifiers": [n.name for n in notifiers]},
        )
        try:
            await alerter.run(_stop_event(), hb)
        finally:
            server.close()
            await redis.aclose()


async def cmd_simulate(s: Settings, args: argparse.Namespace) -> None:
    from flightline.simulator import Scenario, fly

    result = await fly(
        args.url,
        aircraft=args.aircraft,
        duration_s=args.duration,
        hz=args.hz,
        scenario=Scenario(args.scenario),
        api_key=s.api_keys[0].get_secret_value() if s.api_keys else None,
        seed=args.seed,
    )
    print(orjson.dumps(result.__dict__).decode())


async def cmd_bench(s: Settings, args: argparse.Namespace) -> None:
    """Closed-loop load test: `concurrency` clients, each posting as fast as acked."""
    from flightline.simulator import synthetic_frames

    frames = list(synthetic_frames(args.batch * 64, aircraft=args.aircraft))
    bodies = [
        orjson.dumps({"frames": frames[i : i + args.batch]})
        for i in range(0, len(frames), args.batch)
    ]
    samples_per_frame = len(frames[0]["samples"])
    latencies: list[float] = []
    accepted = 0
    deadline = time.monotonic() + args.seconds
    headers = {"content-type": "application/json"}
    if s.api_keys:
        headers["authorization"] = f"Bearer {s.api_keys[0].get_secret_value()}"

    async def client_loop(client: httpx.AsyncClient, k: int) -> None:
        nonlocal accepted
        i = k
        while time.monotonic() < deadline:
            t0 = time.perf_counter()
            resp = await client.post("/v1/frames", content=bodies[i % len(bodies)])
            latencies.append(time.perf_counter() - t0)
            if resp.status_code == 202:
                accepted += resp.json()["accepted"]
            elif resp.status_code == 503:
                await asyncio.sleep(0.2)
            i += args.concurrency

    limits = httpx.Limits(max_connections=args.concurrency)
    start = time.monotonic()
    async with httpx.AsyncClient(
        base_url=args.url, headers=headers, timeout=30, limits=limits
    ) as client:
        await asyncio.gather(*(client_loop(client, k) for k in range(args.concurrency)))
    elapsed = time.monotonic() - start
    q = statistics.quantiles(latencies, n=100)
    report = {
        "seconds": round(elapsed, 1),
        "requests": len(latencies),
        "frames_per_s": round(accepted / elapsed),
        "samples_per_s": round(accepted * samples_per_frame / elapsed),
        "p50_ms": round(q[49] * 1000, 1),
        "p99_ms": round(q[98] * 1000, 1),
        "batch": args.batch,
        "concurrency": args.concurrency,
    }
    print(orjson.dumps(report, option=orjson.OPT_INDENT_2).decode())


def cmd_pager_mock(_: Settings, args: argparse.Namespace) -> None:
    import uvicorn

    from flightline.pager_mock import create_app

    uvicorn.run(create_app(), host="0.0.0.0", port=args.port, log_config=None)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="flightline", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    ing = sub.add_parser("ingest", help="run the ingest API")
    ing.add_argument("--port", type=int, default=8080)
    sub.add_parser("writer", help="run a lake writer")
    sub.add_parser("alerter", help="run a threshold alerter")
    comp = sub.add_parser("compact", help="compact and dedupe one hour of the lake")
    comp.add_argument("--date", help="YYYY-MM-DD (default: the hour that closed 2h ago)")
    comp.add_argument("--hour", default="0")

    sim = sub.add_parser("simulate", help="fly simulated aircraft against the ingest API")
    sim.add_argument("--url", default="http://localhost:8080")
    sim.add_argument("--aircraft", type=int, default=2)
    sim.add_argument("--duration", type=float, default=120.0, help="flight length, seconds")
    sim.add_argument("--hz", type=int, default=50)
    sim.add_argument(
        "--scenario",
        default="nominal",
        choices=["nominal", "motor-overtemp", "sensor-fault", "dropout", "rogue-channel"],
    )
    sim.add_argument("--seed", type=int, default=None)

    bench = sub.add_parser("bench", help="load-test the ingest API")
    bench.add_argument("--url", default="http://localhost:8080")
    bench.add_argument("--seconds", type=float, default=20)
    bench.add_argument("--concurrency", type=int, default=16)
    bench.add_argument("--batch", type=int, default=200, help="frames per request")
    bench.add_argument("--aircraft", type=int, default=8)

    pm = sub.add_parser("pager-mock", help="run a local PagerDuty Events v2 stand-in")
    pm.add_argument("--port", type=int, default=8090)
    return p


def main(argv: Sequence[str] | None = None) -> None:
    from flightline.observability import configure_logging

    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    handlers = {
        "ingest": cmd_ingest,
        "pager-mock": cmd_pager_mock,
        "writer": cmd_writer,
        "alerter": cmd_alerter,
        "simulate": cmd_simulate,
        "bench": cmd_bench,
        "compact": cmd_compact,
    }
    result = handlers[args.cmd](settings, args)
    if asyncio.iscoroutine(result):
        asyncio.run(result)


if __name__ == "__main__":
    main()
