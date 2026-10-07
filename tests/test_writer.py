from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest

from flightline.governance import Catalog, GovernedFrame, Quarantined, QuarantineReason
from flightline.streams import WRITER_GROUP, TelemetryBus
from flightline.writer.sinks import LocalSink
from flightline.writer.worker import LakeWriter, read_quarantine


class FlakySink(LocalSink):
    def __init__(self, root: Path, failures: int) -> None:
        super().__init__(root)
        self.failures = failures
        self.keys: list[str] = []

    async def put(self, key: str, body: bytes, tags: dict[str, str], content_type: str) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise OSError("S3 is having a bad day")
        self.keys.append(key)
        await super().put(key, body, tags, content_type)


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def writer(bus: TelemetryBus, sink: LocalSink, catalog: Catalog, **kw: object) -> LakeWriter:
    return LakeWriter(bus, sink, catalog, "w-0", flush_interval_s=3600, **kw)  # type: ignore[arg-type]


async def test_flush_writes_partitioned_parquet_by_classification(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    w = writer(bus, LocalSink(lake), catalog)
    await w.start()
    await bus.publish(
        [
            make_frame(
                {"motor.1.rpm": 4000.0, "nav.airspeed_kt": 120.0, "battery.soc_pct": 80.0},
                seq=i,
            )
            for i in range(10)
        ]
    )
    await w.poll_once()
    await w.flush()

    files = sorted(p.relative_to(lake).as_posix() for p in lake.rglob("*.parquet"))
    classes = sorted(f.split("/")[1] for f in files)
    assert classes == [
        "classification=export_controlled",
        "classification=internal",
        "classification=proprietary",
    ]
    assert all("campaign=FT-TEST/aircraft=N301FL/date=" in f for f in files)

    table = pq.read_table(next(lake.rglob("classification=proprietary/**/*.parquet")))
    assert table.num_rows == 10
    assert set(table.column("classification").to_pylist()) == {"proprietary"}
    assert table.column("catalog_version")[0].as_py() == catalog.version
    meta = table.schema.metadata
    assert meta[b"flightline.retention_days"] == b"2555"
    tags = orjson.loads(next(lake.rglob("classification=proprietary/**/*.tags.json")).read_bytes())
    assert tags == {"classification": "proprietary", "retention_class": "flight_test"}

    assert await bus.lag(WRITER_GROUP) == 0  # acked only after the write


async def test_failed_flush_acks_nothing_and_retry_is_idempotent(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    sink = FlakySink(lake, failures=1)
    w = writer(bus, sink, catalog)
    await w.start()
    await bus.publish([make_frame(seq=i) for i in range(5)])
    await w.poll_once()

    with pytest.raises(OSError, match="bad day"):
        await w.flush()
    assert await bus.lag(WRITER_GROUP) == 1  # nothing lost
    first_keys = set(sink.keys)

    await w.flush()  # retry the same buffer
    assert await bus.lag(WRITER_GROUP) == 0
    # The deterministic batch id means a retry targets the same object keys.
    assert first_keys <= set(sink.keys)
    assert len({k.rsplit("/", 1)[1] for k in sink.keys}) == 1


async def test_crashed_writer_resumes_its_pending_entries(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    w1 = writer(bus, LocalSink(lake), catalog)
    await w1.start()
    await bus.publish([make_frame()])
    await w1.poll_once()  # read but never flushed: simulated crash

    w2 = writer(bus, LocalSink(lake), catalog)  # same consumer name, as after a restart
    await w2.start()
    assert w2.buffer.rows == 2
    await w2.flush()
    assert await bus.lag(WRITER_GROUP) == 0


async def test_other_replica_reclaims_dead_consumers_entries(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    dead = writer(bus, LocalSink(lake), catalog)
    await dead.start()
    await bus.publish([make_frame()])
    await dead.poll_once()

    survivor = LakeWriter(
        bus, LocalSink(lake), catalog, "w-1", flush_interval_s=3600, reclaim_idle_ms=0
    )
    await survivor.start()
    await survivor.poll_once()
    assert survivor.buffer.rows == 2
    await survivor.poll_once()  # reclaiming again must not double-buffer
    assert survivor.buffer.rows == 2


async def test_quarantine_lands_under_restricted_prefix(
    bus: TelemetryBus, catalog: Catalog, lake: Path
) -> None:
    w = writer(bus, LocalSink(lake), catalog)
    await w.start()
    q = Quarantined(QuarantineReason.UNREGISTERED_CHANNEL, "x.y", {"samples": {"x.y": 1}}, "i", 1)
    await bus.publish([], [q])
    await w.poll_once()
    await w.flush()

    [obj] = list(lake.rglob("*.jsonl.gz"))
    assert "_quarantine/date=" in obj.as_posix()
    assert read_quarantine(obj.read_bytes())[0]["reason"] == "unregistered_channel"
    tags = orjson.loads(Path(f"{obj}.tags.json").read_bytes())
    assert tags["classification"] == "export_controlled"


async def test_flushes_on_row_threshold(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    w = writer(bus, LocalSink(lake), catalog, flush_max_rows=5)
    await w.start()
    await bus.publish([make_frame(seq=i) for i in range(5)])  # 10 rows
    await w.poll_once()
    assert w.buffer.rows == 0
    assert list(lake.rglob("*.parquet"))


async def test_run_flushes_on_shutdown(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    w = writer(bus, LocalSink(lake), catalog)
    await bus.ensure_group(WRITER_GROUP, w.streams)
    await bus.publish([make_frame()])
    stop = asyncio.Event()
    task = asyncio.create_task(w.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert list(lake.rglob("*.parquet"))
    assert await bus.lag(WRITER_GROUP) == 0


async def test_empty_flush_is_noop(bus: TelemetryBus, catalog: Catalog, lake: Path) -> None:
    w = writer(bus, LocalSink(lake), catalog)
    await w.flush()
    assert not lake.exists()


async def test_writer_skips_poison_and_keeps_writing(
    bus: TelemetryBus, catalog: Catalog, lake: Path, make_frame: Callable[..., GovernedFrame]
) -> None:
    from flightline.streams import DEADLETTER_STREAM, telemetry_stream

    w = writer(bus, LocalSink(lake), catalog)
    await w.start()
    await bus.redis.xadd(telemetry_stream(0), {"d": b"not zlib"})
    await bus.publish([make_frame()])
    await w.poll_once()
    await w.flush()
    assert list(lake.rglob("*.parquet"))
    assert await bus.redis.xlen(DEADLETTER_STREAM) == 1
    assert await bus.lag(WRITER_GROUP) == 0
