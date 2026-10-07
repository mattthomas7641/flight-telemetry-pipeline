from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq

from flightline.governance import Catalog, GovernedFrame
from flightline.streams import TelemetryBus
from flightline.writer.compaction import Compactor, default_target
from flightline.writer.sinks import LocalSink, S3Sink
from flightline.writer.worker import LakeWriter

TS = int(datetime(2026, 3, 1, 14, 30, tzinfo=UTC).timestamp() * 1e9)


async def land(
    bus: TelemetryBus,
    writer: LakeWriter,
    frames: list[GovernedFrame],
) -> None:
    await bus.publish(frames)
    await writer.poll_once()
    await writer.flush()


async def test_compaction_merges_small_files_and_removes_retry_duplicates(
    bus: TelemetryBus,
    catalog: Catalog,
    tmp_path: Path,
    make_frame: Callable[..., GovernedFrame],
) -> None:
    sink = LocalSink(tmp_path)
    w = LakeWriter(bus, sink, catalog, "w-0", flush_interval_s=3600)
    await w.start()
    frames = [make_frame({"motor.1.rpm": 4000.0 + i}, seq=i, ts_ns=TS + i) for i in range(50)]
    await land(bus, w, frames[:30])
    # A DAU whose 202 was lost re-sends frames 20..49: rows 20..29 are now duplicated.
    await land(bus, w, frames[20:])
    await land(bus, w, [make_frame({"battery.soc_pct": 50.0}, seq=99, ts_ns=TS)])

    before = await sink.list_keys("telemetry")
    assert len(before) == 3

    stats = await Compactor(sink).compact_hour("2026-03-01", "14")
    assert (stats.partitions, stats.input_files) == (2, 3)
    assert stats.duplicates_removed == 10

    after = await sink.list_keys("telemetry")
    assert len(after) == 2
    assert all(Path(k).name.startswith("compacted-") for k in after)
    proprietary = next(k for k in after if "classification=proprietary" in k)
    table = pq.read_table(tmp_path / proprietary)
    assert table.num_rows == 50
    assert table.column("seq").to_pylist() == list(range(50))  # sorted by ts within channel
    assert table.schema.metadata[b"flightline.compacted_from"] == b"2"
    tags = orjson.loads((tmp_path / f"{proprietary}.tags.json").read_bytes())
    assert tags == {"classification": "proprietary", "retention_class": "flight_test"}

    # Idempotent: a second run has nothing to do.
    again = await Compactor(sink).compact_hour("2026-03-01", "14")
    assert again.partitions == 0


async def test_late_data_is_merged_into_existing_compacted_file(
    bus: TelemetryBus,
    catalog: Catalog,
    tmp_path: Path,
    make_frame: Callable[..., GovernedFrame],
) -> None:
    sink = LocalSink(tmp_path)
    w = LakeWriter(bus, sink, catalog, "w-0", flush_interval_s=3600)
    await w.start()

    def mk(i: int) -> GovernedFrame:
        return make_frame({"motor.1.rpm": 1.0}, seq=i, ts_ns=TS + i)

    await land(bus, w, [mk(i) for i in range(5)])
    await Compactor(sink).compact_hour("2026-03-01", "14")
    await land(bus, w, [mk(i) for i in range(5, 8)])  # backfilled after compaction

    stats = await Compactor(sink).compact_hour("2026-03-01", "14")
    assert (stats.input_files, stats.output_rows) == (2, 8)
    assert len(await sink.list_keys("telemetry")) == 1


async def test_compaction_on_s3(s3: Any) -> None:
    sink = S3Sink("lake", client=s3)
    assert await sink.list_keys("telemetry") == []
    await sink.put("telemetry/a", b"1", {}, "x")
    assert await sink.read("telemetry/a") == b"1"
    await sink.delete_keys(["telemetry/a"])
    assert await sink.list_keys("telemetry") == []


def test_default_target_leaves_a_late_data_window() -> None:
    now = datetime(2026, 3, 1, 1, 15, tzinfo=UTC)
    assert default_target(now) == ("2026-02-28", "23")
