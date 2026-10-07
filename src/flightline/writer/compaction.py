"""Hourly compaction: many small flush files -> one deduplicated file per partition.

Two problems, one job:

1. Small files. Writers flush every few seconds per partition, which is right for
   ingest latency and wrong for query engines (Athena/Spark pay per object opened).
2. Duplicates. Delivery is at-least-once, so a writer that crashed after writing but
   before acking leaves rows that the redelivered batch writes again. Rows carry their
   natural key (source_id, seq, channel); compaction keeps the first occurrence.

Crash safety without coordination: the compacted object is written *before* inputs are
deleted, under a key derived from the input set. A crash in between leaves duplicates,
which the next run removes, because compacted files are themselves valid inputs. Writers
can keep landing late data in the same hour; anything not in the listing is untouched.
"""

from __future__ import annotations

import hashlib
import io
import logging
import posixpath
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from flightline.writer.sinks import Sink

log = logging.getLogger(__name__)

NATURAL_KEY = ["source_id", "seq", "channel"]


@dataclass
class CompactionStats:
    partitions: int = 0
    input_files: int = 0
    input_rows: int = 0
    output_rows: int = 0

    @property
    def duplicates_removed(self) -> int:
        return self.input_rows - self.output_rows


def default_target(now: datetime | None = None, lag_hours: int = 2) -> tuple[str, str]:
    """Compact the hour that closed `lag_hours` ago, leaving room for late frames."""
    t = (now or datetime.now(tz=UTC)) - timedelta(hours=lag_hours)
    return t.strftime("%Y-%m-%d"), t.strftime("%H")


def dedupe(table: pa.Table) -> pa.Table:
    indexed = table.append_column("__row", pa.array(range(table.num_rows), pa.int64()))
    first = indexed.group_by(NATURAL_KEY, use_threads=False).aggregate([("__row", "min")])
    keep = pc.sort_indices(first["__row_min"])
    return table.take(pc.take(first["__row_min"], keep))


class Compactor:
    def __init__(self, sink: Sink, prefix: str = "telemetry") -> None:
        self.sink = sink
        self.prefix = prefix.strip("/")

    async def compact_hour(self, date: str, hour: str) -> CompactionStats:
        marker = f"/date={date}/hour={hour}/"
        keys = [
            k
            for k in await self.sink.list_keys(f"{self.prefix}/classification=")
            if marker in k and k.endswith(".parquet")
        ]
        by_dir: dict[str, list[str]] = defaultdict(list)
        for k in keys:
            by_dir[posixpath.dirname(k)].append(k)

        stats = CompactionStats()
        for directory, inputs in sorted(by_dir.items()):
            if len(inputs) == 1 and posixpath.basename(inputs[0]).startswith("compacted-"):
                continue  # already compacted, nothing new arrived
            await self._compact_partition(directory, sorted(inputs), stats)
        log.info("compaction done", extra={"date": date, "hour": hour, **stats.__dict__})
        return stats

    async def _compact_partition(
        self, directory: str, inputs: list[str], stats: CompactionStats
    ) -> None:
        tables = [pq.read_table(io.BytesIO(await self.sink.read(k))) for k in inputs]
        merged = pa.concat_tables(tables, promote_options="default")
        out = dedupe(merged).sort_by([("channel", "ascending"), ("ts", "ascending")])

        meta = dict(tables[0].schema.metadata or {})
        meta[b"flightline.compacted_from"] = str(len(inputs)).encode()
        out = out.replace_schema_metadata(meta)

        digest = hashlib.sha256("\n".join(inputs).encode()).hexdigest()[:16]
        target = f"{directory}/compacted-{digest}.parquet"
        buf = io.BytesIO()
        pq.write_table(out, buf, compression="zstd", write_statistics=True)
        classification = meta.get(b"flightline.classification", b"").decode()
        retention = meta.get(b"flightline.retention_class", b"").decode()
        await self.sink.put(
            target,
            buf.getvalue(),
            tags={"classification": classification, "retention_class": retention},
            content_type="application/vnd.apache.parquet",
        )
        await self.sink.delete_keys([k for k in inputs if k != target])

        stats.partitions += 1
        stats.input_files += len(inputs)
        stats.input_rows += merged.num_rows
        stats.output_rows += out.num_rows
