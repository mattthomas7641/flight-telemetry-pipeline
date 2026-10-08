"""Lake writer: drains the telemetry and quarantine streams into partitioned Parquet.

Stateless and horizontally scalable: any replica can process any shard, and KEDA
scales replicas on consumer-group lag. Entries are acked only after every object in
a flush is durably stored, so a crash mid-flush means redelivery, never loss.

Layout:
    {prefix}/classification={c}/campaign={x}/aircraft={a}/date=YYYY-MM-DD/hour=HH/part-{h}.parquet
    {prefix}/_quarantine/date=YYYY-MM-DD/part-{h}.jsonl.gz

Classification is the top-level prefix because that is the boundary IAM policies are
written against: an analyst role can read `classification=internal/*` without any
possibility of touching export-controlled data.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import io
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, NamedTuple

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from flightline import __version__
from flightline.governance import Catalog, GovernedFrame
from flightline.health import Heartbeat
from flightline.observability import (
    INGEST_TO_DURABLE_SECONDS,
    STREAM_LAG,
    WRITER_FILES,
    WRITER_FLUSH_ERRORS,
    WRITER_FLUSH_SECONDS,
    WRITER_RECLAIMED,
    WRITER_ROWS,
)
from flightline.streams import (
    QUARANTINE_STREAM,
    WRITER_GROUP,
    RawEntry,
    TelemetryBus,
)
from flightline.writer.sinks import Sink

log = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("aircraft_id", pa.string(), nullable=False),
        pa.field("flight_id", pa.string(), nullable=False),
        pa.field("campaign", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("phase", pa.string()),
        pa.field("channel", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("quality", pa.string(), nullable=False),
        pa.field("classification", pa.string(), nullable=False),
        pa.field("retention_class", pa.string(), nullable=False),
        pa.field("owner", pa.string(), nullable=False),
        pa.field("ingest_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("catalog_version", pa.string(), nullable=False),
        pa.field("pipeline_version", pa.string(), nullable=False),
    ]
)
COLUMNS = SCHEMA.names
QUARANTINE_CLASSIFICATION = "export_controlled"  # unvetted data gets the strictest handling


class PartitionKey(NamedTuple):
    classification: str
    retention: str
    campaign: str
    aircraft_id: str
    date: str
    hour: str


@dataclass
class Buffer:
    entries: list[RawEntry] = field(default_factory=list)
    ids: set[tuple[str, str]] = field(default_factory=set)
    columns: dict[PartitionKey, dict[str, list[Any]]] = field(default_factory=dict)
    quarantine: list[bytes] = field(default_factory=list)
    rows: int = 0
    oldest_ingest_ns: int | None = None
    opened_at: float = field(default_factory=time.monotonic)

    def add_frames(self, frames: list[GovernedFrame]) -> None:
        for f in frames:
            dt = datetime.fromtimestamp(f.ts_ns / 1e9, tz=UTC)
            date, hour = dt.strftime("%Y-%m-%d"), dt.strftime("%H")
            lin = f.lineage
            if self.oldest_ingest_ns is None or lin.ingested_at_ns < self.oldest_ingest_ns:
                self.oldest_ingest_ns = lin.ingested_at_ns
            for s in f.samples:
                key = PartitionKey(
                    s.classification, s.retention, f.campaign, f.aircraft_id, date, hour
                )
                cols = self.columns.get(key)
                if cols is None:
                    cols = self.columns[key] = {c: [] for c in COLUMNS}
                cols["ts"].append(f.ts_ns)
                cols["aircraft_id"].append(f.aircraft_id)
                cols["flight_id"].append(f.flight_id)
                cols["campaign"].append(f.campaign)
                cols["source_id"].append(f.source_id)
                cols["seq"].append(f.seq)
                cols["phase"].append(f.phase)
                cols["channel"].append(s.channel)
                cols["value"].append(s.value)
                cols["unit"].append(s.unit)
                cols["quality"].append(s.quality)
                cols["classification"].append(s.classification)
                cols["retention_class"].append(s.retention)
                cols["owner"].append(s.owner)
                cols["ingest_id"].append(lin.ingest_id)
                cols["ingested_at"].append(lin.ingested_at_ns)
                cols["catalog_version"].append(lin.catalog_version)
                cols["pipeline_version"].append(lin.pipeline_version)
                self.rows += 1

    def batch_id(self) -> str:
        """Deterministic in the entry set: retrying a failed flush overwrites the same
        objects instead of creating duplicates."""
        h = hashlib.sha256()
        for e in sorted(self.entries, key=lambda e: (e.stream, e.id)):
            h.update(f"{e.stream}/{e.id};".encode())
        return h.hexdigest()[:16]


class LakeWriter:
    def __init__(
        self,
        bus: TelemetryBus,
        sink: Sink,
        catalog: Catalog,
        consumer: str,
        *,
        prefix: str = "telemetry",
        flush_max_rows: int = 250_000,
        flush_interval_s: float = 5.0,
        reclaim_idle_ms: int = 60_000,
        read_count: int = 128,
    ) -> None:
        self.bus = bus
        self.sink = sink
        self.catalog = catalog
        self.consumer = consumer
        self.prefix = prefix.strip("/")
        self.flush_max_rows = flush_max_rows
        self.flush_interval_s = flush_interval_s
        self.reclaim_idle_ms = reclaim_idle_ms
        self.read_count = read_count
        self.streams = [*bus.streams_for(), QUARANTINE_STREAM]
        self.buffer = Buffer()
        self._last_reclaim = 0.0

    async def start(self) -> None:
        await self.bus.ensure_group(WRITER_GROUP, self.streams)
        # Resume anything this consumer name was holding when it last stopped.
        pending = await self.bus.read_raw(WRITER_GROUP, self.consumer, self.streams, pending=True)
        await self._absorb(pending)
        if pending:
            log.info("resumed pending entries", extra={"count": len(pending)})

    async def run(self, stop: asyncio.Event, heartbeat: Heartbeat | None = None) -> None:
        await self.start()
        backoff = 0.5
        while not stop.is_set():
            try:
                await self.poll_once()
                backoff = 0.5
                if heartbeat:
                    heartbeat.beat()
            except Exception:
                WRITER_FLUSH_ERRORS.inc()
                log.exception("writer iteration failed; retrying", extra={"backoff_s": backoff})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            # Yield even when the read returned without suspending, so a hot stream
            # cannot starve the health server or the stop signal.
            await asyncio.sleep(0)
        await self.flush()

    async def poll_once(self) -> None:
        now = time.monotonic()
        if now - self._last_reclaim > self.reclaim_idle_ms / 1000:
            self._last_reclaim = now
            claimed = await self.bus.reclaim(
                WRITER_GROUP, self.consumer, self.streams, self.reclaim_idle_ms
            )
            if claimed:
                WRITER_RECLAIMED.inc(len(claimed))
                log.warning("reclaimed entries from idle consumer", extra={"n": len(claimed)})
                await self._absorb(claimed)
            STREAM_LAG.labels(WRITER_GROUP).set(await self.bus.lag(WRITER_GROUP))

        # Stop reading while full: the backlog then shows up as stream lag, which is
        # exactly the signal KEDA scales on.
        if self.buffer.rows < self.flush_max_rows:
            remaining = self.flush_interval_s - (time.monotonic() - self.buffer.opened_at)
            block_ms = max(1, min(1000, int(remaining * 1000)))
            await self._absorb(
                await self.bus.read_raw(
                    WRITER_GROUP,
                    self.consumer,
                    self.streams,
                    count=self.read_count,
                    block_ms=block_ms,
                )
            )

        age = time.monotonic() - self.buffer.opened_at
        if self.buffer.rows >= self.flush_max_rows or age >= self.flush_interval_s:
            await self.flush()

    async def _absorb(self, raw: list[RawEntry]) -> None:
        entries = [e for e in raw if (e.stream, e.id) not in self.buffer.ids]
        telemetry = [e for e in entries if e.stream != QUARANTINE_STREAM]
        decoded = {
            (d.stream, d.id): d.frames
            for d in await self.bus.decode_or_dead_letter(WRITER_GROUP, telemetry)
        }
        for e in entries:
            # XAUTOCLAIM can hand back entries this consumer already buffers (if flushes
            # have been failing for longer than reclaim_idle_ms); never double-count.
            key = (e.stream, e.id)
            if e.stream == QUARANTINE_STREAM:
                self.buffer.quarantine.append(e.payload.get(b"d") or e.payload.get("d") or b"")
            elif key in decoded:
                self.buffer.add_frames(decoded[key])
            else:
                continue  # dead-lettered (and acked) above
            self.buffer.ids.add(key)
            self.buffer.entries.append(e)

    async def flush(self) -> None:
        buf = self.buffer
        if not buf.entries:
            buf.opened_at = time.monotonic()
            return
        start = time.perf_counter()
        batch = buf.batch_id()

        writes = [self._write_partition(key, cols, batch) for key, cols in buf.columns.items()]
        if buf.quarantine:
            writes.append(self._write_quarantine(buf.quarantine, batch))
        # All-or-nothing from the stream's point of view: if any write raises, nothing
        # is acked and the whole batch is retried under the same deterministic keys.
        await asyncio.gather(*writes)
        await self.bus.ack(WRITER_GROUP, buf.entries)

        if buf.oldest_ingest_ns:
            INGEST_TO_DURABLE_SECONDS.observe((time.time_ns() - buf.oldest_ingest_ns) / 1e9)
        WRITER_FLUSH_SECONDS.observe(time.perf_counter() - start)
        log.info(
            "flushed",
            extra={
                "batch": batch,
                "entries": len(buf.entries),
                "rows": buf.rows,
                "partitions": len(buf.columns),
                "quarantined": len(buf.quarantine),
                "ms": round((time.perf_counter() - start) * 1000, 1),
            },
        )
        self.buffer = Buffer()

    async def _write_partition(
        self, key: PartitionKey, cols: dict[str, list[Any]], batch: str
    ) -> None:
        table = pa.Table.from_pydict(cols, schema=SCHEMA).sort_by(
            [("channel", "ascending"), ("ts", "ascending")]
        )
        retention_days = self.catalog.retention_days(key.retention)
        meta = {
            "flightline.pipeline_version": __version__,
            "flightline.catalog_versions": ",".join(sorted(set(cols["catalog_version"]))),
            "flightline.classification": key.classification,
            "flightline.retention_class": key.retention,
            "flightline.retention_days": str(retention_days),
            "flightline.batch": batch,
            "flightline.writer": self.consumer,
        }
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), **meta})
        out = io.BytesIO()
        pq.write_table(
            table,
            out,
            compression="zstd",
            use_dictionary=[
                "aircraft_id",
                "flight_id",
                "campaign",
                "source_id",
                "phase",
                "channel",
                "unit",
                "quality",
                "classification",
                "retention_class",
                "owner",
                "ingest_id",
                "catalog_version",
                "pipeline_version",
            ],
            write_statistics=True,
        )
        path = (
            f"{self.prefix}/classification={key.classification}/campaign={key.campaign}"
            f"/aircraft={key.aircraft_id}/date={key.date}/hour={key.hour}/part-{batch}.parquet"
        )
        await self.sink.put(
            path,
            out.getvalue(),
            tags={"classification": key.classification, "retention_class": key.retention},
            content_type="application/vnd.apache.parquet",
        )
        WRITER_ROWS.labels(key.classification).inc(table.num_rows)
        WRITER_FILES.inc()

    async def _write_quarantine(self, records: list[bytes], batch: str) -> None:
        date = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        body = gzip.compress(b"\n".join(r for r in records if r) + b"\n")
        await self.sink.put(
            f"{self.prefix}/_quarantine/date={date}/part-{batch}.jsonl.gz",
            body,
            tags={"classification": QUARANTINE_CLASSIFICATION, "retention_class": "engineering"},
            content_type="application/gzip",
        )
        WRITER_FILES.inc()


def read_quarantine(body: bytes) -> list[dict[str, Any]]:
    return [orjson.loads(line) for line in gzip.decompress(body).splitlines() if line]
