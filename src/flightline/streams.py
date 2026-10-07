"""The telemetry bus: sharded Redis Streams with consumer groups.

Why shards: per-aircraft ordering matters to the alerter (a "sustained for 400 ms"
rule is meaningless if samples arrive out of order), and a single stream would cap
throughput at one Redis key. Routing by hash(aircraft_id) gives Kafka-partition-like
semantics: total order per aircraft, parallelism across aircraft.

Delivery is at-least-once. Consumers ack only after their side effect (a durable
write, a dispatched page) has happened; a consumer that dies leaves its entries in
the pending list, where another replica reclaims them after `reclaim_idle_ms`.
"""

from __future__ import annotations

import asyncio
import logging
import time
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import orjson
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from flightline.governance import GovernedFrame, Quarantined
from flightline.observability import DEAD_LETTERED

TELEMETRY_PREFIX = "fl:telemetry:"
QUARANTINE_STREAM = "fl:quarantine"
# Entries no consumer can decode. Parked (not retried forever) so one bad entry cannot
# wedge a consumer group, which for the alerter would mean silently stopping paging.
DEADLETTER_STREAM = "fl:deadletter"
ALERTS_STREAM = "fl:alerts"

WRITER_GROUP = "writer"
ALERTER_GROUP = "alerter"

ENTRY_CODEC_VERSION = 2
# Bounded so a single stream entry stays well under a megabyte.
MAX_FRAMES_PER_ENTRY = 250
# Audit streams are capped; the lake is the system of record for both.
AUDIT_MAXLEN = 100_000

log = logging.getLogger(__name__)


def shard_for(aircraft_id: str, shards: int) -> int:
    # crc32, not hash(): Python's str hash is salted per process, and every producer
    # and consumer must agree on the mapping.
    return zlib.crc32(aircraft_id.encode()) % shards


def telemetry_stream(shard: int) -> str:
    return f"{TELEMETRY_PREFIX}{shard}"


@dataclass(slots=True)
class Entry:
    stream: str
    id: str
    frames: list[GovernedFrame]


@dataclass(slots=True)
class RawEntry:
    stream: str
    id: str
    payload: dict[Any, Any]


class TelemetryBus:
    def __init__(self, redis: Redis, shards: int) -> None:
        self.redis = redis
        self.shards = shards

    # ---- producer side -------------------------------------------------------------

    async def publish(
        self, frames: Sequence[GovernedFrame], quarantined: Sequence[Quarantined] = ()
    ) -> None:
        by_shard: dict[int, list[GovernedFrame]] = defaultdict(list)
        for f in frames:
            by_shard[shard_for(f.aircraft_id, self.shards)].append(f)

        async with self.redis.pipeline(transaction=False) as pipe:
            for shard, shard_frames in by_shard.items():
                key = telemetry_stream(shard)
                for i in range(0, len(shard_frames), MAX_FRAMES_PER_ENTRY):
                    chunk = shard_frames[i : i + MAX_FRAMES_PER_ENTRY]
                    pipe.xadd(key, {"d": encode_entry(chunk)})
            for q in quarantined:
                pipe.xadd(
                    QUARANTINE_STREAM,
                    {"d": orjson.dumps(q.to_wire())},
                    maxlen=AUDIT_MAXLEN,
                    approximate=True,
                )
            await pipe.execute()

    # ---- consumer side -------------------------------------------------------------

    def streams_for(self, shards: Iterable[int] | None = None) -> list[str]:
        return [telemetry_stream(s) for s in (shards if shards is not None else range(self.shards))]

    async def wait_ready(self, timeout_s: float = 120.0) -> None:
        """Block until Redis answers, with capped exponential backoff. Pods start in
        any order; crash-looping until a dependency is up is noisy and slow to recover."""
        deadline = time.monotonic() + timeout_s
        delay = 0.25
        while True:
            try:
                await self.redis.ping()
                return
            except (ConnectionError, OSError, RedisConnectionError) as e:
                if time.monotonic() + delay > deadline:
                    raise
                log.warning("waiting for redis", extra={"error": str(e), "retry_s": delay})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    async def ensure_group(self, group: str, streams: Iterable[str]) -> None:
        await self.wait_ready()
        for key in streams:
            try:
                await self.redis.xgroup_create(key, group, id="0", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    async def read(
        self,
        group: str,
        consumer: str,
        streams: Sequence[str],
        *,
        count: int = 64,
        block_ms: int = 1000,
        pending: bool = False,
        keep: Callable[[str], bool] | None = None,
    ) -> list[Entry]:
        """Read new entries, or (pending=True) entries already delivered to this
        consumer but not acked, which is how a restarted consumer resumes."""
        raw = await self.read_raw(
            group, consumer, streams, count=count, block_ms=block_ms, pending=pending
        )
        return await self.decode_or_dead_letter(group, raw, keep)

    async def decode_or_dead_letter(
        self,
        group: str,
        raw: Sequence[RawEntry],
        keep: Callable[[str], bool] | None = None,
    ) -> list[Entry]:
        good: list[Entry] = []
        poison: list[tuple[RawEntry, str]] = []
        for r in raw:
            try:
                good.append(Entry(r.stream, r.id, decode_frames(r.payload, keep)))
            except Exception as e:
                poison.append((r, f"{type(e).__name__}: {e}"))
        if poison:
            await self.dead_letter(group, poison)
        return good

    async def dead_letter(self, group: str, poison: Sequence[tuple[RawEntry, str]]) -> None:
        async with self.redis.pipeline(transaction=False) as pipe:
            for r, error in poison:
                pipe.xadd(
                    DEADLETTER_STREAM,
                    {
                        "stream": r.stream,
                        "id": r.id,
                        "group": group,
                        "error": error[:500],
                        "d": r.payload.get(b"d", r.payload.get("d", b"")),
                    },
                    maxlen=AUDIT_MAXLEN,
                    approximate=True,
                )
            await pipe.execute()
        await self.ack(group, [r for r, _ in poison])
        DEAD_LETTERED.labels(group).inc(len(poison))
        log.error("dead-lettered undecodable entries", extra={"group": group, "n": len(poison)})

    async def read_raw(
        self,
        group: str,
        consumer: str,
        streams: Sequence[str],
        *,
        count: int = 64,
        block_ms: int = 1000,
        pending: bool = False,
    ) -> list[RawEntry]:
        start = "0" if pending else ">"
        resp: Any = await self.redis.xreadgroup(
            group,
            consumer,
            dict.fromkeys(streams, start),
            count=count,
            block=None if pending else block_ms,
        )
        out: list[RawEntry] = []
        for stream, items in resp or []:
            skey = _s(stream)
            for entry_id, fields in items:
                if fields:  # acked-then-trimmed entries come back with no fields
                    out.append(RawEntry(skey, _s(entry_id), dict(fields)))
        return out

    async def ack(self, group: str, entries: Iterable[Entry | RawEntry]) -> int:
        by_stream: dict[str, list[str]] = defaultdict(list)
        for e in entries:
            by_stream[e.stream].append(e.id)
        if not by_stream:
            return 0
        async with self.redis.pipeline(transaction=False) as pipe:
            for stream, ids in by_stream.items():
                pipe.xack(stream, group, *ids)
            results = await pipe.execute()
        return sum(int(r) for r in results)

    async def reclaim(
        self, group: str, consumer: str, streams: Sequence[str], min_idle_ms: int, count: int = 64
    ) -> list[RawEntry]:
        """Take over entries a dead consumer left pending."""
        out: list[RawEntry] = []
        for stream in streams:
            resp = await self.redis.xautoclaim(
                stream, group, consumer, min_idle_time=min_idle_ms, start_id="0-0", count=count
            )
            for entry_id, fields in resp[1]:
                if fields:
                    out.append(RawEntry(stream, _s(entry_id), dict(fields)))
        return out

    async def lag(self, group: str, streams: Sequence[str] | None = None) -> int:
        """Entries not yet delivered + delivered but unacked, summed across shards."""
        total = 0
        for stream in streams or self.streams_for():
            try:
                groups = await self.redis.xinfo_groups(stream)
            except ResponseError:
                continue  # stream does not exist yet
            for g in groups:
                if _s(g["name"]) == group:
                    lag = g.get("lag")
                    if lag is None:  # Redis cannot compute lag after some trims
                        lag = await self.redis.xlen(stream)
                    total += int(lag) + int(g.get("pending", 0))
        return total


class CachedLag:
    """Ingest checks backlog on every request; one XINFO per request would be wasteful."""

    def __init__(self, bus: TelemetryBus, group: str, ttl_s: float = 1.0) -> None:
        self.bus, self.group, self.ttl_s = bus, group, ttl_s
        self._value = 0
        self._at = 0.0

    async def get(self) -> int:
        now = time.monotonic()
        if now - self._at > self.ttl_s:
            self._value = await self.bus.lag(self.group)
            self._at = now
        return self._value


def encode_entry(frames: Sequence[GovernedFrame]) -> bytes:
    """Entry codec v2: policy tags dictionary-encoded per entry, then zlib level 1.
    ~15x smaller than naive per-sample JSON; the bus is bandwidth-bound, not CPU-bound."""
    policies: dict[str, list[str]] = {}
    for f in frames:
        policies.update(f.policies())
    body = orjson.dumps(
        {"v": ENTRY_CODEC_VERSION, "pol": policies, "f": [f.to_wire() for f in frames]}
    )
    return zlib.compress(body, 1)


def decode_entry(data: bytes, keep: Callable[[str], bool] | None = None) -> list[GovernedFrame]:
    obj = orjson.loads(zlib.decompress(data))
    if obj.get("v") != ENTRY_CODEC_VERSION:
        raise ValueError(f"unsupported entry codec version {obj.get('v')!r}")
    pol = obj["pol"]
    return [GovernedFrame.from_wire(w, pol, keep) for w in obj["f"]]


def decode_frames(
    payload: dict[Any, Any], keep: Callable[[str], bool] | None = None
) -> list[GovernedFrame]:
    data = payload.get(b"d", payload.get("d"))
    if data is None:
        return []
    return decode_entry(data, keep)


def _s(v: bytes | str) -> str:
    return v.decode() if isinstance(v, bytes) else v
