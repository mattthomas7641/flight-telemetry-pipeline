from __future__ import annotations

from collections.abc import Callable

import pytest

from flightline.governance import GovernedFrame
from flightline.streams import (
    MAX_FRAMES_PER_ENTRY,
    QUARANTINE_STREAM,
    WRITER_GROUP,
    CachedLag,
    TelemetryBus,
    shard_for,
    telemetry_stream,
)


def test_shard_routing_is_stable_and_spreads_load() -> None:
    assert shard_for("N301FL", 8) == shard_for("N301FL", 8)  # not process-salted
    shards = {shard_for(f"N{300 + i}FL", 8) for i in range(64)}
    assert len(shards) == 8


async def test_publish_preserves_per_aircraft_order_within_shard(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    frames = [make_frame(seq=i) for i in range(1, 6)]
    await bus.publish(frames)
    await bus.ensure_group(WRITER_GROUP, bus.streams_for())
    entries = await bus.read(WRITER_GROUP, "c1", bus.streams_for(), block_ms=1)
    seqs = [f.seq for e in entries for f in e.frames]
    assert seqs == [1, 2, 3, 4, 5]
    assert {e.stream for e in entries} == {telemetry_stream(shard_for("N301FL", 4))}


async def test_large_publishes_are_chunked(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    await bus.publish([make_frame(seq=i) for i in range(MAX_FRAMES_PER_ENTRY + 10)])
    await bus.ensure_group(WRITER_GROUP, bus.streams_for())
    entries = await bus.read(WRITER_GROUP, "c1", bus.streams_for(), block_ms=1)
    assert [len(e.frames) for e in entries] == [MAX_FRAMES_PER_ENTRY, 10]


async def test_unacked_entries_survive_restart_and_can_be_reclaimed(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    streams = bus.streams_for()
    await bus.ensure_group(WRITER_GROUP, streams)
    await bus.publish([make_frame()])
    [entry] = await bus.read(WRITER_GROUP, "dead-pod", streams, block_ms=1)

    # Same consumer name after restart sees its pending entry.
    [again] = await bus.read(WRITER_GROUP, "dead-pod", streams, pending=True)
    assert again.id == entry.id

    # A different consumer takes it over once idle.
    claimed = await bus.reclaim(WRITER_GROUP, "survivor", streams, min_idle_ms=0)
    assert [c.id for c in claimed] == [entry.id]
    assert await bus.lag(WRITER_GROUP) == 1
    assert await bus.ack(WRITER_GROUP, claimed) == 1
    assert await bus.lag(WRITER_GROUP) == 0


async def test_lag_counts_undelivered_and_pending(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    streams = bus.streams_for()
    await bus.ensure_group(WRITER_GROUP, streams)
    await bus.ensure_group(WRITER_GROUP, streams)  # idempotent
    aircraft = [f"N{300 + i}FL" for i in range(10)]
    await bus.publish([make_frame(aircraft_id=a) for a in aircraft])
    # Lag is in stream entries; one publish yields one entry per shard touched.
    expected = len({shard_for(a, 4) for a in aircraft})
    assert await bus.lag(WRITER_GROUP) == expected
    entries = await bus.read(WRITER_GROUP, "c1", streams, block_ms=1)
    assert await bus.lag(WRITER_GROUP) == expected  # delivered but unacked still counts
    await bus.ack(WRITER_GROUP, entries)
    assert await bus.lag(WRITER_GROUP) == 0
    assert await bus.lag("no-such-group") == 0


async def test_cached_lag_avoids_hitting_redis_every_call(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    await bus.ensure_group(WRITER_GROUP, bus.streams_for())
    cached = CachedLag(bus, WRITER_GROUP, ttl_s=60)
    assert await cached.get() == 0
    await bus.publish([make_frame()])
    assert await cached.get() == 0  # still cached
    cached.ttl_s = 0
    assert await cached.get() == 1


async def test_ack_of_nothing_is_a_noop(bus: TelemetryBus) -> None:
    assert await bus.ack(WRITER_GROUP, []) == 0


async def test_quarantine_goes_to_its_own_stream(bus: TelemetryBus) -> None:
    from flightline.governance import Quarantined, QuarantineReason

    q = Quarantined(QuarantineReason.SCHEMA_INVALID, "bad", {"x": 1}, "i", 1)
    await bus.publish([], [q])
    assert await bus.redis.xlen(QUARANTINE_STREAM) == 1


async def test_wait_ready_retries_until_redis_answers(bus: TelemetryBus) -> None:
    from redis.exceptions import ConnectionError as RedisConnectionError

    real_ping = bus.redis.ping
    calls = 0

    async def flaky_ping() -> bool:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RedisConnectionError("not yet")
        return bool(await real_ping())

    bus.redis.ping = flaky_ping  # type: ignore[method-assign]
    await bus.wait_ready(timeout_s=5)
    assert calls == 3


async def test_wait_ready_gives_up_after_timeout(bus: TelemetryBus) -> None:
    from redis.exceptions import ConnectionError as RedisConnectionError

    async def down() -> bool:
        raise RedisConnectionError("down")

    bus.redis.ping = down  # type: ignore[method-assign]
    with pytest.raises(RedisConnectionError):
        await bus.wait_ready(timeout_s=0.1)


async def test_poison_entry_is_dead_lettered_not_retried_forever(
    bus: TelemetryBus, make_frame: Callable[..., GovernedFrame]
) -> None:
    """Regression: an entry in a stale codec once wedged the alerter in a crash loop."""
    from flightline.streams import DEADLETTER_STREAM

    streams = bus.streams_for()
    await bus.ensure_group(WRITER_GROUP, streams)
    poison_stream = telemetry_stream(shard_for("N301FL", 4))
    await bus.redis.xadd(poison_stream, {"d": b'[{"legacy": "uncompressed json"}]'})
    await bus.publish([make_frame()])

    entries = await bus.read(WRITER_GROUP, "c1", streams, block_ms=1)
    assert [len(e.frames) for e in entries] == [1]  # good entry still delivered
    [(_, dead)] = await bus.redis.xrange(DEADLETTER_STREAM)
    assert dead[b"group"] == b"writer"
    assert b"error" in dead and dead[b"d"].startswith(b"[")
    await bus.ack(WRITER_GROUP, entries)
    assert await bus.lag(WRITER_GROUP) == 0  # poison acked, nothing stuck
