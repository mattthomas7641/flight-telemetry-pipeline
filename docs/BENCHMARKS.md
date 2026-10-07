# Benchmarks and findings

All numbers come from one laptop: Docker Desktop with an 8-vCPU / 4 GB VM shared by the
whole stack (ingest, 2 writers, alerter, Redis, LocalStack, Prometheus, Grafana). They
are useful for *relative* comparisons and for showing where the ceilings are. They are
not production capacity numbers; on dedicated nodes every component scales horizontally
except where noted.

Reproduce with `make up && make bench`. The load generator (`flightline bench`) runs
**inside** the compose network: running it from the host through Docker Desktop's port
forwarding capped Redis-bound throughput near 1 MB/s and made the numbers meaningless.

Frames come from the simulator: 38 channels per frame, 8 aircraft interleaved, 200
frames per request.

## Ingest (single pod, one uvicorn worker = one core)

| Client concurrency | Frames/s | Samples/s | p50 | p99 |
|---|---|---|---|---|
| 2 | 11,065 | 420k | 29.7 ms | 121 ms |
| 16 | 8,829 | 335k | 334 ms | 783 ms |

At 16 the server is saturated and latency is queueing, which is what the HPA (scale-out
at 65% CPU) exists for. Scale is per pod; ingest is stateless.

## Alerter (single core, in-process: decode + evaluate)

| Version | Frames/s | Samples/s |
|---|---|---|
| Initial | 8,520 | 324k |
| + fast path for nominal readings, decode only watched channels | **46,077** | **1.75M** |

Found with `cProfile`. Most time went to materialising samples no rule watches and to
running the full state machine on readings nowhere near a threshold.

## Isolation: paging under storage saturation

Background load of 9.3k frames/s (355k samples/s) saturated the two writers, and an
overtemp scenario flew at the same time:

| | Value |
|---|---|
| Writer p99 ingest→durable | 30 s (histogram cap) |
| Writer peak lag | ~4,900 entries |
| **Alerter peak lag** | **36 entries** |
| **Mean ingest→page** | **166 ms** |
| p99 ingest→page | < 250 ms |

That is the payoff of separate consumer groups: storage falling 30 s behind cost the
paging path nothing.

## Stream entry size

| Encoding (50 frames × 38 channels) | Bytes |
|---|---|
| Raw input JSON | 59,921 |
| v1: per-sample policy arrays | 171,421 |
| v2: policies dictionary-encoded per entry | 66,951 |
| **v2 + zlib level 1** (shipped) | **11,407** (15× smaller than v1) |

## Writers: honest ceiling

Two writers sustained about 170–210k rows/s. Scaling to four did **not** raise that on
this machine (191k rows/s), and ingest throughput fell, because all the processes were
competing for the same 8 vCPUs. The writer's own ceiling is row assembly in Python
(~100k rows/s/core); DESIGN.md "What I would build next" item 5 covers the fix. KEDA
scaling is validated by schema and by design, not by a load test on dedicated nodes.

## Incident: poison-message crash loop

Found by the isolation benchmark, before the fix above.

**What happened.** After the entry codec changed from v1 to v2, the alerter's
pending-entry list still held v1 entries from an earlier run. On start the alerter
replays its pending entries; decoding a v1 entry raised, the process crashed, the
container restarted, and the cycle repeated. **The alerter silently stopped paging**
while every other component looked healthy.

**Why it matters.** It is the worst failure mode for this system: the thing that pages
cannot page about itself.

**Fix.**
1. Consumers decode entry by entry; anything undecodable goes to `fl:deadletter` with
   its raw payload and error, and is acked so it cannot block the group.
2. `flightline_dead_lettered_total` plus a critical `DeadLetteredEntries` alert.
3. Regression tests for the stream, the writer, and the alerter-restart-with-poison
   scenario specifically (`test_alerter_survives_poison_in_its_pending_list`).
4. Rule written into DESIGN.md: codec changes deploy consumers before producers.

On redeploy against the same Redis, 632 stale entries were dead-lettered and both
consumer groups caught up within seconds.

## Other bugs found by actually running it

- **Startup crash loop on k3s.** Pods started before Redis and crashed, recovering only
  after Kubernetes backoff. Added `wait_ready()` with capped exponential backoff.
- **Reserved PriorityClass name.** `system-*` names are reserved by Kubernetes. Schema
  validation (kubeconform) passed; only a real apply caught it.
- **LocalStack 4.0 pagination.** Bucket-wide `ListObjectsV2` past 1,000 keys returns
  empty continuation pages. Production uses real S3; the e2e test lists by partition
  prefix, which is also how the lake is actually read.
