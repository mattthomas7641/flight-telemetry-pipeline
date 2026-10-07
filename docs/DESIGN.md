# Flightline design

## Problem

A flight-test program streams thousands of channels per aircraft at 50–1000 Hz from
onboard data acquisition units (DAUs). The platform has to:

1. **Store everything**, durably and queryably, for years.
2. **Govern it**: classify each channel (some navigation and flight-control data of a
   developmental aircraft is export controlled), record ownership and retention, and
   prove which policy applied to any stored value.
3. **Keep up with bursts**: several aircraft flying at once, or a post-flight backfill
   from onboard recorders, must not cause data to fall behind or be dropped.
4. **Page a human** when a reading crosses a safety threshold, fast and reliably, and
   without crying wolf.

(4) is the requirement that shapes everything else. A pipeline that only stores data is
an ETL job; one that someone trusts during a flight test has to page correctly while the
storage side is degraded.

## Non-goals

- Onboard or safety-of-flight software. This is a ground system; the aircraft does not
  depend on it.
- Sub-10 ms alerting. Ground paging budgets are human-scale (seconds). The target is
  p99 ingest→page < 2 s, alerted on in `ops/alerts.yml`.
- Exactly-once delivery end to end. At-least-once plus idempotent sinks and natural-key
  dedup gives exactly-once *results*, which is what matters, at much lower complexity.

## Architecture

```
ingest (stateless) ──▶ Redis Streams, sharded by aircraft ──┬─▶ writer group  ──▶ S3 Parquet ──▶ compactor
                                                            └─▶ alerter group ──▶ PagerDuty / Slack
```

### Ingest: governance at the door

Every frame is validated individually (one bad frame quarantines itself, not the
batch), then each sample is resolved against `config/catalog.yaml`:

- **Registered** → stamped with classification, owner, retention class, unit, and
  lineage: `ingest_id` (per request), `catalog_version` (`v<N>-<sha256 prefix>` of the
  catalog file), `pipeline_version`.
- **Unregistered** → the *whole frame* is quarantined. Alternative considered: drop the
  unknown channel and keep the rest. Rejected because a DAU emitting an unregistered
  channel is misconfigured, and silently storing a partial frame hides that. The
  quarantine is visible (metric, alert, raw frame preserved for triage).
- **Physically impossible value** (outside the sensor's range) → stored with
  `quality=out_of_physical_range`, never paged on. A sensor fault is engineering data,
  not an emergency; paging on it trains on-call to ignore pages.
- **Future timestamp** beyond 30 s → quarantined as clock skew. Past timestamps are
  accepted: backfill is normal.

The catalog is mounted from a hash-suffixed ConfigMap, so changing policy is a deploy:
reviewed, auditable, revertible. Hot-reloading policy was rejected for that reason.

### The bus: sharded Redis Streams

Frames are routed to `fl:telemetry:{crc32(aircraft_id) % shards}`. This gives
Kafka-partition semantics: total order per aircraft (which stateful rules need) and
parallelism across aircraft.

**Why Redis Streams over Kafka / Kinesis / MSK?** Consumer groups, per-entry acks,
pending-entry lists and `XAUTOCLAIM` provide everything this design needs (at-least-once,
replay, takeover of a dead consumer) with one managed dependency (ElastiCache/MemoryDB)
that the team likely runs already. The trade-off is retention: Redis is a buffer, not a
log. Data is durable once it is in S3; Redis holds minutes of backlog. With Kafka/Kinesis
the stream itself could be replayed for days, which would matter for reprocessing
history through new rules. At larger scale (dozens of aircraft, kHz channels) the
`TelemetryBus` interface is the seam where Kinesis or MSK would go.

**Entry codec.** Within an entry every frame comes from one request and so one catalog
version, which makes the policy tags identical per channel. They are dictionary-encoded
once per entry and the JSON is zlib-1 compressed: 15× smaller than naive per-sample
JSON. Profiling showed the bus was bandwidth-bound, not CPU-bound. The codec is versioned
(`v: 2`), and consumers dead-letter anything they cannot decode (see Failure modes).
Codec changes must deploy consumers before producers.

### Writer: stateless, scales on lag

Writers read the writer group across all shards, buffer rows, and flush every 5 s or
250k rows into Parquet (zstd, dictionary-encoded string columns), partitioned:

```
telemetry/classification=<c>/campaign=<x>/aircraft=<a>/date=YYYY-MM-DD/hour=HH/part-<batch>.parquet
```

Classification first, because that is the IAM boundary. Objects carry
`classification` and `retention_class` tags set atomically with the PUT. The bucket
policy denies untagged PUTs, so the governance invariant is enforced server-side.

Entries are acked only after **every** object in a flush is written. The object key
derives from the hash of the entry IDs in the batch, so retrying a failed flush
overwrites the same keys rather than creating duplicates.

Writers scale on **stream lag** via KEDA (one trigger per shard; KEDA takes the max).
CPU is the wrong signal for a consumer that spends its time blocked on S3: lag leads a
burst, CPU lags it. When its buffer is full a writer stops reading, so the backlog shows
up as lag, which is exactly the signal KEDA scales on.

### Compactor: small files and duplicates

Five-second flushes are right for freshness and wrong for query engines, which pay per
object opened. An hourly CronJob compacts the hour that closed two hours ago (the gap is
the late-data window): it merges a partition's files, deduplicates on the natural key
`(source_id, seq, channel)`, sorts by `(channel, ts)`, writes `compacted-<hash>.parquet`,
then deletes the inputs.

There are two duplicate sources. A writer can crash after writing but before acking, and
a DAU can retry a request whose 202 was lost. The second is the common one, and no amount
of broker-side exactly-once would fix it.

Crash safety needs no locks. Output is written before inputs are deleted, under a key
derived from the input set; a crash between the two leaves duplicates that the next run
removes, because compacted files are valid inputs. Late data simply produces new part
files that the next run merges.

### Alerter: stateful, shard-owned

The alerter has its own consumer group, so storage backpressure never delays a page.
That claim was measured: writer p99 ingest→durable sat at 30 s under saturation while
mean ingest→page was 166 ms.

Rules are stateful per `(rule, aircraft, flight, channel)`, so any-replica-any-shard
(as the writer does) would split an aircraft's state across pods. Instead the alerter
is a **StatefulSet**: replica *N* owns shards `s % replicas == N`, and the stable pod name
doubles as the consumer name. A restarted `alerter-1` replays exactly its own pending
entries. Resharding means changing `replicas` and `FLIGHTLINE_ALERTER_REPLICAS` together,
a deliberate operation rather than an autoscaling event. The cost: on restart, in-memory
state is lost. For thresholds the worst case is one sustain window (≤ 1 s) of extra
delay before a page. For telemetry loss, a flight that went silent *while* its alerter was
down is not tracked until it sends again; `AlerterLagging` and pod-restart alerts cover
that window. Persisting flight liveness to Redis would close it at a per-frame write cost.

**Why not Flink?** Keyed state with event-time semantics is exactly Flink's job, and
checkpointed state would remove the restart-state caveat above. It was rejected at this
scale for operational weight: a JobManager/TaskManager cluster, checkpoint storage, and a
JVM (or PyFlink) runtime, all to evaluate a few dozen threshold rules. A shard-owned
StatefulSet gives the same per-key ordering and ownership with the deployment model the
rest of the platform already uses. If rules grow into windowed aggregations or
cross-channel correlations, that changes the answer.

**Rule semantics** (`config/rules.yaml`):

- **Event time, not wall clock.** A level fires once it has held for `sustain_ms` of
  sample time. Replay, backfill or a slow consumer therefore produce identical alerts,
  which also makes the engine deterministic to test.
- **Per-level sustain timers.** A reading that jumps straight to critical pages critical
  after one sustain window rather than paging warning first.
- **Hysteresis** on the way down stops flapping at the threshold. De-escalation is
  immediate: on-call should learn of recovery now.
- **One incident per (rule, aircraft, flight, channel).** Warning→critical→resolve all
  use the same PagerDuty `dedup_key`. That is also what makes at-least-once safe: a
  redelivered trigger updates the incident instead of opening a second one.
- **Telemetry loss.** A watchdog pages if an active flight goes silent for 5 s. A clean
  `shutdown` frame resolves open alerts and frees state.

**Dispatch.** Concurrent across incidents and ordered within one, so a trigger and its
resolve never race. Each notifier retries independently with full-jitter backoff; 4xx
responses are not retried. Every alert also goes to an audit stream regardless of
delivery outcome.

**Hot path.** Profiling put most alerter time in materialising unwatched samples and
running the state machine on nominal readings. A fast path (state OK, no pending timers,
value inside the lowest threshold → return) plus decode-only-watched-channels gave 5.4×,
to 1.75M samples/s per core.

## Failure modes

| Failure | Effect | Mitigation |
|---|---|---|
| Writer pod dies mid-flush | Entries stay pending | Same-name restart resumes them; other replicas `XAUTOCLAIM` after 60 s. Deterministic keys make retries idempotent. |
| S3 slow or erroring | Writer lag grows | Flush retries with backoff, nothing acked; KEDA adds writers; **alerting unaffected** (separate group). `WriterFallingBehind` alert. |
| Redis down | Ingest `/readyz` fails, LB stops routing; DAUs buffer and retry | AOF `everysec` (ElastiCache Multi-AZ in prod). At most ~1 s of *acked-to-DAU* data at risk on a hard crash, a known and documented RPO. |
| Writer backlog unbounded | Redis memory exhaustion | Ingest returns `503 Retry-After` above `backpressure_max_lag`; DAUs hold data. |
| Undecodable entry (bad deploy, codec skew) | Previously: alerter crash-looped and stopped paging | Dead-letter stream + ack + `DeadLetteredEntries` critical alert. Regression tests cover it. |
| Alerter pod dies | Its shards stop being evaluated until it restarts | StatefulSet restarts it (seconds); PDB `maxUnavailable: 1`; `AlerterLagging` alert; Guaranteed QoS + PriorityClass so it is the last thing evicted. |
| PagerDuty unreachable | Pages not delivered | Retries, then `PageDeliveryFailing` alert via the platform's own Alertmanager (an independent path), Slack in parallel, and the audit stream. |
| DAU clock wrong | Future timestamps would corrupt event-time rules | Quarantined at ingest (`clock_skew`). |
| Duplicate frames (DAU retry) | Duplicate rows | Natural-key dedup in compaction; PagerDuty dedup for alerts. |

## Security

- Non-root, read-only root filesystem, all capabilities dropped, `RuntimeDefault`
  seccomp; namespace enforces Pod Security `restricted` (prod).
- Default-deny NetworkPolicy; ingest port open, metrics only from `monitoring`.
- No static AWS credentials in prod: IRSA, with a trust policy pinned to one service
  account. Secrets come from AWS Secrets Manager via External Secrets.
- SSE-KMS with rotation, TLS-only bucket policy, versioning, public access blocked,
  ACLs disabled.
- Optional bearer-token auth on ingest with constant-time comparison. In production
  this would be mTLS per DAU at the load balancer.

## What I would build next

1. **Schema registry for channels.** The catalog is YAML in git; at program scale it
   becomes a service with an API that DAU configuration tooling validates against
   pre-flight (`GET /v1/catalog` is the start of that).
2. **Kinesis/MSK behind `TelemetryBus`** for multi-day replay, so new rules can be
   back-tested against historical flights.
3. **Rule back-testing CLI**: run `rules.yaml` changes against stored flights and diff
   the alerts before deploying. Event-time semantics make this exact.
4. **Glue catalog + Athena tables** over the lake, with Lake Formation tag-based access
   mirroring the classification tags.
5. **Writer throughput.** Row assembly in Python lists is the writer's ceiling (~100k
   rows/s/core). Building Arrow arrays directly from the decoded entry would roughly
   triple it.
