# Flightline runbook

Every page links here. Flight-safety pages (first section) concern the aircraft; platform
pages (second section) concern the pipeline that produces the flight-safety pages.

> **Flight-safety pages are advisory to the test conductor.** This ground system does
> not replace onboard monitoring or the test card's own limits. Follow the test card.

## Flight-safety pages

### motor-winding-overtemp
**Meaning:** a propulsion motor's winding temperature has held above 140 °C (warning) or
165 °C (critical) for 400 ms.
1. Tell the test conductor: tail number, motor index and current value (in the page).
2. Check the same motor's `rpm`. High temperature at low or zero RPM on a lift motor in
   cruise (should be stowed) suggests a bearing or ESC fault rather than load.
3. Check neighbouring motors. If several are rising together, suspect load or ambient
   conditions rather than one failing unit.
4. The page auto-resolves once the reading is 5 °C below the threshold.

### battery-cell-overtemp
**Meaning:** the hottest cell in a pack has held above 55 °C / 65 °C for 1 s.
Notify the test conductor immediately at critical. Compare against `battery.pack_current_a`:
heating without matching current draw is the dangerous case.

### battery-pack-undervoltage
**Meaning:** pack voltage has held below 640 V / 600 V for 500 ms. Check `battery.soc_pct`.
Low SoC means an energy-margin call for the test conductor; normal SoC with low voltage
suggests a cell or connection fault.

### structural-vibration
**Meaning:** RMS vibration has held above 3 g / 5 g for 250 ms. Correlate with flight
phase (transition is expected to be higher) and with individual motor RPMs to localise.

### telemetry-loss
**Meaning:** an active flight has sent no frames for over 5 s.
1. Is it one aircraft or all of them? All → platform problem: check ingest health and
   [alerter-lagging](#alerter-lagging). One → link or DAU problem: contact the ground
   station.
2. Resolves automatically when frames resume. Data the DAU buffered will backfill.

## Platform pages

### alert-latency-high
p99 ingest→page above 2 s. Check `flightline_stream_lag_entries{group="alerter"}`. If it
is lag, see [alerter-lagging](#alerter-lagging). If lag is near zero, the delay is in
delivery: check `flightline_notify_failures_total` and PagerDuty status.

### alerter-lagging
The alerter is behind the stream, so safety pages are delayed. **Treat as critical.**
1. `kubectl -n flightline get pods -l app.kubernetes.io/name=alerter`: is a replica
   down or crash-looping? Check logs for exceptions.
2. If a replica is healthy but behind, it is CPU-bound: check its CPU against its 500m
   limit. Short term, raise the limit. Long term, reshard (increase `replicas` *and*
   `FLIGHTLINE_ALERTER_REPLICAS` together).

### writer-falling-behind
Writer lag is above 5k entries for 5 min. Paging is unaffected; data freshness in the
lake is not.
1. Check KEDA scaled the deployment: `kubectl -n flightline get hpa,scaledobject`.
2. Check `flightline_writer_flush_errors_total` and logs for S3 errors (throttling, KMS,
   IAM).
3. If ingest is returning 503s, DAUs are buffering. That is by design, but tell the test
   team.

### page-delivery-failing
Alerts fire but PagerDuty delivery has exhausted its retries. Slack and the
`fl:alerts` audit stream still have them:
`redis-cli XREVRANGE fl:alerts + - COUNT 20`. Check the routing key secret and PagerDuty
status. Relay any open critical alerts to the test conductor manually.

### dead-lettered-entries
Stream entries could not be decoded and were parked so the consumers keep running.
1. Inspect: `redis-cli XREVRANGE fl:deadletter + - COUNT 5` (fields: `stream`, `id`,
   `group`, `error`, raw payload `d`).
2. The usual cause is codec skew during a deploy: producers on a newer entry codec than
   consumers. Roll consumers forward first.
3. The data is **not** in the lake. After the fix, re-publish the payloads from the
   dead-letter stream.

### quarantine-spike
Frames are being quarantined at a sustained rate. Check
`flightline_ingest_quarantined_total` by `reason`:
- `unregistered_channel`: a DAU config has a channel the catalog does not. Either fix
  the DAU or add the channel to `config/catalog.yaml` (with its classification, which
  needs a data-governance reviewer).
- `clock_skew`: the DAU clock is ahead; check its time source.
- `schema_invalid`: client bug; the 202 response body lists offending frame indices.
