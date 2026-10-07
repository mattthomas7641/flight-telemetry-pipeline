"""Structured logging and the Prometheus metrics every component exports.

Metrics are defined in one place so dashboards, alerts and code agree on names.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Any

import orjson
from prometheus_client import Counter, Gauge, Histogram

# Latency buckets span sub-millisecond ingest through multi-second end-to-end paging.
_LATENCY = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)

INGEST_FRAMES = Counter(
    "flightline_ingest_frames_total", "Frames received by the ingest API", ["result"]
)
INGEST_QUARANTINED = Counter(
    "flightline_ingest_quarantined_total", "Frames quarantined at ingest", ["reason"]
)
INGEST_REQUEST_SECONDS = Histogram(
    "flightline_ingest_request_seconds", "Ingest request latency", buckets=_LATENCY
)
INGEST_REJECTED = Counter(
    "flightline_ingest_rejected_total", "Requests rejected before processing", ["reason"]
)

WRITER_ROWS = Counter(
    "flightline_writer_rows_total", "Rows durably written to the lake", ["classification"]
)
WRITER_FILES = Counter("flightline_writer_files_total", "Objects written to the lake")
WRITER_FLUSH_SECONDS = Histogram(
    "flightline_writer_flush_seconds", "Time to encode and persist one flush", buckets=_LATENCY
)
WRITER_RECLAIMED = Counter(
    "flightline_writer_reclaimed_entries_total", "Entries reclaimed from dead consumers"
)
WRITER_FLUSH_ERRORS = Counter("flightline_writer_flush_errors_total", "Failed flushes")
DEAD_LETTERED = Counter(
    "flightline_dead_lettered_total", "Undecodable stream entries parked", ["group"]
)
STREAM_LAG = Gauge(
    "flightline_stream_lag_entries", "Unprocessed entries for a consumer group", ["group"]
)
INGEST_TO_DURABLE_SECONDS = Histogram(
    "flightline_ingest_to_durable_seconds",
    "Time from ingest to durable lake write (oldest frame in each flush)",
    buckets=_LATENCY,
)

ALERTER_SAMPLES = Counter(
    "flightline_alerter_samples_total", "Samples evaluated against rules", ["quality"]
)
ALERTS = Counter("flightline_alerts_total", "Alert transitions", ["rule", "severity", "action"])
ALERT_LATENCY_SECONDS = Histogram(
    "flightline_alert_latency_seconds",
    "Time from frame ingest to page dispatched",
    buckets=_LATENCY,
)
NOTIFY_FAILURES = Counter(
    "flightline_notify_failures_total", "Alert deliveries that exhausted retries", ["notifier"]
)
ACTIVE_ALERTS = Gauge("flightline_active_alerts", "Alerts currently firing", ["severity"])


class JsonFormatter(logging.Formatter):
    converter = staticmethod(time.gmtime)  # UTC everywhere; flight-test timelines span time zones
    _RESERVED = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": f"{self.formatTime(record, '%Y-%m-%dT%H:%M:%S')}.{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update({k: v for k, v in record.__dict__.items() if k not in self._RESERVED})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return orjson.dumps(out, default=str).decode()


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("uvicorn.access", "botocore", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
