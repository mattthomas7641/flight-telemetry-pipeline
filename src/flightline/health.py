"""Minimal HTTP endpoint for background workers: /metrics plus a /healthz that fails
when the main loop stops making progress, so Kubernetes restarts a wedged pod rather
than leaving it alive but doing nothing."""

from __future__ import annotations

import asyncio
import time

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class Heartbeat:
    def __init__(self, max_age_s: float = 30.0) -> None:
        self.max_age_s = max_age_s
        self._last = time.monotonic()

    def beat(self) -> None:
        self._last = time.monotonic()

    @property
    def healthy(self) -> bool:
        return time.monotonic() - self._last < self.max_age_s


async def serve(heartbeat: Heartbeat, port: int) -> asyncio.Server:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            path = line.split(b" ")[1] if line.count(b" ") >= 2 else b"/"
            if path.startswith(b"/metrics"):
                status, ctype, body = b"200 OK", CONTENT_TYPE_LATEST.encode(), generate_latest()
            elif path.startswith(b"/healthz"):
                ok = heartbeat.healthy
                status = b"200 OK" if ok else b"503 Service Unavailable"
                ctype, body = b"text/plain", b"ok\n" if ok else b"stalled\n"
            else:
                status, ctype, body = b"404 Not Found", b"text/plain", b"not found\n"
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nContent-Type: "
                + ctype
                + b"\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        except (TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(handle, "0.0.0.0", port)
