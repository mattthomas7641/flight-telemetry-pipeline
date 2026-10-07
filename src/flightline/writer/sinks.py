"""Object sinks for the data lake. S3 in production; local filesystem for dev/tests."""

from __future__ import annotations

import asyncio
import os
import posixpath
from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from urllib.parse import urlencode

import orjson

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class Sink(Protocol):
    async def put(self, key: str, body: bytes, tags: dict[str, str], content_type: str) -> None:
        """Durably store `body` at `key`. Must be idempotent: retries reuse the same key."""
        ...

    async def list_keys(self, prefix: str) -> list[str]: ...

    async def read(self, key: str) -> bytes: ...

    async def delete_keys(self, keys: list[str]) -> None: ...


class LocalSink:
    def __init__(self, root: Path) -> None:
        self.root = root

    async def put(self, key: str, body: bytes, tags: dict[str, str], content_type: str) -> None:
        await asyncio.to_thread(self._put, key, body, tags)

    def _put(self, key: str, body: bytes, tags: dict[str, str]) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename so a reader never sees a half-written file.
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, path)
        # Sidecar mirrors S3 object tags, keeping the local lake governance-equivalent.
        path.with_suffix(path.suffix + ".tags.json").write_bytes(orjson.dumps(tags))

    async def list_keys(self, prefix: str) -> list[str]:
        # S3 prefix semantics: a plain string match, not a directory boundary.
        def _list() -> list[str]:
            base = self.root / posixpath.dirname(prefix)
            if not base.exists():
                return []
            keys = (p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())
            return sorted(
                k for k in keys if k.startswith(prefix) and not k.endswith((".tags.json", ".tmp"))
            )

        return await asyncio.to_thread(_list)

    async def read(self, key: str) -> bytes:
        return await asyncio.to_thread((self.root / key).read_bytes)

    async def delete_keys(self, keys: list[str]) -> None:
        def _delete() -> None:
            for k in keys:
                for p in (self.root / k, self.root / f"{k}.tags.json"):
                    p.unlink(missing_ok=True)

        await asyncio.to_thread(_delete)


class S3Sink:
    def __init__(
        self,
        bucket: str,
        endpoint_url: str | None = None,
        client: S3Client | None = None,
        kms_key_id: str | None = None,
    ) -> None:
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.client = client
        self.bucket = bucket
        self.kms_key_id = kms_key_id

    async def put(self, key: str, body: bytes, tags: dict[str, str], content_type: str) -> None:
        await asyncio.to_thread(self._put, key, body, tags, content_type)

    def _put(self, key: str, body: bytes, tags: dict[str, str], content_type: str) -> None:
        extra: dict[str, str] = {}
        if self.kms_key_id:
            extra = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": self.kms_key_id}
        # Object tags drive lifecycle (retention) and can be used in IAM conditions
        # (s3:ExistingObjectTag/classification), so they are set atomically with the PUT.
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            Tagging=urlencode(tags),
            **extra,  # type: ignore[arg-type]
        )

    async def list_keys(self, prefix: str) -> list[str]:
        def _list() -> list[str]:
            keys: list[str] = []
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix
            ):
                keys.extend(o["Key"] for o in page.get("Contents", []))
            return keys

        return await asyncio.to_thread(_list)

    async def read(self, key: str) -> bytes:
        def _get() -> bytes:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

        return await asyncio.to_thread(_get)

    async def delete_keys(self, keys: list[str]) -> None:
        def _delete() -> None:
            for i in range(0, len(keys), 1000):  # DeleteObjects limit
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]], "Quiet": True},
                )

        await asyncio.to_thread(_delete)
