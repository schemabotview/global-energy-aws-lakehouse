"""Small object-store abstraction so the same code runs on S3 (Glue, Databricks) and on a local folder (tests)."""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Obj:
    key: str
    mtime: dt.datetime  # timezone-aware UTC


class Store:
    def list(self, prefix: str = "") -> list[Obj]: ...
    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...

    def read_json(self, key: str):
        return json.loads(self.read_bytes(key)) if self.exists(key) else None

    def write_json(self, key: str, obj) -> None:
        self.write_bytes(key, json.dumps(obj, indent=2, default=str).encode())

    def copy_to(self, key: str, dst: Store, dst_key: str) -> None:
        dst.write_bytes(dst_key, self.read_bytes(key))


class LocalStore(Store):
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def list(self, prefix: str = "") -> list[Obj]:
        if not self.root.exists():
            return []
        out = []
        for p in self.root.rglob("*"):
            if p.is_file():
                key = p.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    out.append(Obj(key, dt.datetime.fromtimestamp(p.stat().st_mtime, tz=dt.timezone.utc)))
        return sorted(out, key=lambda o: o.key)

    def read_bytes(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def exists(self, key: str) -> bool:
        return (self.root / key).is_file()

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)


class S3Store(Store):
    def __init__(self, bucket: str, prefix: str = ""):
        import boto3  # provided by Glue and Databricks runtimes

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3")

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def list(self, prefix: str = "") -> list[Obj]:
        out = []
        base = self._k(prefix)
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=base):
            for o in page.get("Contents", []):
                rel = o["Key"][len(self.prefix) + 1:] if self.prefix else o["Key"]
                if rel and not rel.endswith("/"):
                    out.append(Obj(rel, o["LastModified"].astimezone(dt.timezone.utc)))
        return sorted(out, key=lambda o: o.key)

    def read_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.s3.head_object(Bucket=self.bucket, Key=self._k(key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=self._k(key))


def open_store(uri: str) -> Store:
    if uri.startswith("s3://"):
        bucket, _, prefix = uri[5:].partition("/")
        return S3Store(bucket, prefix)
    return LocalStore(uri)
