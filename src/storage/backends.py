from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class StorageBackendError(Exception):
    """Raised when a storage backend operation fails."""


class StorageBackend(ABC):
    """Abstract storage backend for lakehouse objects."""

    @abstractmethod
    def put_bytes(
        self,
        relative_path: str | Path,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        """Write bytes and return a backend URI."""

    @abstractmethod
    def put_text(
        self,
        relative_path: str | Path,
        text: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = "text/plain",
    ) -> str:
        """Write text and return a backend URI."""

    @abstractmethod
    def upload_file(
        self,
        local_path: str | Path,
        relative_path: str | Path,
        *,
        content_type: str | None = None,
    ) -> str:
        """Upload a local file and return a backend URI."""

    @abstractmethod
    def exists(self, relative_path: str | Path) -> bool:
        """Return whether an object exists."""

    @abstractmethod
    def uri(self, relative_path: str | Path) -> str:
        """Return the backend URI for a relative object path."""


class LocalStorageBackend(StorageBackend):
    """Local filesystem implementation of StorageBackend."""

    def __init__(self, root_path: str | Path = "lakehouse") -> None:
        self.root_path = Path(root_path)

    def put_bytes(
        self,
        relative_path: str | Path,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        target_path = self._resolve(relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)
        return self.uri(relative_path)

    def put_text(
        self,
        relative_path: str | Path,
        text: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = "text/plain",
    ) -> str:
        target_path = self._resolve(relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(text, encoding=encoding)
        return self.uri(relative_path)

    def upload_file(
        self,
        local_path: str | Path,
        relative_path: str | Path,
        *,
        content_type: str | None = None,
    ) -> str:
        source_path = Path(local_path)

        if not source_path.exists():
            raise StorageBackendError(f"Local file does not exist: {source_path}")

        target_path = self._resolve(relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(source_path.read_bytes())

        return self.uri(relative_path)

    def exists(self, relative_path: str | Path) -> bool:
        return self._resolve(relative_path).exists()

    def uri(self, relative_path: str | Path) -> str:
        path = self._resolve(relative_path)
        return path.as_posix()

    def _resolve(self, relative_path: str | Path) -> Path:
        clean_path = _clean_relative_path(relative_path)
        return self.root_path / clean_path


class S3StorageBackend(StorageBackend):
    """AWS S3 implementation of StorageBackend.

    The backend accepts an optional client for unit tests. In production it creates
    a boto3 S3 client.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        region_name: str | None = None,
        profile_name: str | None = None,
        s3_client: Any | None = None,
    ) -> None:
        if not bucket:
            raise StorageBackendError("S3 bucket must be non-empty.")

        self.bucket = bucket
        self.prefix = _clean_prefix(prefix)

        if s3_client is not None:
            self.s3_client = s3_client
        else:
            self.s3_client = self._build_boto3_client(
                region_name=region_name,
                profile_name=profile_name,
            )

    def put_bytes(
        self,
        relative_path: str | Path,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        key = self._key(relative_path)
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
        }

        if content_type:
            kwargs["ContentType"] = content_type

        self.s3_client.put_object(**kwargs)
        return self.uri(relative_path)

    def put_text(
        self,
        relative_path: str | Path,
        text: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = "text/plain",
    ) -> str:
        return self.put_bytes(
            relative_path,
            text.encode(encoding),
            content_type=content_type,
        )

    def upload_file(
        self,
        local_path: str | Path,
        relative_path: str | Path,
        *,
        content_type: str | None = None,
    ) -> str:
        source_path = Path(local_path)

        if not source_path.exists():
            raise StorageBackendError(f"Local file does not exist: {source_path}")

        key = self._key(relative_path)

        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        if extra_args:
            self.s3_client.upload_file(
                str(source_path),
                self.bucket,
                key,
                ExtraArgs=extra_args,
            )
        else:
            self.s3_client.upload_file(str(source_path), self.bucket, key)

        return self.uri(relative_path)

    def exists(self, relative_path: str | Path) -> bool:
        key = self._key(relative_path)

        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def uri(self, relative_path: str | Path) -> str:
        return f"s3://{self.bucket}/{self._key(relative_path)}"

    def _key(self, relative_path: str | Path) -> str:
        clean_path = _clean_relative_path(relative_path)

        if self.prefix:
            return f"{self.prefix}/{clean_path}"

        return clean_path

    @staticmethod
    def _build_boto3_client(
        *,
        region_name: str | None,
        profile_name: str | None,
    ) -> Any:
        try:
            import boto3
        except ImportError as exc:
            raise StorageBackendError("boto3 is required for S3StorageBackend.") from exc

        if profile_name:
            session = boto3.Session(
                profile_name=profile_name,
                region_name=region_name,
            )
            return session.client("s3")

        return boto3.client("s3", region_name=region_name)


def build_storage_backend_from_env(
    *,
    zone: str = "bronze",
) -> StorageBackend:
    """Build a storage backend from environment variables.

    STORAGE_BACKEND=local:
        writes to LOCAL_LAKEHOUSE_ROOT / zone

    STORAGE_BACKEND=s3:
        writes to AWS_S3_BUCKET_RAW / AWS_S3_BRONZE_PREFIX for bronze
        writes to AWS_S3_BUCKET_PROCESSED / AWS_S3_SILVER_PREFIX for silver
    """
    backend = os.getenv("STORAGE_BACKEND", "local").strip().lower()

    if backend == "local":
        root = Path(os.getenv("LOCAL_LAKEHOUSE_ROOT", "lakehouse")) / zone
        return LocalStorageBackend(root)

    if backend == "s3":
        region = os.getenv("AWS_REGION") or None
        profile = os.getenv("AWS_PROFILE") or None

        if zone == "bronze":
            bucket = os.getenv("AWS_S3_BUCKET_RAW", "")
            prefix = os.getenv("AWS_S3_BRONZE_PREFIX", "bronze")
        elif zone == "silver":
            bucket = os.getenv("AWS_S3_BUCKET_PROCESSED", "")
            prefix = os.getenv("AWS_S3_SILVER_PREFIX", "silver")
        else:
            bucket = os.getenv("AWS_S3_BUCKET_PROCESSED", "")
            prefix = zone

        return S3StorageBackend(
            bucket=bucket,
            prefix=prefix,
            region_name=region,
            profile_name=profile,
        )

    raise StorageBackendError(f"Unsupported STORAGE_BACKEND: {backend}")


def _clean_relative_path(relative_path: str | Path) -> str:
    raw = str(relative_path).replace("\\", "/").strip()

    if not raw:
        raise StorageBackendError("relative_path must be non-empty.")

    path = Path(raw)

    if path.is_absolute():
        raise StorageBackendError(f"Absolute paths are not allowed: {relative_path}")

    parts = raw.split("/")

    if any(part in {"", ".", ".."} for part in parts):
        raise StorageBackendError(f"Unsafe relative path: {relative_path}")

    return "/".join(parts)


def _clean_prefix(prefix: str | Path) -> str:
    raw = str(prefix).replace("\\", "/").strip().strip("/")

    if not raw:
        return ""

    parts = raw.split("/")

    if any(part in {"", ".", ".."} for part in parts):
        raise StorageBackendError(f"Unsafe storage prefix: {prefix}")

    return "/".join(parts)
