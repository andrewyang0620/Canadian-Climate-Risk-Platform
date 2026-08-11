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
        target_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
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
        target_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target_path.write_text(
            text,
            encoding=encoding,
        )
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
            raise StorageBackendError(
                f"Local file does not exist: {source_path}"
            )

        target_path = self._resolve(relative_path)
        target_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        target_path.write_bytes(
            source_path.read_bytes()
        )

        return self.uri(relative_path)

    def exists(
        self,
        relative_path: str | Path,
    ) -> bool:
        return self._resolve(
            relative_path
        ).exists()

    def uri(
        self,
        relative_path: str | Path,
    ) -> str:
        return self._resolve(
            relative_path
        ).as_posix()

    def _resolve(
        self,
        relative_path: str | Path,
    ) -> Path:
        return (
            self.root_path
            / _clean_relative_path(relative_path)
        )


class AzureDataLakeStorageBackend(StorageBackend):
    """Azure Data Lake Storage Gen2 implementation."""

    def __init__(
        self,
        *,
        account_name: str,
        file_system: str,
        file_system_client: Any | None = None,
    ) -> None:
        if not account_name:
            raise StorageBackendError(
                "Azure storage account name must be non-empty."
            )

        if not file_system:
            raise StorageBackendError(
                "Azure file system name must be non-empty."
            )

        self.account_name = account_name
        self.file_system = file_system

        self.file_system_client = (
            file_system_client
            if file_system_client is not None
            else self._build_file_system_client()
        )

    def put_bytes(
        self,
        relative_path: str | Path,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        file_client = self._file_client(
            relative_path
        )

        file_client.upload_data(
            data,
            overwrite=True,
        )

        self._set_content_type(
            file_client=file_client,
            content_type=content_type,
        )

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
            raise StorageBackendError(
                f"Local file does not exist: {source_path}"
            )

        file_client = self._file_client(
            relative_path
        )

        with source_path.open("rb") as data:
            file_client.upload_data(
                data,
                overwrite=True,
            )

        self._set_content_type(
            file_client=file_client,
            content_type=content_type,
        )

        return self.uri(relative_path)

    def exists(
        self,
        relative_path: str | Path,
    ) -> bool:
        return bool(
            self._file_client(
                relative_path
            ).exists()
        )

    def uri(
        self,
        relative_path: str | Path,
    ) -> str:
        path = _clean_relative_path(
            relative_path
        )

        return (
            f"abfss://{self.file_system}"
            f"@{self.account_name}.dfs.core.windows.net/"
            f"{path}"
        )

    def _file_client(
        self,
        relative_path: str | Path,
    ) -> Any:
        path = _clean_relative_path(
            relative_path
        )

        return (
            self.file_system_client
            .get_file_client(path)
        )

    def _build_file_system_client(
        self,
    ) -> Any:
        try:
            from azure.identity import (
                DefaultAzureCredential,
            )
            from azure.storage.filedatalake import (
                DataLakeServiceClient,
            )
        except ImportError as exc:
            raise StorageBackendError(
                "azure-identity and "
                "azure-storage-file-datalake are required "
                "for AzureDataLakeStorageBackend."
            ) from exc

        credential = DefaultAzureCredential()

        service_client = DataLakeServiceClient(
            account_url=(
                f"https://{self.account_name}"
                ".dfs.core.windows.net"
            ),
            credential=credential,
        )

        return service_client.get_file_system_client(
            self.file_system
        )

    @staticmethod
    def _set_content_type(
        *,
        file_client: Any,
        content_type: str | None,
    ) -> None:
        if not content_type:
            return

        try:
            from azure.storage.filedatalake import (
                ContentSettings,
            )
        except ImportError as exc:
            raise StorageBackendError(
                "azure-storage-file-datalake is required "
                "to set Azure file content type."
            ) from exc

        file_client.set_http_headers(
            content_settings=ContentSettings(
                content_type=content_type
            )
        )


def build_storage_backend_from_env(
    *,
    zone: str = "bronze",
) -> StorageBackend:
    """Build local or ADLS Gen2 storage from environment."""

    backend = (
        os.getenv(
            "STORAGE_BACKEND",
            "local",
        )
        .strip()
        .lower()
    )

    zone = zone.strip().lower()

    if backend == "local":
        root = (
            Path(
                os.getenv(
                    "LOCAL_LAKEHOUSE_ROOT",
                    "lakehouse",
                )
            )
            / zone
        )

        return LocalStorageBackend(root)

    if backend in {
        "azure",
        "adls",
        "adls2",
    }:
        account_name = os.getenv(
            "AZURE_STORAGE_ACCOUNT_NAME",
            "",
        )

        file_system = os.getenv(
            (
                "AZURE_STORAGE_FILE_SYSTEM_"
                f"{zone.upper()}"
            ),
            zone,
        )

        return AzureDataLakeStorageBackend(
            account_name=account_name,
            file_system=file_system,
        )

    raise StorageBackendError(
        f"Unsupported STORAGE_BACKEND: {backend}"
    )


def _clean_relative_path(
    relative_path: str | Path,
) -> str:
    raw = (
        str(relative_path)
        .replace("\\", "/")
        .strip()
    )

    if not raw:
        raise StorageBackendError(
            "relative_path must be non-empty."
        )

    path = Path(raw)

    if path.is_absolute():
        raise StorageBackendError(
            "Absolute paths are not allowed: "
            f"{relative_path}"
        )

    parts = raw.split("/")

    if any(
        part in {"", ".", ".."}
        for part in parts
    ):
        raise StorageBackendError(
            f"Unsafe relative path: {relative_path}"
        )

    return "/".join(parts)