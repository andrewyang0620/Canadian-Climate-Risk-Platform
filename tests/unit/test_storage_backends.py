from pathlib import Path

import pytest

from src.storage import (
    AzureDataLakeStorageBackend,
    LocalStorageBackend,
    StorageBackendError,
    build_storage_backend_from_env,
)


class FakeAzureFileClient:
    def __init__(self, objects, path):
        self.objects = objects
        self.path = path
        self.content_type = None
        self.upload_length = None
        self.upload_chunk_size = None
        self.upload_max_concurrency = None

    def upload_data(
        self,
        data,
        *,
        overwrite=False,
        length=None,
        chunk_size=None,
        max_concurrency=None,
    ):
        self.upload_length = length
        self.upload_chunk_size = chunk_size
        self.upload_max_concurrency = max_concurrency

        if hasattr(data, "read"):
            data = data.read()

        self.objects[self.path] = data

    def exists(self):
        return self.path in self.objects

    def set_http_headers(self, *, content_settings):
        self.content_type = content_settings.content_type


class FakeAzureFileSystemClient:
    def __init__(self):
        self.objects = {}

    def get_file_client(self, path):
        return FakeAzureFileClient(self.objects, path)


def test_local_storage_put_text_and_exists(tmp_path):
    backend = LocalStorageBackend(tmp_path)

    uri = backend.put_text("bronze/test/file.txt", "hello")

    assert uri.endswith("bronze/test/file.txt")
    assert backend.exists("bronze/test/file.txt")
    assert (tmp_path / "bronze/test/file.txt").read_text(encoding="utf-8") == "hello"


def test_local_storage_upload_file(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("payload", encoding="utf-8")

    backend = LocalStorageBackend(tmp_path / "lakehouse")
    backend.upload_file(source, "bronze/raw/source.txt")

    assert backend.exists("bronze/raw/source.txt")
    assert (
        tmp_path / "lakehouse/bronze/raw/source.txt"
    ).read_text(encoding="utf-8") == "payload"


def test_local_storage_rejects_unsafe_path(tmp_path):
    backend = LocalStorageBackend(tmp_path)

    with pytest.raises(StorageBackendError):
        backend.put_text("../escape.txt", "bad")

    with pytest.raises(StorageBackendError):
        backend.put_text("/absolute/path.txt", "bad")


def test_azure_storage_put_bytes_and_exists():
    fake_client = FakeAzureFileSystemClient()

    backend = AzureDataLakeStorageBackend(
        account_name="climateriskdev",
        file_system="bronze",
        file_system_client=fake_client,
    )

    uri = backend.put_bytes(
        "source/file.json",
        b"{}",
    )

    assert (
        uri
        == "abfss://bronze@climateriskdev.dfs.core.windows.net/source/file.json"
    )
    assert fake_client.objects["source/file.json"] == b"{}"
    assert backend.exists("source/file.json")


def test_azure_storage_upload_file(tmp_path):
    fake_client = FakeAzureFileSystemClient()

    source = tmp_path / "payload.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")

    backend = AzureDataLakeStorageBackend(
        account_name="climateriskdev",
        file_system="silver",
        file_system_client=fake_client,
    )

    uri = backend.upload_file(
        source,
        "tables/payload.csv",
    )

    assert (
        uri
        == "abfss://silver@climateriskdev.dfs.core.windows.net/tables/payload.csv"
    )
    assert fake_client.objects["tables/payload.csv"] == source.read_bytes()


def test_build_local_storage_backend_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKEHOUSE_ROOT", str(tmp_path / "lakehouse"))

    backend = build_storage_backend_from_env(zone="bronze")

    assert isinstance(backend, LocalStorageBackend)

    uri = backend.put_text("test.txt", "ok")

    assert uri.endswith("lakehouse/bronze/test.txt")


def test_build_azure_storage_backend_from_env(monkeypatch):
    fake_client = FakeAzureFileSystemClient()

    monkeypatch.setenv("STORAGE_BACKEND", "azure")
    monkeypatch.setenv(
        "AZURE_STORAGE_ACCOUNT_NAME",
        "climateriskdev",
    )
    monkeypatch.setenv(
        "AZURE_STORAGE_FILE_SYSTEM_BRONZE",
        "bronze",
    )

    monkeypatch.setattr(
        AzureDataLakeStorageBackend,
        "_build_file_system_client",
        lambda self: fake_client,
    )

    backend = build_storage_backend_from_env(zone="bronze")

    assert isinstance(
        backend,
        AzureDataLakeStorageBackend,
    )
    assert backend.account_name == "climateriskdev"
    assert backend.file_system == "bronze"


def test_azure_storage_rejects_missing_account():
    with pytest.raises(StorageBackendError):
        AzureDataLakeStorageBackend(
            account_name="",
            file_system="bronze",
            file_system_client=FakeAzureFileSystemClient(),
        )


def test_azure_storage_rejects_missing_file_system():
    with pytest.raises(StorageBackendError):
        AzureDataLakeStorageBackend(
            account_name="climateriskdev",
            file_system="",
            file_system_client=FakeAzureFileSystemClient(),
        )