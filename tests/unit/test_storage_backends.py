from pathlib import Path

import pytest

from src.storage import (
    LocalStorageBackend,
    S3StorageBackend,
    StorageBackendError,
    build_storage_backend_from_env,
)


class FakeS3Client:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        body = kwargs["Body"]
        self.objects[(bucket, key)] = body

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise FileNotFoundError(f"Missing object: s3://{Bucket}/{Key}")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}


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
    assert (tmp_path / "lakehouse/bronze/raw/source.txt").read_text(encoding="utf-8") == "payload"


def test_local_storage_rejects_unsafe_path(tmp_path):
    backend = LocalStorageBackend(tmp_path)

    with pytest.raises(StorageBackendError):
        backend.put_text("../escape.txt", "bad")

    with pytest.raises(StorageBackendError):
        backend.put_text("/absolute/path.txt", "bad")


def test_s3_storage_put_bytes_and_exists():
    fake_client = FakeS3Client()
    backend = S3StorageBackend(
        bucket="test-bucket",
        prefix="bronze",
        s3_client=fake_client,
    )

    uri = backend.put_bytes("source/file.json", b"{}", content_type="application/json")

    assert uri == "s3://test-bucket/bronze/source/file.json"
    assert fake_client.objects[("test-bucket", "bronze/source/file.json")] == b"{}"
    assert backend.exists("source/file.json")


def test_s3_storage_upload_file(tmp_path):
    fake_client = FakeS3Client()
    source = tmp_path / "payload.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")

    backend = S3StorageBackend(
        bucket="test-bucket",
        prefix="silver",
        s3_client=fake_client,
    )

    uri = backend.upload_file(source, "tables/payload.csv")

    assert uri == "s3://test-bucket/silver/tables/payload.csv"
    assert fake_client.objects[("test-bucket", "silver/tables/payload.csv")] == source.read_bytes()


def test_build_local_storage_backend_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKEHOUSE_ROOT", str(tmp_path / "lakehouse"))

    backend = build_storage_backend_from_env(zone="bronze")

    assert isinstance(backend, LocalStorageBackend)
    uri = backend.put_text("test.txt", "ok")
    assert uri.endswith("lakehouse/bronze/test.txt")


def test_s3_storage_rejects_missing_bucket():
    with pytest.raises(StorageBackendError):
        S3StorageBackend(bucket="", prefix="bronze", s3_client=FakeS3Client())
