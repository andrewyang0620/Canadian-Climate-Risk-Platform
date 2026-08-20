from pathlib import Path

import pytest

import src.storage.run_backfill_adls as runner
from src.storage.canonical_backfill import (
    CanonicalBackfillEntry,
    CanonicalBackfillObject,
    CanonicalBackfillPlan,
)


def make_plan(tmp_path: Path):
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"

    f1.write_text("abc", encoding="utf-8")
    f2.write_text("hello", encoding="utf-8")

    entries = (
        CanonicalBackfillEntry(
            name="bronze_a",
            zone="bronze",
            local_path=f1,
            remote_path="data/a.txt",
        ),
        CanonicalBackfillEntry(
            name="gold_b",
            zone="gold",
            local_path=f2,
            remote_path="data/b.txt",
        ),
    )

    objects = (
        CanonicalBackfillObject(
            entry_name="bronze_a",
            zone="bronze",
            local_path=f1,
            remote_path="data/a.txt",
            size_bytes=3,
        ),
        CanonicalBackfillObject(
            entry_name="gold_b",
            zone="gold",
            local_path=f2,
            remote_path="data/b.txt",
            size_bytes=5,
        ),
    )

    return CanonicalBackfillPlan(
        manifest_version=1,
        entries=entries,
        objects=objects,
    )


class FakeFileProperties:
    def __init__(self, size):
        self.size = size


class FakeAzureFileClient:
    def __init__(
        self,
        remote_sizes,
        path,
    ):
        self.remote_sizes = remote_sizes
        self.path = path

    def exists(self):
        return self.path in self.remote_sizes

    def get_file_properties(self):
        return FakeFileProperties(
            self.remote_sizes[self.path]
        )


class FakeAzureFileSystemClient:
    def __init__(self, remote_sizes=None):
        self.remote_sizes = (
            remote_sizes
            if remote_sizes is not None
            else {}
        )

    def get_file_client(self, path):
        return FakeAzureFileClient(
            self.remote_sizes,
            path,
        )


def test_execute_requires_azure_backend(
    tmp_path,
    monkeypatch,
):
    plan = make_plan(tmp_path)

    monkeypatch.delenv(
        "STORAGE_BACKEND",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="requires STORAGE_BACKEND",
    ):
        runner._execute_plan(plan)


def test_execute_uploads_all_objects(
    tmp_path,
    monkeypatch,
):
    plan = make_plan(tmp_path)

    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "azure",
    )

    uploaded = []

    class FakeAzureBackend:
        def __init__(self, zone):
            self.zone = zone
            self.file_system_client = (
                FakeAzureFileSystemClient()
            )

        def upload_file(
            self,
            local_path,
            relative_path,
            *,
            content_type=None,
        ):
            uploaded.append(
                (
                    self.zone,
                    Path(local_path),
                    relative_path,
                    content_type,
                )
            )

            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

        def uri(self, relative_path):
            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

    monkeypatch.setattr(
        runner,
        "AzureDataLakeStorageBackend",
        FakeAzureBackend,
    )

    monkeypatch.setattr(
        runner,
        "build_storage_backend_from_env",
        lambda zone: FakeAzureBackend(zone),
    )

    report = runner._execute_plan(plan)

    assert report["execution_status"] == "success"

    assert report["uploaded_object_count"] == 2
    assert report["uploaded_size_bytes"] == 8

    assert report["skipped_object_count"] == 0
    assert report["skipped_size_bytes"] == 0

    assert len(uploaded) == 2

    assert uploaded[0][0] == "bronze"
    assert uploaded[0][2] == "data/a.txt"

    assert uploaded[1][0] == "gold"
    assert uploaded[1][2] == "data/b.txt"

    assert report["results"][0]["status"] == "uploaded"
    assert report["results"][1]["status"] == "uploaded"


def test_execute_stops_on_first_failure(
    tmp_path,
    monkeypatch,
):
    plan = make_plan(tmp_path)

    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "azure",
    )

    calls = []

    class FakeAzureBackend:
        def __init__(self, zone):
            self.zone = zone
            self.file_system_client = (
                FakeAzureFileSystemClient()
            )

        def upload_file(
            self,
            local_path,
            relative_path,
            *,
            content_type=None,
        ):
            calls.append(relative_path)

            if relative_path == "data/a.txt":
                raise RuntimeError(
                    "upload failed"
                )

            return "unused"

        def uri(self, relative_path):
            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

    monkeypatch.setattr(
        runner,
        "AzureDataLakeStorageBackend",
        FakeAzureBackend,
    )

    monkeypatch.setattr(
        runner,
        "build_storage_backend_from_env",
        lambda zone: FakeAzureBackend(zone),
    )

    report = runner._execute_plan(plan)

    assert report["execution_status"] == "failed"

    assert report["uploaded_object_count"] == 0
    assert report["uploaded_size_bytes"] == 0

    assert report["skipped_object_count"] == 0
    assert report["skipped_size_bytes"] == 0

    assert calls == [
        "data/a.txt",
    ]

    assert report["results"][0]["status"] == "failed"

    assert (
        "upload failed"
        in report["results"][0]["error"]
    )


def test_execute_skips_existing_same_size(
    tmp_path,
    monkeypatch,
):
    plan = make_plan(tmp_path)

    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "azure",
    )

    uploaded = []

    remote_sizes_by_zone = {
        "bronze": {
            "data/a.txt": 3,
        },
        "gold": {
            "data/b.txt": 5,
        },
    }

    class FakeAzureBackend:
        def __init__(self, zone):
            self.zone = zone

            self.file_system_client = (
                FakeAzureFileSystemClient(
                    remote_sizes_by_zone[zone]
                )
            )

        def upload_file(
            self,
            local_path,
            relative_path,
            *,
            content_type=None,
        ):
            uploaded.append(
                relative_path
            )

            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

        def uri(self, relative_path):
            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

    monkeypatch.setattr(
        runner,
        "AzureDataLakeStorageBackend",
        FakeAzureBackend,
    )

    monkeypatch.setattr(
        runner,
        "build_storage_backend_from_env",
        lambda zone: FakeAzureBackend(zone),
    )

    report = runner._execute_plan(plan)

    assert report["execution_status"] == "success"

    assert report["uploaded_object_count"] == 0
    assert report["uploaded_size_bytes"] == 0

    assert report["skipped_object_count"] == 2
    assert report["skipped_size_bytes"] == 8

    assert uploaded == []

    assert (
        report["results"][0]["status"]
        == "skipped_existing"
    )

    assert (
        report["results"][1]["status"]
        == "skipped_existing"
    )


def test_execute_reuploads_existing_wrong_size(
    tmp_path,
    monkeypatch,
):
    plan = make_plan(tmp_path)

    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "azure",
    )

    uploaded = []

    remote_sizes_by_zone = {
        "bronze": {
            "data/a.txt": 2,
        },
        "gold": {
            "data/b.txt": 4,
        },
    }

    class FakeAzureBackend:
        def __init__(self, zone):
            self.zone = zone

            self.file_system_client = (
                FakeAzureFileSystemClient(
                    remote_sizes_by_zone[zone]
                )
            )

        def upload_file(
            self,
            local_path,
            relative_path,
            *,
            content_type=None,
        ):
            uploaded.append(
                relative_path
            )

            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

        def uri(self, relative_path):
            return (
                f"abfss://{self.zone}@test/"
                f"{relative_path}"
            )

    monkeypatch.setattr(
        runner,
        "AzureDataLakeStorageBackend",
        FakeAzureBackend,
    )

    monkeypatch.setattr(
        runner,
        "build_storage_backend_from_env",
        lambda zone: FakeAzureBackend(zone),
    )

    report = runner._execute_plan(plan)

    assert report["execution_status"] == "success"

    assert report["uploaded_object_count"] == 2
    assert report["uploaded_size_bytes"] == 8

    assert report["skipped_object_count"] == 0
    assert report["skipped_size_bytes"] == 0

    assert uploaded == [
        "data/a.txt",
        "data/b.txt",
    ]