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

    assert len(uploaded) == 2

    assert uploaded[0][0] == "bronze"
    assert uploaded[0][2] == "data/a.txt"

    assert uploaded[1][0] == "gold"
    assert uploaded[1][2] == "data/b.txt"


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

        def upload_file(
            self,
            local_path,
            relative_path,
            *,
            content_type=None,
        ):
            calls.append(relative_path)

            if relative_path == "data/a.txt":
                raise RuntimeError("upload failed")

            return "unused"

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

    assert calls == ["data/a.txt"]

    assert report["results"][0]["status"] == "failed"
    assert "upload failed" in report["results"][0]["error"]
