import json
from pathlib import Path

import pytest

from src.storage.canonical_backfill import (
    build_canonical_backfill_plan,
)


def write_manifest(tmp_path: Path, entries, version=1):
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps({
            "manifest_version": version,
            "entries": entries,
        }),
        encoding="utf-8",
    )
    return path


def entry(name, zone, local_path, remote_path):
    return {
        "name": name,
        "zone": zone,
        "local_path": str(local_path),
        "remote_path": remote_path,
    }


def test_single_file(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("abc", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [entry("a", "bronze", f, "data/a.txt")],
    )

    plan = build_canonical_backfill_plan(manifest)

    assert plan.object_count == 1
    assert plan.total_size_bytes == 3
    assert plan.objects[0].remote_path == "data/a.txt"


def test_recursive_directory(tmp_path):
    root = tmp_path / "data"
    (root / "sub").mkdir(parents=True)

    (root / "a.txt").write_text("a", encoding="utf-8")
    (root / "sub" / "b.txt").write_text("bb", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [entry("data", "silver", root, "product")],
    )

    plan = build_canonical_backfill_plan(manifest)

    assert plan.object_count == 2
    assert {
        obj.remote_path
        for obj in plan.objects
    } == {
        "product/a.txt",
        "product/sub/b.txt",
    }


def test_deterministic_sort(tmp_path):
    root = tmp_path / "data"
    root.mkdir()

    (root / "z.txt").write_text("z", encoding="utf-8")
    (root / "a.txt").write_text("a", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [entry("data", "gold", root, "product")],
    )

    plan = build_canonical_backfill_plan(manifest)

    assert [
        obj.remote_path
        for obj in plan.objects
    ] == [
        "product/a.txt",
        "product/z.txt",
    ]


def test_missing_local_path_fails(tmp_path):
    manifest = write_manifest(
        tmp_path,
        [
            entry(
                "missing",
                "bronze",
                tmp_path / "missing.txt",
                "missing.txt",
            )
        ],
    )

    with pytest.raises(FileNotFoundError):
        build_canonical_backfill_plan(manifest)


def test_invalid_zone_fails(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [entry("a", "profiles", f, "a.txt")],
    )

    with pytest.raises(ValueError, match="Invalid zone"):
        build_canonical_backfill_plan(manifest)


def test_unsafe_remote_path_fails(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [entry("a", "bronze", f, "../a.txt")],
    )

    with pytest.raises(ValueError, match="Unsafe remote_path"):
        build_canonical_backfill_plan(manifest)


def test_duplicate_remote_target_fails(tmp_path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"

    a.write_text("a", encoding="utf-8")
    b.write_text("b", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [
            entry("a", "gold", a, "same.txt"),
            entry("b", "gold", b, "same.txt"),
        ],
    )

    with pytest.raises(ValueError, match="Duplicate remote target"):
        build_canonical_backfill_plan(manifest)


def test_overlapping_entries_fail(tmp_path):
    root = tmp_path / "root"
    child = root / "child"

    child.mkdir(parents=True)
    (child / "a.txt").write_text("x", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [
            entry("root", "silver", root, "root"),
            entry("child", "silver", child, "child"),
        ],
    )

    with pytest.raises(ValueError, match="Overlapping local entries"):
        build_canonical_backfill_plan(manifest)


def test_zone_filter(tmp_path):
    bronze = tmp_path / "bronze.txt"
    gold = tmp_path / "gold.txt"

    bronze.write_text("b", encoding="utf-8")
    gold.write_text("g", encoding="utf-8")

    manifest = write_manifest(
        tmp_path,
        [
            entry("bronze", "bronze", bronze, "bronze.txt"),
            entry("gold", "gold", gold, "gold.txt"),
        ],
    )

    plan = build_canonical_backfill_plan(
        manifest,
        zones=["gold"],
    )

    assert len(plan.entries) == 1
    assert plan.object_count == 1
    assert plan.objects[0].zone == "gold"


def test_unsupported_manifest_version_fails(tmp_path):
    manifest = write_manifest(
        tmp_path,
        [],
        version=999,
    )

    with pytest.raises(
        ValueError,
        match="Unsupported manifest_version",
    ):
        build_canonical_backfill_plan(manifest)
