from __future__ import annotations

from pathlib import Path


def latest_table_parquet(
    *,
    root: str | Path,
    table_name: str,
) -> Path:
    candidates = list((Path(root) / table_name).glob("extract_date=*/run_id=*/*.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No Parquet output found for {table_name}.")

    return max(
        candidates,
        key=lambda path: path.stat().st_mtime,
    )


def latest_partitioned_table_parquet_files(
    *,
    table_root: str | Path,
    partition_pattern: str,
) -> list[Path]:
    candidates = list(Path(table_root).glob(f"extract_date=*/run_id=*/{partition_pattern}"))

    if not candidates:
        raise FileNotFoundError(
            "No partitioned Parquet output found under "
            f"{table_root} with pattern {partition_pattern}."
        )

    latest_extract_date = max(_partition_value(path, "extract_date") for path in candidates)

    latest_extract_files = [
        path for path in candidates if _partition_value(path, "extract_date") == latest_extract_date
    ]

    run_ids = {_partition_value(path, "run_id") for path in latest_extract_files}

    def run_modified_time(run_id: str) -> float:
        return max(
            path.stat().st_mtime
            for path in latest_extract_files
            if _partition_value(path, "run_id") == run_id
        )

    latest_run_id = max(
        run_ids,
        key=lambda run_id: (
            run_modified_time(run_id),
            run_id,
        ),
    )

    return sorted(
        path for path in latest_extract_files if _partition_value(path, "run_id") == latest_run_id
    )


def _partition_value(path: Path, partition_name: str) -> str:
    prefix = f"{partition_name}="

    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)

    raise ValueError(f"Could not find partition {partition_name} in path {path}.")
