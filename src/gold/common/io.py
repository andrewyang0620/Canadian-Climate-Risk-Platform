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
