from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4
import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.scoring.builder import (
    SCORE_TABLE_NAME,
    build_gold_grid_month_risk_score,
)

GOLD_ROOT = Path("lakehouse/gold")
INPUT_TABLE_NAME = "gold_grid_month_risk_feature_mart"

def run_gold_grid_month_risk_score(
    *,
    gold_root: str | Path = GOLD_ROOT,
    extract_date: str | None = None,
    run_id: str | None = None,
) -> dict:
    final_extract_date = (
        extract_date or date.today().isoformat()
    )
    final_run_id = run_id or str(uuid4())
    
    input_path = latest_table_parquet(
        root=gold_root,
        table_name=INPUT_TABLE_NAME,
    )
    
    risk_feature_mart = pd.read_parquet(input_path)
    
    score, summary = build_gold_grid_month_risk_score(risk_feature_mart)
    
    output_path = (
        Path(gold_root)
        / SCORE_TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={final_run_id}"
        / f"{SCORE_TABLE_NAME}.parquet"
    )
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    score.to_parquet(output_path, index=False)
    metadata = {
        "table_name": SCORE_TABLE_NAME,
        "run_id": final_run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        **summary,
        "input_table": INPUT_TABLE_NAME,
        "input_path": input_path.as_posix(),
        "output_path": output_path.as_posix(),
    }


    metadata_path = (
        Path(gold_root)
        / "_metadata"
        / SCORE_TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={final_run_id}"
        / "metadata.json"
    )
    
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8"
    )
    
    metadata["metadata_path"] = metadata_path.as_posix()
    
    return metadata

def main() -> None:
    metadata = run_gold_grid_month_risk_score()
    print(
        "[OK] wrote Gold grid-month risk score | "
        f"rows={metadata['row_count']} "
        f"grid_cells={metadata['grid_cell_count']} "
        f"months={metadata['month_count']} "
        f"composite_eligible="
        f"{metadata['composite_score_eligible_count']} "
        f"ranking_eligible="
        f"{metadata['ranking_eligible_count']} "
        f"run_id={metadata['run_id']}"
    )
    print(json.dumps(metadata, indent=2))
    
if __name__ == "__main__":
    main()