import gzip
import json
from pathlib import Path

from src.silver.eccc_hydro_realtime_observation import (
    build_eccc_hydro_realtime_observation_silver,
)


def write_jsonl_gz(path: Path, rows: list[dict]):
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def feature(station_id, observed_at, level, discharge):
    return {
        "type": "Feature",
        "id": f"{station_id}.{observed_at}",
        "geometry": {
            "type": "Point",
            "coordinates": [-113.23631, 50.76404],
        },
        "properties": {
            "IDENTIFIER": f"{station_id}.{observed_at}",
            "STATION_NUMBER": station_id,
            "STATION_NAME": "TEST STATION",
            "PROV_TERR_STATE_LOC": "AB",
            "DATETIME": observed_at,
            "DATETIME_LST": "2026-06-12T00:00:00-07:00",
            "LEVEL": level,
            "DISCHARGE": discharge,
            "LEVEL_SYMBOL_EN": None,
            "LEVEL_SYMBOL_FR": None,
            "DISCHARGE_SYMBOL_EN": None,
            "DISCHARGE_SYMBOL_FR": None,
        },
    }


def test_build_eccc_hydro_realtime_observation_silver(tmp_path):
    raw_path = tmp_path / "raw.jsonl.gz"

    write_jsonl_gz(
        raw_path,
        [
            feature("05BM014", "2026-06-12T07:00:00Z", 0.268, 0.004),
            feature("05BM014", "2026-06-12T07:05:00Z", -0.1, -0.2),
        ],
    )

    result = build_eccc_hydro_realtime_observation_silver(raw_path)

    assert len(result) == 2
    assert result["source_name"].unique().tolist() == ["eccc_hydrometric_realtime"]
    assert result["station_id"].tolist() == ["05BM014", "05BM014"]
    assert result["province_code"].tolist() == ["AB", "AB"]
    assert result["latitude"].tolist() == [50.76404, 50.76404]
    assert result["longitude"].tolist() == [-113.23631, -113.23631]
    assert result["geometry_wkt"].tolist() == [
        "POINT (-113.23631 50.76404)",
        "POINT (-113.23631 50.76404)",
    ]

    assert result.loc[0, "water_level_m"] == 0.268
    assert result.loc[0, "discharge_cms"] == 0.004

    assert result.loc[1, "water_level_m"] == -0.1
    assert result.loc[1, "raw_discharge_cms"] == -0.2
    assert result.loc[1, "discharge_cms"] != result.loc[1, "discharge_cms"]
    assert bool(result.loc[1, "negative_discharge_flag"]) is True

    assert result["hydro_realtime_observation_key"].isna().sum() == 0
    assert result["hydro_realtime_observation_key"].duplicated().sum() == 0
    assert result["source_record_count"].tolist() == [1, 1]
