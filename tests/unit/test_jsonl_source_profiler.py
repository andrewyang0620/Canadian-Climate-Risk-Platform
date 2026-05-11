import gzip
import json

from src.profiling.source_profiler import profile_raw_file


def test_profile_jsonl_gzip_geojson_features(tmp_path):
    path = tmp_path / "climate.jsonl.gz"

    rows = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-123.1, 49.2]},
            "properties": {
                "STATION_ID": "123",
                "LOCAL_DATE": "2024-01-01",
                "MAX_TEMPERATURE": 10.5,
                "TOTAL_PRECIPITATION": 4.2,
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-114.1, 51.0]},
            "properties": {
                "STATION_ID": "456",
                "LOCAL_DATE": "2024-01-01",
                "MIN_TEMPERATURE": -5.0,
                "TOTAL_RAIN": 0.0,
            },
        },
    ]

    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")

    profile = profile_raw_file(path, count_rows=True)

    assert profile["file_type"] == "jsonl_gzip"
    assert profile["row_count_exact"] == 2
    assert profile["geometry_types"] == {"Point": 2}
    assert "properties.STATION_ID" in profile["columns"]
    assert "properties.LOCAL_DATE" in profile["columns"]
    assert "properties.MAX_TEMPERATURE" in profile["columns"]
