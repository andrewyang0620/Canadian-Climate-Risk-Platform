from src.silver.municipal_flood_hazard import (
    deduplicate_flood_zones,
    first_non_empty,
    geometry_to_wkt,
)


def test_geometry_to_wkt_polygon():
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-123.0, 49.0],
                [-123.1, 49.0],
                [-123.1, 49.1],
                [-123.0, 49.0],
            ]
        ],
    }

    assert geometry_to_wkt(geometry).startswith("POLYGON")


def test_geometry_to_wkt_multipolygon():
    geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [-114.0, 51.0],
                    [-114.1, 51.0],
                    [-114.1, 51.1],
                    [-114.0, 51.0],
                ]
            ]
        ],
    }

    assert geometry_to_wkt(geometry).startswith("MULTIPOLYGON")


def test_first_non_empty_uses_normalized_names():
    properties = {
        "Flood CD": "100",
        "Description": "Flood Fringe",
    }

    assert first_non_empty(properties, ["flood_cd"]) == "100"
    assert first_non_empty(properties, ["description"]) == "Flood Fringe"


def test_deduplicate_flood_zones_keeps_first_record():
    import pandas as pd

    dataframe = pd.DataFrame(
        [
            {
                "flood_hazard_zone_key": "calgary_1",
                "source_feature_index": 2,
                "geometry_wkt": "POLYGON ((0 0, 1 0, 0 0))",
            },
            {
                "flood_hazard_zone_key": "calgary_1",
                "source_feature_index": 1,
                "geometry_wkt": "POLYGON ((0 0, 2 0, 0 0))",
            },
        ]
    )

    result = deduplicate_flood_zones(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["source_feature_index"] == 1
    assert result.iloc[0]["source_record_count"] == 2
