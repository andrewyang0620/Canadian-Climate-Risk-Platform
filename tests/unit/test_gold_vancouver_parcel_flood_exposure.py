import pandas as pd
import pytest

from src.gold.city.vancouver_parcel_flood_exposure import (
    VancouverParcelFloodExposureError,
    build_gold_vancouver_parcel_flood_exposure,
)


def _parcel(key: str) -> dict:
    return {
        "property_parcel_key": key,
        "city": "vancouver",
        "province": "BC",
        "source_name": "vancouver_property_parcels",
        "source_parcel_id": f"source_{key}",
        "source_tax_coord": f"tax_{key}",
        "address_text": f"{key} Example Street",
        "geometry_wkt": (
            "POLYGON (("
            "-123.12 49.25, "
            "-123.11 49.25, "
            "-123.11 49.26, "
            "-123.12 49.26, "
            "-123.12 49.25"
            "))"
        ),
    }


def _scenario(
    *,
    parcel_key: str,
    scenario_name: str,
    source_zone_count: int = 1,
    area: float = 40.0,
    ratio: float = 0.4,
) -> dict:
    return {
        "property_parcel_key": parcel_key,
        "scenario_name": scenario_name,
        "source_zone_count": source_zone_count,
        "scenario_intersection_area_sq_m": area,
        "scenario_overlap_ratio": ratio,
    }


def test_build_retains_full_parcel_universe():
    parcels = pd.DataFrame(
        [
            _parcel("parcel_1"),
            _parcel("parcel_2"),
        ]
    )

    scenarios = pd.DataFrame(
        [
            _scenario(
                parcel_key="parcel_1",
                scenario_name=(
                    "Designated Floodplain (FCL 4.6m)"
                ),
            )
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_flood_exposure(
            parcel_dataframe=parcels,
            scenario_dataframe=scenarios,
        )
    )

    assert len(result) == 2

    exposed = result[
        result["property_parcel_key"]
        == "parcel_1"
    ].iloc[0]

    unexposed = result[
        result["property_parcel_key"]
        == "parcel_2"
    ].iloc[0]

    assert bool(exposed["is_flood_exposed"])
    assert exposed["scenario_count"] == 1

    assert not bool(
        unexposed["is_flood_exposed"]
    )
    assert unexposed["scenario_count"] == 0
    assert (
        unexposed[
            "designated_floodplain_overlap_ratio"
        ]
        == 0
    )

    assert summary["output_row_count"] == 2
    assert summary["exposed_parcel_count"] == 1
    assert summary["unexposed_parcel_count"] == 1


def test_multiple_scenarios_are_kept_separate():
    parcels = pd.DataFrame(
        [
            _parcel("parcel_1"),
        ]
    )

    scenarios = pd.DataFrame(
        [
            _scenario(
                parcel_key="parcel_1",
                scenario_name=(
                    "Designated Floodplain (FCL 4.6m)"
                ),
                area=80.0,
                ratio=0.8,
            ),
            _scenario(
                parcel_key="parcel_1",
                scenario_name="Wave Effect Zone",
                area=20.0,
                ratio=0.2,
            ),
        ]
    )

    result, _ = (
        build_gold_vancouver_parcel_flood_exposure(
            parcel_dataframe=parcels,
            scenario_dataframe=scenarios,
        )
    )

    row = result.iloc[0]

    assert row["scenario_count"] == 2

    assert bool(
        row["designated_floodplain_flag"]
    )
    assert bool(
        row["wave_effect_zone_flag"]
    )

    assert (
        row[
            "designated_floodplain_overlap_ratio"
        ]
        == pytest.approx(0.8)
    )

    assert (
        row["wave_effect_zone_overlap_ratio"]
        == pytest.approx(0.2)
    )


def test_source_zone_counts_are_preserved():
    parcels = pd.DataFrame(
        [
            _parcel("parcel_1"),
        ]
    )

    scenarios = pd.DataFrame(
        [
            _scenario(
                parcel_key="parcel_1",
                scenario_name=(
                    "Fraser Risk Today 1/500 Storm"
                ),
                source_zone_count=2,
            ),
            _scenario(
                parcel_key="parcel_1",
                scenario_name=(
                    "Still Creek Floodplain"
                ),
                source_zone_count=1,
            ),
        ]
    )

    result, _ = (
        build_gold_vancouver_parcel_flood_exposure(
            parcel_dataframe=parcels,
            scenario_dataframe=scenarios,
        )
    )

    row = result.iloc[0]

    assert row["scenario_count"] == 2
    assert row["source_zone_count"] == 3

    assert (
        row["fraser_risk_today_source_zone_count"]
        == 2
    )


def test_unknown_scenario_is_rejected():
    parcels = pd.DataFrame(
        [
            _parcel("parcel_1"),
        ]
    )

    scenarios = pd.DataFrame(
        [
            _scenario(
                parcel_key="parcel_1",
                scenario_name="Unknown Scenario",
            )
        ]
    )

    with pytest.raises(
        VancouverParcelFloodExposureError,
        match="Unexpected Vancouver flood scenarios",
    ):
        build_gold_vancouver_parcel_flood_exposure(
            parcel_dataframe=parcels,
            scenario_dataframe=scenarios,
        )


def test_orphan_scenario_parcel_is_rejected():
    parcels = pd.DataFrame(
        [
            _parcel("parcel_1"),
        ]
    )

    scenarios = pd.DataFrame(
        [
            _scenario(
                parcel_key="parcel_missing",
                scenario_name=(
                    "Still Creek Floodplain"
                ),
            )
        ]
    )

    with pytest.raises(
        VancouverParcelFloodExposureError,
        match="not present in silver_property_parcel",
    ):
        build_gold_vancouver_parcel_flood_exposure(
            parcel_dataframe=parcels,
            scenario_dataframe=scenarios,
        )