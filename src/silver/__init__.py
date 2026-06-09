from src.silver.canadian_disaster_database import (
    run_canadian_disaster_database_silver,
)
from src.silver.census_boundaries import run_census_boundary_silver
from src.silver.eccc_climate_daily import run_eccc_climate_daily_silver
from src.silver.hydat_archive import run_hydat_archive_silver
from src.silver.municipal_flood_hazard import run_municipal_flood_hazard_silver
from src.silver.municipal_building_permit import (
    run_municipal_building_permit_silver,
)
from src.silver.municipal_property_assessment import (
    run_municipal_property_assessment_silver,
)
from src.silver.validation import (
    validate_canadian_disaster_database_silver_outputs,
    validate_census_boundary_silver_outputs,
    validate_eccc_climate_daily_silver_outputs,
    validate_hydat_archive_silver_outputs,
    validate_municipal_building_permit_silver_outputs,
    validate_municipal_flood_hazard_silver_outputs,
    validate_municipal_property_assessment_silver_outputs,
    validate_municipal_property_parcel_silver_outputs,
    validate_wildfire_history_silver_outputs,
)
from src.silver.wildfire_history import run_wildfire_history_silver

__all__ = [
    "run_canadian_disaster_database_silver",
    "run_census_boundary_silver",
    "run_eccc_climate_daily_silver",
    "run_hydat_archive_silver",
    "run_municipal_building_permit_silver",
    "run_municipal_flood_hazard_silver",
    "run_municipal_property_assessment_silver",
    "run_wildfire_history_silver",
    "validate_canadian_disaster_database_silver_outputs",
    "validate_census_boundary_silver_outputs",
    "validate_eccc_climate_daily_silver_outputs",
    "validate_hydat_archive_silver_outputs",
    "validate_municipal_building_permit_silver_outputs",
    "validate_municipal_flood_hazard_silver_outputs",
    "validate_municipal_property_assessment_silver_outputs",
    "validate_municipal_property_parcel_silver_outputs",
    "validate_wildfire_history_silver_outputs",
]
from src.silver.municipal_property_parcel import build_vancouver_property_parcel_silver

__all__ = sorted(
    set(globals().get("__all__", []))
    | {
        "build_vancouver_property_parcel_silver",
    }
)
from src.silver.municipal_property_tax_assessment import (
    build_vancouver_property_tax_assessment_silver,
)

__all__ = sorted(
    set(globals().get("__all__", []))
    | {
        "build_vancouver_property_tax_assessment_silver",
    }
)
from src.silver.validation import (
    validate_municipal_property_tax_assessment_silver_outputs,
)

__all__ = sorted(
    set(globals().get("__all__", []))
    | {
        "validate_municipal_property_tax_assessment_silver_outputs",
    }
)
