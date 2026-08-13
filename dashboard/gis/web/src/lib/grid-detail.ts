import {
  getNumberValue,
  getStringValue,
  loadGridMetadata,
  loadMonthlyDataset,
} from "./gis-data";

export interface GridDetailData {
  identity: {
    gridCellKey: string;
    provinceCode: string | null;
    provinceName: string | null;
    municipality: string | null;
    gridSystem: string | null;
    areaSqKm: number | null;
    boundaryCoverage: number | null;
  };
  risk: {
    composite: number | null;
    confidence: number | null;
    percentile: number | null;
    tier: string | null;
    climate: number | null;
    hydro: number | null;
    wildfire: number | null;
  };
  climate: {
    meanTemp: number | null;
    minTemp: number | null;
    maxTemp: number | null;
    precipitation: number | null;
    extremeHeatDays: number | null;
    heavyPrecipDays: number | null;
    extremeColdDays: number | null;
    freezeThawDays: number | null;
  };
  hydro: {
    meanFlow: number | null;
    p95Flow: number | null;
    meanLevel: number | null;
    p95Level: number | null;
  };
  wildfire: {
    perimeterCount: number | null;
    intersectionAreaSqKm: number | null;
    overlapRatio: number | null;
    maxSourceSizeHa: number | null;
  };
  quality: {
    climateStationCount: number | null;
    climateMappingMethod: string | null;
    nearestClimateStationKm: number | null;
    climateIdwConfidence: number | null;
    temperatureCompleteness: number | null;
    precipitationCompleteness: number | null;
    hydroStationCount: number | null;
    hydroAssignmentMethod: string | null;
    hydroBasinCoverage: number | null;
    flowCompleteness: number | null;
    levelCompleteness: number | null;
    domainCoverageCount: number | null;
    domainCoverageRatio: number | null;
  };
}

export async function loadGridDetail(
  gridCellKey: string,
  referenceMonth: string,
): Promise<GridDetailData> {
  const [metadata, monthly] = await Promise.all([
    loadGridMetadata(),
    loadMonthlyDataset(referenceMonth),
  ]);

  const metaNumber = (column: string) =>
    getNumberValue(metadata, gridCellKey, column);
  const metaString = (column: string) =>
    getStringValue(metadata, gridCellKey, column);
  const monthlyNumber = (column: string) =>
    getNumberValue(monthly, gridCellKey, column);
  const monthlyString = (column: string) =>
    getStringValue(monthly, gridCellKey, column);

  return {
    identity: {
      gridCellKey,
      provinceCode: metaString("province_code"),
      provinceName: metaString("province_name"),
      municipality: metaString("primary_municipality_name"),
      gridSystem: metaString("grid_system"),
      areaSqKm: metaNumber("analysis_area_sq_km"),
      boundaryCoverage: metaNumber("boundary_coverage_ratio"),
    },
    risk: {
      composite: monthlyNumber("composite_risk_score"),
      confidence: monthlyNumber("score_confidence"),
      percentile: monthlyNumber("priority_percentile"),
      tier: monthlyString("priority_tier"),
      climate: monthlyNumber("climate_sub_score"),
      hydro: monthlyNumber("hydro_sub_score"),
      wildfire: monthlyNumber("wildfire_sub_score"),
    },
    climate: {
      meanTemp: monthlyNumber("climate_mean_temp_c"),
      minTemp: monthlyNumber("climate_min_temp_c"),
      maxTemp: monthlyNumber("climate_max_temp_c"),
      precipitation: monthlyNumber("climate_total_precip_mm"),
      extremeHeatDays: monthlyNumber("climate_extreme_heat_days"),
      heavyPrecipDays: monthlyNumber("climate_heavy_precipitation_days"),
      extremeColdDays: monthlyNumber("climate_extreme_cold_days"),
      freezeThawDays: monthlyNumber("climate_freeze_thaw_days"),
    },
    hydro: {
      meanFlow: monthlyNumber("flow_mean_measurement_value"),
      p95Flow: monthlyNumber("flow_p95_measurement_value"),
      meanLevel: monthlyNumber("level_mean_measurement_value"),
      p95Level: monthlyNumber("level_p95_measurement_value"),
    },
    wildfire: {
      perimeterCount: monthlyNumber("wildfire_perimeter_count"),
      intersectionAreaSqKm: monthlyNumber("wildfire_intersection_area_sq_km"),
      overlapRatio: monthlyNumber("wildfire_intersection_area_ratio_of_grid"),
      maxSourceSizeHa: monthlyNumber("wildfire_max_source_size_ha"),
    },
    quality: {
      climateStationCount: monthlyNumber("climate_station_count"),
      climateMappingMethod: monthlyString("climate_mapping_method"),
      nearestClimateStationKm: monthlyNumber(
        "climate_nearest_station_distance_km",
      ),
      climateIdwConfidence: monthlyNumber("climate_idw_confidence_score"),
      temperatureCompleteness: monthlyNumber(
        "climate_temperature_completeness_ratio",
      ),
      precipitationCompleteness: monthlyNumber(
        "climate_precipitation_completeness_ratio",
      ),
      hydroStationCount: monthlyNumber("hydro_station_count"),
      hydroAssignmentMethod: monthlyString("hydro_spatial_assignment_method"),
      hydroBasinCoverage: monthlyNumber("hydro_basin_grid_coverage_ratio"),
      flowCompleteness: monthlyNumber("flow_measurement_completeness_ratio"),
      levelCompleteness: monthlyNumber("level_measurement_completeness_ratio"),
      domainCoverageCount: monthlyNumber("domain_coverage_count"),
      domainCoverageRatio: monthlyNumber("domain_coverage_ratio"),
    },
  };
}
