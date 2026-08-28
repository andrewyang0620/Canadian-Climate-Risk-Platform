with panel as (
    select *
    from {{ ref('int_grid_month_risk_panel') }}
)
select 
    risk_score_key,  -- keys/grain
    grid_month_risk_feature_key,
    grid_month_disaster_label_key,
    grid_cell_key,
    reference_month,
    year(reference_month) as reference_year,  -- time
    month(reference_month) as reference_month_number,
    grid_system,  -- grid
    province_key,
    province_code,
    province_name,
    primary_municipality_name,
    boundary_coverage_ratio,
    composite_risk_score,  -- composite risk
    score_confidence,
    priority_percentile,
    priority_tier,
    climate_sub_score,  -- domain risk
    hydro_sub_score,
    wildfire_sub_score,
    domain_coverage_count,  -- scoring coverage
    domain_coverage_ratio,
    composite_score_eligible,
    ranking_eligible,
    ranking_exclusion_reason,
    climate_effective_quality,  -- scoring quality
    hydro_effective_quality,
    wildfire_effective_quality,
    climate_mean_temp_c,  -- climate physical conditions
    climate_min_temp_c,
    climate_max_temp_c,
    climate_total_precip_mm,
    climate_heavy_precipitation_days,
    climate_extreme_heat_days,
    climate_extreme_cold_days,
    climate_freeze_thaw_days,
    climate_station_count,  -- climate source quality
    climate_mapping_method,
    climate_nearest_station_distance_km,
    climate_idw_confidence_score,
    climate_temperature_completeness_ratio,
    climate_precipitation_completeness_ratio,
    climate_feature_quality_flag,
    flow_mean_measurement_value,  -- hydro physical conditions
    flow_p95_measurement_value,
    level_mean_measurement_value,
    level_p95_measurement_value,
    hydro_station_count,  -- hydro source quality
    hydro_spatial_assignment_method,
    hydro_basin_grid_coverage_ratio,
    flow_measurement_completeness_ratio,
    level_measurement_completeness_ratio,
    hydro_feature_quality_flag,
    wildfire_perimeter_count,  -- wildfire physical conditions
    wildfire_intersection_area_sq_km,
    wildfire_intersection_area_ratio_of_grid,
    wildfire_max_source_size_ha,
    wildfire_has_observed_perimeter_overlap,
    wildfire_temporal_assignment_method,
    grid_month_disaster_label_key is not null as has_disaster_label_record,
    label_is_observed,
    disaster_event_occurred,
    disaster_event_count,
    wildfire_event_count,
    flood_event_count,
    storm_or_climate_event_count,
    climate_extreme_event_count
from panel