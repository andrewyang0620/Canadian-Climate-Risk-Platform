with risk_feature as (
    select *
    from {{ ref('stg_grid_month_risk_feature') }}
),
risk_score as (
    select *
    from {{ ref('stg_grid_month_risk_score') }}
),
disaster_label as (
    select *
    from {{ ref('stg_grid_month_disaster_event_label') }}
),
final as (
    select 
        f.*,
        s.* exclude (
            grid_month_risk_feature_key,
            grid_cell_key,
            reference_month,
            grid_system,
            province_key,
            boundary_coverage_ratio
        ),
        l.* exclude (
            grid_cell_key,
            reference_month,
            province_key,
            grid_system
        )
    from risk_feature f
        join risk_score s
            on f.grid_month_risk_feature_key = s.grid_month_risk_feature_key
        left join disaster_label l
            on f.grid_cell_key = l.grid_cell_key and f.reference_month = l.reference_month
)
select *
from final