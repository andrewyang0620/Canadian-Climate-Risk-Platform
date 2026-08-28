with municipality as (
    select 
        municipality_key,
        municipality_name,
        municipality_type,
        municipality_province_code,
        municipality_province_name,
        municipality_boundary_year
    from {{ ref('bridge_grid_municipality') }}
),
final as (
    select distinct
        municipality_key,
        municipality_name,
        municipality_type,
        municipality_province_code,
        municipality_province_name,
        municipality_boundary_year
    from municipality
)
select *
from final