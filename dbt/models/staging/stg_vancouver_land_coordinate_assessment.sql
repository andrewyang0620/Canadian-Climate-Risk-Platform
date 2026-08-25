with source as (
    select *
    from {{ source('core', 'vancouver_land_coordinate_assessment') }}
)
select *
from source