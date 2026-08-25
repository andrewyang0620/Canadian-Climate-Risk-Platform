with source as (
    select *
    from {{ source('core', 'vancouver_building_permit_context') }}
)
select *
from source