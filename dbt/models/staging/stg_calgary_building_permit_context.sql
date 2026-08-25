with source as (
    select *
    from {{ source('core', 'calgary_building_permit_context') }}
)
select *
from source