with source as (
    select *
    from {{ source('core', 'calgary_development_permit_context') }}
)
select *
from source