with source as (
    select *
    from {{ source('core', 'calgary_property_risk_context') }}
)
select *
from source