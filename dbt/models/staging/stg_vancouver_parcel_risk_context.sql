with source as (
    select *
    from {{ source('core', 'vancouver_parcel_risk_context') }}
)
select *
from source