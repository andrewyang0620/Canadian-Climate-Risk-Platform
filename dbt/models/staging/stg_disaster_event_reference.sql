with source as (
    select *
    from {{ source('core', 'disaster_event_reference') }}
)
select *
from source