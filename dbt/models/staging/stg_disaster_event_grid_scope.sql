with source as (
    select *
    from {{ source('core', 'disaster_event_grid_scope') }}
)
select * 
from source