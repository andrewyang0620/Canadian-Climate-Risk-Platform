with source as(
    select * from {{ source('core', 'grid_municipality_bridge') }}
)
select * 
from source