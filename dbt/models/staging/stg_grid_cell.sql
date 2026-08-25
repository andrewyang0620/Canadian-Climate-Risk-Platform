with source as (
    select * from {{ source('core', 'grid_cell') }}
)
select *
from source