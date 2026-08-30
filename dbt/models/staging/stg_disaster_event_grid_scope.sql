with source as (
    select *
    from {{ source('core', 'disaster_event_grid_scope') }}
),
typed as (
    select
        * replace (
            try_to_date(reference_month || '-01') as reference_month
        )
    from source
)
select *
from typed