with source as (
    select *
    from {{ source('core', 'grid_month_disaster_event_label') }}
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