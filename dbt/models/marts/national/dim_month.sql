with months as (
    select distinct reference_month
    from {{ ref('int_grid_month_risk_panel') }}
),
final as (
    select 
        reference_month as month_key,
        year(reference_month) as year,
        month(reference_month) as month_number,
        quarter(reference_month) as quarter_number,
        to_char(reference_month, 'YYYY-MM') as year_month,
        to_char(reference_month, 'MON') as month_short_name,
        to_char(reference_month, 'MMMM') as month_name,
        date_trunc('quarter', reference_month) as quarter_start_date,
        date_trunc('year', reference_month) as year_start_date
    from months
)
select *
from final