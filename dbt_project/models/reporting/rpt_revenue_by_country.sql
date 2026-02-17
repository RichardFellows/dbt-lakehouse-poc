-- Reporting: Revenue rollups by country and period (month/year)
-- Built on top of the orders_enriched mart

with enriched as (
    select * from {{ ref('orders_enriched') }}
),

by_country_month as (
    select
        country,
        order_year                                          as year,
        order_month                                         as month,
        count(distinct order_id)                            as num_orders,
        sum(quantity)                                       as units_sold,
        sum(line_total)::decimal(12, 2)                     as total_revenue,
        (sum(line_total) / count(distinct order_id))::decimal(10, 2) as avg_order_value
    from enriched
    group by 1, 2, 3
)

select
    country,
    year,
    month,
    num_orders,
    units_sold,
    total_revenue,
    avg_order_value
from by_country_month
order by year, month, country
