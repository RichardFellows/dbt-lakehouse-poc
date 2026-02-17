-- Reporting: Product category performance
-- Total revenue, units sold, avg order value per category

with enriched as (
    select * from {{ ref('orders_enriched') }}
)

select
    category,
    count(distinct order_id)                                as num_orders,
    sum(quantity)::integer                                  as units_sold,
    sum(line_total)::decimal(12, 2)                         as total_revenue,
    (sum(line_total) / count(distinct order_id))::decimal(10, 2) as avg_order_value,
    avg(unit_price)::decimal(10, 2)                         as avg_unit_price,
    count(distinct product_id)                              as num_products
from enriched
group by category
order by total_revenue desc
