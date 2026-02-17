-- Reporting: Per-customer lifetime metrics
-- Total orders, total spend, avg order value, first/last order date

with enriched as (
    select * from {{ ref('orders_enriched') }}
)

select
    customer_id,
    customer_name,
    email,
    country,
    count(distinct order_id)                                as total_orders,
    sum(quantity)::integer                                  as total_units,
    sum(line_total)::decimal(12, 2)                         as total_spend,
    (sum(line_total) / count(distinct order_id))::decimal(10, 2) as avg_order_value,
    min(order_date)                                         as first_order_date,
    max(order_date)                                         as last_order_date,
    date_diff('day', min(order_date), max(order_date))      as days_as_customer
from enriched
group by customer_id, customer_name, email, country
order by total_spend desc
