-- Reporting: Product-level performance with stock remaining
-- Joins orders_enriched (for sales data) with stg_products (for stock_qty)

with enriched as (
    select * from {{ ref('orders_enriched') }}
),

products as (
    select
        product_id,
        stock_qty
    from {{ ref('stg_products') }}
),

sales as (
    select
        product_id,
        product_name,
        category,
        unit_price,
        count(distinct order_id)                                as num_orders,
        sum(quantity)::integer                                  as units_sold,
        sum(line_total)::decimal(12, 2)                         as total_revenue,
        (sum(line_total) / count(distinct order_id))::decimal(10, 2) as avg_order_value
    from enriched
    group by product_id, product_name, category, unit_price
)

select
    s.product_id,
    s.product_name,
    s.category,
    s.unit_price,
    s.num_orders,
    s.units_sold,
    s.total_revenue,
    s.avg_order_value,
    p.stock_qty                                             as stock_remaining,
    (p.stock_qty + s.units_sold)::integer                   as original_stock_estimate
from sales s
inner join products p on s.product_id = p.product_id
order by s.total_revenue desc
