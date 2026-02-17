-- Mart: join orders + customers + products into one enriched fact table
-- This model is also exported as a Parquet file via the post-hook
{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
        -- Order fields
        o.order_id,
        o.order_date,
        o.status,
        o.quantity,

        -- Customer fields
        c.customer_id,
        c.customer_name,
        c.email,
        c.country,
        c.signup_date,

        -- Product fields
        p.product_id,
        p.product_name,
        p.category,
        p.unit_price,

        -- Computed metrics
        (o.quantity * p.unit_price)::decimal(10,2)          as line_total,
        date_diff('day', c.signup_date, o.order_date)       as days_since_signup,
        extract('month' from o.order_date)::integer         as order_month,
        extract('year' from o.order_date)::integer          as order_year

    from orders o
    inner join customers c on o.customer_id = c.customer_id
    inner join products  p on o.product_id  = p.product_id
)

select * from enriched
order by order_id
