-- Staging: read orders CSV via DuckDB's read_csv_auto
with source as (
    select * from read_csv_auto('{{ var("csv_path") }}/orders.csv', header=true)
),

renamed as (
    select
        order_id::integer     as order_id,
        customer_id::integer  as customer_id,
        product_id::integer   as product_id,
        quantity::integer     as quantity,
        order_date::date      as order_date,
        lower(status)         as status
    from source
)

select * from renamed
