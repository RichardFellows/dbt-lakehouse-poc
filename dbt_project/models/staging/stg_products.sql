-- Staging: read products CSV via DuckDB's read_csv_auto
with source as (
    select * from read_csv_auto('{{ var("csv_path") }}/products.csv', header=true)
),

renamed as (
    select
        product_id::integer   as product_id,
        product_name,
        category,
        unit_price::decimal(10,2) as unit_price,
        stock_qty::integer    as stock_qty
    from source
)

select * from renamed
