-- Staging: read products Parquet via DuckDB's read_parquet (fallback: read_csv_auto)
with source as (
    select * from read_parquet('{{ var("parquet_path") }}/products.parquet')
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
