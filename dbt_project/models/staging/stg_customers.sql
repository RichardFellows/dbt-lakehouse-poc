-- Staging: read customers Parquet via DuckDB's read_parquet (fallback: read_csv_auto)
with source as (
    select * from read_parquet('{{ var("parquet_path") }}/customers.parquet')
),

renamed as (
    select
        customer_id::integer       as customer_id,
        customer_name,
        lower(email)               as email,
        upper(country)             as country,
        signup_date::date          as signup_date
    from source
)

select * from renamed
