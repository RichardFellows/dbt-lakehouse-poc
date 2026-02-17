-- Staging: read customers CSV via DuckDB's read_csv_auto
with source as (
    select * from read_csv_auto('{{ var("csv_path") }}/customers.csv', header=true)
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
