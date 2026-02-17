-- Export the enriched orders table to Parquet using dbt-duckdb external materialization
{{
    config(
        materialized='external',
        location='{{ var("output_path") }}/orders_enriched.parquet',
        format='parquet',
        options={
            'compression': 'snappy'
        }
    )
}}

select * from {{ ref('orders_enriched') }}
