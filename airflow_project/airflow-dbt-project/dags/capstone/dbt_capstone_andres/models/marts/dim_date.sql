{{
    config(
        materialized = "table"
    )
}}
{{ dbt_date.get_date_dimension('1995-01-01', '2040-12-31') }}