{{
    config(
        materialized='table'
    )
}}

with date_spine as (

    {{
        dbt.date_spine(
            'day',
            "date_sub(current_date(),interval 4 year)",
            "date_add(current_date(), interval 31 day)"
        )
    }}
)

select
    cast(date_day as date) as date_day

from date_spine