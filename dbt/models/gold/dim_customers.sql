with customers as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        country_name,
        created_at,
        updated_at,
        last_changed_at,
        flink_ingested_at

    from {{ ref('customers_current') }}

)

select
    customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    email,
    country_name,

    date(created_at) as customer_since_date,
    created_at as customer_created_at,
    updated_at as customer_updated_at,

    last_changed_at as source_last_changed_at,
    flink_ingested_at

from customers
