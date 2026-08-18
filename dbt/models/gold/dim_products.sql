with products as (

    select
        product_id,
        product_name,
        category,
        unit_price,
        created_at,
        updated_at,
        last_changed_at,
        flink_ingested_at

    from {{ ref('products_current') }}

)

select
    product_id,

    product_name,
    upper(trim(category)) as product_category,
    unit_price as current_unit_price,

    date(created_at) as product_created_date,
    created_at as product_created_at,
    updated_at as product_updated_at,

    last_changed_at as source_last_changed_at,
    flink_ingested_at

from products