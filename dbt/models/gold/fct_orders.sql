{{
    config(
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=[
            "customer_id",
            "order_status"
        ]
    )
}}

with orders as (

    select
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        created_at,
        updated_at,
        last_changed_at,
        flink_ingested_at

    from {{ ref('orders_current') }}

)

select
    order_id,
    customer_id,

    date(order_date) as order_date,
    order_date as order_placed_at,
    status as order_status,

    total_amount as order_amount,

    status = 'CANCELLED' as is_cancelled,
    status = 'DELIVERED' as is_delivered,

    created_at as order_created_at,
    updated_at as order_updated_at,

    last_changed_at as source_last_changed_at,
    flink_ingested_at

from orders
