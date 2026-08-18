{{
    config(
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=[
            "customer_id",
            "product_id",
            "order_status"
        ]
    )
}}

with order_items as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        created_at,
        updated_at,
        last_changed_at,
        flink_ingested_at

    from {{ ref('order_items_current') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date,
        status

    from {{ ref('orders_current') }}

)

select
    order_items.order_item_id,
    order_items.order_id,
    orders.customer_id,
    order_items.product_id,

    date(orders.order_date) as order_date,
    orders.order_date as order_placed_at,
    orders.status as order_status,

    order_items.quantity,
    order_items.unit_price,
    order_items.line_amount,

    order_items.created_at as order_item_created_at,
    order_items.updated_at as order_item_updated_at,
    order_items.last_changed_at as source_last_changed_at,
    order_items.flink_ingested_at

from order_items

inner join orders
    on order_items.order_id = orders.order_id
