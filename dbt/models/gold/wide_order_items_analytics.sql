{{
    config(
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=[
            "order_id",
            "customer_id",
            "product_id",
            "order_status"
        ]
    )
}}

with orders as (

    select
        order_id,
        customer_id,
        order_date,
        order_placed_at,
        order_status,
        order_amount,
        is_cancelled,
        is_delivered,
        order_created_at,
        order_updated_at,
        flink_ingested_at

    from {{ ref('fct_orders') }}

),

customers as (

    select
        customer_id,
        full_name,
        email,
        country_name,
        customer_created_at,
        customer_updated_at

    from {{ ref('dim_customers') }}

),

order_items as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        order_item_created_at,
        order_item_updated_at,
        flink_ingested_at

    from {{ ref('fct_order_items') }}

),

products as (

    select
        product_id,
        product_name,
        product_category,
        product_created_at,
        product_updated_at

    from {{ ref('dim_products') }}

)

select
    orders.order_id,
    orders.customer_id,

    customers.full_name as customer_full_name,
    customers.email,
    customers.country_name,
    customers.customer_created_at,
    customers.customer_updated_at,

    orders.order_date,
    time(orders.order_placed_at) as order_time,
    orders.order_placed_at,
    orders.order_status,
    orders.order_amount as total_amount,
    orders.is_cancelled,
    orders.is_delivered,
    orders.order_created_at,
    orders.order_updated_at,
    orders.flink_ingested_at as order_flink_ingested_at,

    order_items.order_item_id,
    order_items.quantity,
    order_items.unit_price as product_unit_price,
    order_items.line_amount,
    order_items.order_item_created_at as line_item_created_at,
    order_items.order_item_updated_at as line_item_updated_at,
    order_items.flink_ingested_at as line_item_flink_ingested_at,

    products.product_id,
    products.product_name,
    products.product_category,
    products.product_created_at,
    products.product_updated_at

from order_items

inner join orders
    on order_items.order_id = orders.order_id

inner join customers
    on orders.customer_id = customers.customer_id

inner join products
    on order_items.product_id = products.product_id