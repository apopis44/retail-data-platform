with ranked_order_item_events as (

    select
        *,
        row_number() over (
            partition by order_item_id
            order by
                source_event_at desc,
                source_lsn desc,
                kafka_offset desc
        ) as event_rank

    from {{ ref('stg_order_items_cdc') }}

)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    created_at,
    updated_at,

    cdc_event_id as latest_cdc_event_id,
    cdc_operation as latest_cdc_operation_code,
    cdc_operation_name as latest_cdc_operation_name,
    source_lsn as latest_source_lsn,
    source_event_at as last_changed_at,
    flink_ingested_at

from ranked_order_item_events

where event_rank = 1
    and not is_deleted