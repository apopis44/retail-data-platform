with ranked_order_events as(

    select
        *,
        row_number() over(
            partition by order_id
            order by
                source_event_at desc,
                source_lsn desc,
                kafka_offset desc
        ) as event_rank

    from {{ ref('stg_orders_cdc') }}

)

select
    order_id,
    customer_id,
    order_date,
    {{ normalize_order_status('status') }} as status,
    total_amount,
    created_at,
    updated_at,

    cdc_event_id as latest_cdc_event_id,
    cdc_operation as latest_cdc_operation_code,
    cdc_operation_name as latest_cdc_operation_name,
    source_lsn as latest_source_lsn,
    source_event_at as last_changed_at,
    flink_ingested_at

from ranked_order_events

where event_rank = 1
    and not is_deleted