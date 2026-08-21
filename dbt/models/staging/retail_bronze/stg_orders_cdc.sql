select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at,
    updated_at,

    cdc_event_id,
    operation as cdc_operation,

    case operation
        when 'r' then 'snapshot'
        when 'c' then 'insert'
        when 'u' then 'update'
        when 'd' then 'delete'
        else 'unknown'
    end as cdc_operation_name,

    operation = 'd' as is_deleted,
    source_snapshot,
    source_lsn,
    source_event_at,

    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,

    flink_ingested_at,
    processing_latency_ms

from {{ source('retail_bronze', 'orders_cdc') }}