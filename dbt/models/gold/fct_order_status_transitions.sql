{{
    config(
        partition_by={
            "field": "customer_id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 140001,
                "interval": 10000
            }
        },
        cluster_by=[
            "status_started_at",
            "new_order_status",
            "transition_type"
        ]
    )
}}


select
    cdc_event_id as order_status_transition_id,

    order_id,
    customer_id,

    date(order_date) as order_date,
    order_date as order_placed_at,

    previous_status as previous_order_status,
    order_status as new_order_status,
    transition_sequence,
    transition_type,

    previous_status_started_at,
    status_started_at,
    seconds_in_previous_status,

    total_amount as order_amount,

    created_at as order_created_at,
    updated_at as order_updated_at,

    cdc_operation_code,
    cdc_operation_name,
    source_lsn,
    flink_ingested_at,
    processing_latency_ms

from {{ ref('order_status_history') }}
