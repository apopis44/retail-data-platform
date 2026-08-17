with normalized_order_events as(

    select
        order_id,
        customer_id,
        order_date,

        case
            when is_deleted then 'DELETED'
            else {{ normalize_order_status('status') }}
        end as order_status,

        total_amount,
        created_at,
        updated_at,

        cdc_event_id,
        cdc_operation,
        cdc_operation_name,
        is_deleted,
        source_lsn,
        source_event_at,
        kafka_offset,
        flink_ingested_at,
        processing_latency_ms
    
    from {{ ref('stg_orders_cdc') }}

),

ordered_events as (

    select
        *,

        lag(order_status) over (
            partition by order_id
            order by
                source_event_at,
                source_lsn,
                kafka_offset
        ) as previous_event_status

    from normalized_order_events

),

status_changes as (

    select *

    from ordered_events

    where previous_event_status is null
        or previous_event_status != order_status

),

status_timeline as (

    select
        *,

        lag(order_status) over (
            partition by order_id
            order by
                source_event_at,
                source_lsn,
                kafka_offset
        ) as previous_status,

        lag(source_event_at) over (
            partition by order_id
            order by
                source_event_at,
                source_lsn,
                kafka_offset
        ) as previous_status_started_at,

        row_number() over (
            partition by order_id
            order by
                source_event_at,
                source_lsn,
                kafka_offset
        ) as transition_sequence

    from status_changes

)

select
    order_id,
    customer_id,
    order_date,

    previous_status,
    order_status,
    transition_sequence,

    previous_status_started_at,

    source_event_at as status_started_at,

    timestamp_diff(
        source_event_at,
        previous_status_started_at,
        second
    ) as seconds_in_previous_status,

    case
        when previous_status is null then 'initial_state'
        when is_deleted then 'deletion'
        else 'status_change'
    end as transition_type,

    total_amount,
    created_at,
    updated_at,

    cdc_event_id,
    cdc_operation as cdc_operation_code,
    cdc_operation_name,
    source_lsn,
    flink_ingested_at,
    processing_latency_ms

from status_timeline