with ranked_customer_events as (

    select
        *,
        row_number() over(
            partition by customer_id
            order by
                source_event_at desc,
                source_lsn desc,
                kafka_offset desc
        ) as event_rank

    from {{ ref('stg_customers_cdc') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    country_name,
    created_at,
    updated_at,

    cdc_event_id as latest_cdc_event_id,
    cdc_operation as latest_cdc_operation_code,
    cdc_operation_name as latest_cdc_operation_name,
    source_lsn as latest_source_lsn,
    source_event_at as last_changed_at,
    flink_ingested_at

from ranked_customer_events

where event_rank = 1
    and not is_deleted
