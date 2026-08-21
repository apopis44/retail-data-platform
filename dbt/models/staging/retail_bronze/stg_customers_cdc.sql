select
    customer_id,
    first_name,
    last_name,
    email,

    case upper(trim(country))
        when 'US' then 'United States'
        when 'CA' then 'Canada'
        when 'IN' then 'India'
        when 'AU' then 'Australia'
        when 'GB' then 'United Kingdom'
        when 'DE' then 'Germany'
        when 'FR' then 'France'
        when 'JP' then 'Japan'
        else null
    end as country_name,

    updated_at,
    created_at,

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

from {{ source('retail_bronze', 'customers_cdc') }}
