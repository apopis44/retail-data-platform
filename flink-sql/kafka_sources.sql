CREATE TEMPORARY TABLE kafka_customers (
    `key_customer_id` BIGINT,
    `after` ROW<
        `customer_id` BIGINT,
        `first_name` STRING,
        `last_name` STRING,
        `email` STRING,
        `country` STRING,
        `created_at` TIMESTAMP_LTZ(6),
        `updated_at` TIMESTAMP_LTZ(6)
    >,
    `source` ROW<
        `version` STRING,
        `connector` STRING,
        `name` STRING,
        `ts_ms` BIGINT,
        `snapshot` STRING,
        `db` STRING,
        `sequence` STRING,
        `ts_us` BIGINT,
        `ts_ns` BIGINT,
        `schema` STRING,
        `table` STRING,
        `txId` BIGINT,
        `lsn` BIGINT,
        `xmin` BIGINT,
        `origin` STRING,
        `origin_lsn` BIGINT
    >,
    `transaction` ROW<
        `id` STRING,
        `total_order` BIGINT,
        `data_collection_order` BIGINT
    >,
    `op` STRING,
    `ts_ms` BIGINT,
    `ts_us` BIGINT,
    `ts_ns` BIGINT,
    `kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
    `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,
    `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    `source_event_at` AS TO_TIMESTAMP_LTZ(`source`.`ts_ms`, 3),
    WATERMARK FOR `source_event_at`
        AS `source_event_at` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'retail.public.customers',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'retail-flink-customers-cdc',
    'scan.startup.mode' = 'group-offsets',
    'key.format' = 'json',
    'key.fields' = 'key_customer_id',
    'key.fields-prefix' = 'key_',
    'key.json.fail-on-missing-field' = 'false',
    'key.json.ignore-parse-errors' = 'false',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.json.fail-on-missing-field' = 'false',
    'value.json.ignore-parse-errors' = 'false',
    'value.json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TEMPORARY TABLE kafka_products (
    `key_product_id` BIGINT,
    `after` ROW<
        `product_id` BIGINT,
        `product_name` STRING,
        `category` STRING,
        `unit_price` DECIMAL(12, 2),
        `created_at` TIMESTAMP_LTZ(6),
        `updated_at` TIMESTAMP_LTZ(6)
    >,
    `source` ROW<
        `version` STRING,
        `connector` STRING,
        `name` STRING,
        `ts_ms` BIGINT,
        `snapshot` STRING,
        `db` STRING,
        `sequence` STRING,
        `ts_us` BIGINT,
        `ts_ns` BIGINT,
        `schema` STRING,
        `table` STRING,
        `txId` BIGINT,
        `lsn` BIGINT,
        `xmin` BIGINT,
        `origin` STRING,
        `origin_lsn` BIGINT
    >,
    `transaction` ROW<
        `id` STRING,
        `total_order` BIGINT,
        `data_collection_order` BIGINT
    >,
    `op` STRING,
    `ts_ms` BIGINT,
    `ts_us` BIGINT,
    `ts_ns` BIGINT,
    `kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
    `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,
    `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    `source_event_at` AS TO_TIMESTAMP_LTZ(`source`.`ts_ms`, 3),
    WATERMARK FOR `source_event_at`
        AS `source_event_at` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'retail.public.products',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'retail-flink-products-cdc',
    'scan.startup.mode' = 'group-offsets',
    'key.format' = 'json',
    'key.fields' = 'key_product_id',
    'key.fields-prefix' = 'key_',
    'key.json.fail-on-missing-field' = 'false',
    'key.json.ignore-parse-errors' = 'false',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.json.fail-on-missing-field' = 'false',
    'value.json.ignore-parse-errors' = 'false',
    'value.json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TEMPORARY TABLE kafka_orders (
    `key_order_id` BIGINT,
    `after` ROW<
        `order_id` BIGINT,
        `customer_id` BIGINT,
        `order_date` TIMESTAMP_LTZ(6),
        `status` STRING,
        `total_amount` DECIMAL(14, 2),
        `created_at` TIMESTAMP_LTZ(6),
        `updated_at` TIMESTAMP_LTZ(6)
    >,
    `source` ROW<
        `version` STRING,
        `connector` STRING,
        `name` STRING,
        `ts_ms` BIGINT,
        `snapshot` STRING,
        `db` STRING,
        `sequence` STRING,
        `ts_us` BIGINT,
        `ts_ns` BIGINT,
        `schema` STRING,
        `table` STRING,
        `txId` BIGINT,
        `lsn` BIGINT,
        `xmin` BIGINT,
        `origin` STRING,
        `origin_lsn` BIGINT
    >,
    `transaction` ROW<
        `id` STRING,
        `total_order` BIGINT,
        `data_collection_order` BIGINT
    >,
    `op` STRING,
    `ts_ms` BIGINT,
    `ts_us` BIGINT,
    `ts_ns` BIGINT,
    `kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
    `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,
    `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    `source_event_at` AS TO_TIMESTAMP_LTZ(`source`.`ts_ms`, 3),
    WATERMARK FOR `source_event_at`
        AS `source_event_at` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'retail.public.orders',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'retail-flink-orders-cdc',
    'scan.startup.mode' = 'group-offsets',
    'scan.watermark.alignment.group' = 'retail-order-metrics',
    'scan.watermark.alignment.max-drift' = '1 min',
    'scan.watermark.alignment.update-interval' = '1 s',
    'key.format' = 'json',
    'key.fields' = 'key_order_id',
    'key.fields-prefix' = 'key_',
    'key.json.fail-on-missing-field' = 'false',
    'key.json.ignore-parse-errors' = 'false',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.json.fail-on-missing-field' = 'false',
    'value.json.ignore-parse-errors' = 'false',
    'value.json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TEMPORARY TABLE kafka_order_items (
    `key_order_item_id` BIGINT,
    `after` ROW<
        `order_item_id` BIGINT,
        `order_id` BIGINT,
        `product_id` BIGINT,
        `quantity` INT,
        `unit_price` DECIMAL(12, 2),
        `created_at` TIMESTAMP_LTZ(6),
        `updated_at` TIMESTAMP_LTZ(6)
    >,
    `source` ROW<
        `version` STRING,
        `connector` STRING,
        `name` STRING,
        `ts_ms` BIGINT,
        `snapshot` STRING,
        `db` STRING,
        `sequence` STRING,
        `ts_us` BIGINT,
        `ts_ns` BIGINT,
        `schema` STRING,
        `table` STRING,
        `txId` BIGINT,
        `lsn` BIGINT,
        `xmin` BIGINT,
        `origin` STRING,
        `origin_lsn` BIGINT
    >,
    `transaction` ROW<
        `id` STRING,
        `total_order` BIGINT,
        `data_collection_order` BIGINT
    >,
    `op` STRING,
    `ts_ms` BIGINT,
    `ts_us` BIGINT,
    `ts_ns` BIGINT,
    `kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
    `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,
    `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    `source_event_at` AS TO_TIMESTAMP_LTZ(`source`.`ts_ms`, 3),
    WATERMARK FOR `source_event_at`
        AS `source_event_at` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'retail.public.order_items',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'retail-flink-order-items-cdc',
    'scan.startup.mode' = 'group-offsets',
    'key.format' = 'json',
    'key.fields' = 'key_order_item_id',
    'key.fields-prefix' = 'key_',
    'key.json.fail-on-missing-field' = 'false',
    'key.json.ignore-parse-errors' = 'false',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.json.fail-on-missing-field' = 'false',
    'value.json.ignore-parse-errors' = 'false',
    'value.json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TEMPORARY TABLE kafka_cdc_heartbeat (
    `ts_ms` BIGINT,
    `heartbeat_event_at`
        AS TO_TIMESTAMP_LTZ(`ts_ms`, 3),

    WATERMARK FOR `heartbeat_event_at`
        as `heartbeat_event_at` - INTERVAL '30' SECOND
) WITH (
    'connector' ='kafka',
    'topic' = '${DEBEZIUM_HEARTBEAT_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'retail-flink-cdc-heartbeat',
    'scan.startup.mode' = 'latest-offset',
    'scan.watermark.alignment.group' = 'retail-order-metrics',
    'scan.watermark.alignment.max-drift' = '1 min',
    'scan.watermark.alignment.update-interval' = '1 s',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false',
    'value.json.ignore-parse-errors' = 'false'
);
