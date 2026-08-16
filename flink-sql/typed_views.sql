CREATE TEMPORARY VIEW customers_typed AS
SELECT
    CONCAT(
        `kafka_topic`,
        ':',
        CAST(`kafka_partition` AS STRING),
        ':',
        CAST(`kafka_offset` AS STRING)
    ) AS `cdc_event_id`,
    `op` AS `operation`,
    COALESCE(`after`.`customer_id`, `key_customer_id`) AS `customer_id`,
    `after`.`first_name` AS `first_name`,
    `after`.`last_name` AS `last_name`,
    `after`.`email` AS `email`,
    `after`.`country` AS `country`,
    `after`.`created_at` AS `created_at`,
    `after`.`updated_at` AS `updated_at`,
    `source`.`version` AS `source_version`,
    `source`.`connector` AS `source_connector`,
    `source`.`name` AS `source_name`,
    `source`.`ts_ms` AS `source_ts_ms`,
    `source`.`ts_us` AS `source_ts_us`,
    `source`.`ts_ns` AS `source_ts_ns`,
    `source`.`snapshot` AS `source_snapshot`,
    `source`.`db` AS `source_database`,
    `source`.`sequence` AS `source_sequence`,
    `source`.`schema` AS `source_schema`,
    `source`.`table` AS `source_table`,
    `source`.`txId` AS `source_transaction_id`,
    `source`.`lsn` AS `source_lsn`,
    `source`.`xmin` AS `source_xmin`,
    `source`.`origin` AS `source_origin`,
    `source`.`origin_lsn` AS `source_origin_lsn`,
    `transaction`.`id` AS `transaction_id`,
    `transaction`.`total_order` AS `transaction_total_order`,
    `transaction`.`data_collection_order`
        AS `transaction_data_collection_order`,
    `ts_ms` AS `debezium_ts_ms`,
    `ts_us` AS `debezium_ts_us`,
    `ts_ns` AS `debezium_ts_ns`,
    `source_event_at`,
    TO_TIMESTAMP_LTZ(`ts_ms`, 3) AS `debezium_processed_at`,
    `kafka_topic`,
    `kafka_partition`,
    `kafka_offset`,
    `kafka_timestamp`,
    CURRENT_TIMESTAMP AS `flink_ingested_at`,
    CAST(
        `ts_ms` - `source`.`ts_ms`
        AS BIGINT
    ) AS `processing_latency_ms`
FROM `kafka_customers`;

CREATE TEMPORARY VIEW products_typed AS
SELECT
    CONCAT(
        `kafka_topic`,
        ':',
        CAST(`kafka_partition` AS STRING),
        ':',
        CAST(`kafka_offset` AS STRING)
    ) AS `cdc_event_id`,
    `op` AS `operation`,
    COALESCE(`after`.`product_id`, `key_product_id`) AS `product_id`,
    `after`.`product_name` AS `product_name`,
    `after`.`category` AS `category`,
    `after`.`unit_price` AS `unit_price`,
    `after`.`created_at` AS `created_at`,
    `after`.`updated_at` AS `updated_at`,
    `source`.`version` AS `source_version`,
    `source`.`connector` AS `source_connector`,
    `source`.`name` AS `source_name`,
    `source`.`ts_ms` AS `source_ts_ms`,
    `source`.`ts_us` AS `source_ts_us`,
    `source`.`ts_ns` AS `source_ts_ns`,
    `source`.`snapshot` AS `source_snapshot`,
    `source`.`db` AS `source_database`,
    `source`.`sequence` AS `source_sequence`,
    `source`.`schema` AS `source_schema`,
    `source`.`table` AS `source_table`,
    `source`.`txId` AS `source_transaction_id`,
    `source`.`lsn` AS `source_lsn`,
    `source`.`xmin` AS `source_xmin`,
    `source`.`origin` AS `source_origin`,
    `source`.`origin_lsn` AS `source_origin_lsn`,
    `transaction`.`id` AS `transaction_id`,
    `transaction`.`total_order` AS `transaction_total_order`,
    `transaction`.`data_collection_order`
        AS `transaction_data_collection_order`,
    `ts_ms` AS `debezium_ts_ms`,
    `ts_us` AS `debezium_ts_us`,
    `ts_ns` AS `debezium_ts_ns`,
    `source_event_at`,
    TO_TIMESTAMP_LTZ(`ts_ms`, 3) AS `debezium_processed_at`,
    `kafka_topic`,
    `kafka_partition`,
    `kafka_offset`,
    `kafka_timestamp`,
    CURRENT_TIMESTAMP AS `flink_ingested_at`,
    CAST(
        `ts_ms` - `source`.`ts_ms`
        AS BIGINT
    ) AS `processing_latency_ms`
FROM `kafka_products`;

CREATE TEMPORARY VIEW orders_typed AS
SELECT
    CONCAT(
        `kafka_topic`,
        ':',
        CAST(`kafka_partition` AS STRING),
        ':',
        CAST(`kafka_offset` AS STRING)
    ) AS `cdc_event_id`,
    `op` AS `operation`,
    COALESCE(`after`.`order_id`, `key_order_id`) AS `order_id`,
    `after`.`customer_id` AS `customer_id`,
    `after`.`order_date` AS `order_date`,
    `after`.`status` AS `status`,
    `after`.`total_amount` AS `total_amount`,
    `after`.`created_at` AS `created_at`,
    `after`.`updated_at` AS `updated_at`,
    `source`.`version` AS `source_version`,
    `source`.`connector` AS `source_connector`,
    `source`.`name` AS `source_name`,
    `source`.`ts_ms` AS `source_ts_ms`,
    `source`.`ts_us` AS `source_ts_us`,
    `source`.`ts_ns` AS `source_ts_ns`,
    `source`.`snapshot` AS `source_snapshot`,
    `source`.`db` AS `source_database`,
    `source`.`sequence` AS `source_sequence`,
    `source`.`schema` AS `source_schema`,
    `source`.`table` AS `source_table`,
    `source`.`txId` AS `source_transaction_id`,
    `source`.`lsn` AS `source_lsn`,
    `source`.`xmin` AS `source_xmin`,
    `source`.`origin` AS `source_origin`,
    `source`.`origin_lsn` AS `source_origin_lsn`,
    `transaction`.`id` AS `transaction_id`,
    `transaction`.`total_order` AS `transaction_total_order`,
    `transaction`.`data_collection_order`
        AS `transaction_data_collection_order`,
    `ts_ms` AS `debezium_ts_ms`,
    `ts_us` AS `debezium_ts_us`,
    `ts_ns` AS `debezium_ts_ns`,
    `source_event_at`,
    TO_TIMESTAMP_LTZ(`ts_ms`, 3) AS `debezium_processed_at`,
    `kafka_topic`,
    `kafka_partition`,
    `kafka_offset`,
    `kafka_timestamp`,
    CURRENT_TIMESTAMP AS `flink_ingested_at`,
    CAST(
        `ts_ms` - `source`.`ts_ms`
        AS BIGINT
    ) AS `processing_latency_ms`
FROM `kafka_orders`;

CREATE TEMPORARY VIEW order_items_typed AS
SELECT
    CONCAT(
        `kafka_topic`,
        ':',
        CAST(`kafka_partition` AS STRING),
        ':',
        CAST(`kafka_offset` AS STRING)
    ) AS `cdc_event_id`,
    `op` AS `operation`,
    COALESCE(
        `after`.`order_item_id`,
        `key_order_item_id`
    ) AS `order_item_id`,
    `after`.`order_id` AS `order_id`,
    `after`.`product_id` AS `product_id`,
    `after`.`quantity` AS `quantity`,
    `after`.`unit_price` AS `unit_price`,
    CAST(
        `after`.`quantity` * `after`.`unit_price`
        AS DECIMAL(14, 2)
    ) AS `line_amount`,
    `after`.`created_at` AS `created_at`,
    `after`.`updated_at` AS `updated_at`,
    `source`.`version` AS `source_version`,
    `source`.`connector` AS `source_connector`,
    `source`.`name` AS `source_name`,
    `source`.`ts_ms` AS `source_ts_ms`,
    `source`.`ts_us` AS `source_ts_us`,
    `source`.`ts_ns` AS `source_ts_ns`,
    `source`.`snapshot` AS `source_snapshot`,
    `source`.`db` AS `source_database`,
    `source`.`sequence` AS `source_sequence`,
    `source`.`schema` AS `source_schema`,
    `source`.`table` AS `source_table`,
    `source`.`txId` AS `source_transaction_id`,
    `source`.`lsn` AS `source_lsn`,
    `source`.`xmin` AS `source_xmin`,
    `source`.`origin` AS `source_origin`,
    `source`.`origin_lsn` AS `source_origin_lsn`,
    `transaction`.`id` AS `transaction_id`,
    `transaction`.`total_order` AS `transaction_total_order`,
    `transaction`.`data_collection_order`
        AS `transaction_data_collection_order`,
    `ts_ms` AS `debezium_ts_ms`,
    `ts_us` AS `debezium_ts_us`,
    `ts_ns` AS `debezium_ts_ns`,
    `source_event_at`,
    TO_TIMESTAMP_LTZ(`ts_ms`, 3) AS `debezium_processed_at`,
    `kafka_topic`,
    `kafka_partition`,
    `kafka_offset`,
    `kafka_timestamp`,
    CURRENT_TIMESTAMP AS `flink_ingested_at`,
    CAST(
        `ts_ms` - `source`.`ts_ms`
        AS BIGINT
    ) AS `processing_latency_ms`
FROM `kafka_order_items`;


CREATE TEMPORARY VIEW order_metric_events AS
SELECT
    'order' AS `event_kind`,
    `operation`,
    `customer_id`,
    CAST(`total_amount` AS DECIMAL(14, 2)) AS `total_amount`,
    `source_event_at`
FROM `orders_typed`

UNION ALL

SELECT
    'heartbeat' AS `event_kind`,
    CAST(NULL AS STRING) AS `operation`,
    CAST(NULL AS BIGINT) AS `customer_id`,
    CAST(NULL AS DECIMAL(14, 2)) AS `total_amount`,
    `heartbeat_event_at` AS `source_event_at`
FROM `kafka_cdc_heartbeat`;
