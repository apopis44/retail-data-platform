EXECUTE STATEMENT SET
BEGIN
    INSERT INTO `bq_customers_cdc`
    SELECT *
    FROM `customers_typed`;

    INSERT INTO `bq_products_cdc`
    SELECT *
    FROM `products_typed`;

    INSERT INTO `bq_orders_cdc`
    SELECT *
    FROM `orders_typed`;

    INSERT INTO `bq_order_items_cdc`
    SELECT *
    FROM `order_items_typed`;

    INSERT INTO `bq_order_metrics_5m`
    SELECT
        `window_start`,
        `window_end`,

        COUNT(
            CASE
                WHEN `event_kind` = 'order'
                    AND `operation` = 'c'
                THEN 1
            END
        ) AS `order_count`,

        CAST(
            SUM(
                CASE
                    WHEN `event_kind` = 'order'
                        AND `operation` = 'c'
                    THEN `total_amount`
                    ELSE CAST(0 AS DECIMAL(14, 2))
                END
            )
            AS DECIMAL(31, 2)
        ) AS `gross_revenue`,

        CAST(
            AVG(
                CASE
                    WHEN `event_kind` = 'order'
                        AND `operation` = 'c'
                    THEN `total_amount`
                END
            )
            AS DECIMAL(31, 2)
        ) AS `average_order_value`,

        COUNT(
            DISTINCT CASE
                WHEN `event_kind` = 'order'
                    AND `operation` = 'c'
                THEN `customer_id`
            END
        ) AS `unique_customers`,

        MAX(
            CASE
                WHEN `event_kind` = 'heartbeat'
                THEN `source_event_at`
            END
        ) AS `watermark_advanced_at`,

        CURRENT_TIMESTAMP AS `metric_generated_at`

    FROM TABLE(
        TUMBLE(
            TABLE `order_metric_events`,
            DESCRIPTOR(`source_event_at`),
            INTERVAL '5' MINUTES
        )
    )
    GROUP BY `window_start`, `window_end`;
END;
