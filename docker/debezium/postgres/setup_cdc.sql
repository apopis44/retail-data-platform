CREATE TABLE IF NOT EXISTS public.debezium_signal (
    id VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'retail_orders_cdc'
    ) THEN
        ALTER PUBLICATION retail_orders_cdc SET TABLE
            public.customers,
            public.products,
            public.orders,
            public.order_items,
            public.debezium_signal;
    ELSE
        CREATE PUBLICATION retail_orders_cdc FOR TABLE
            public.customers,
            public.products,
            public.orders,
            public.order_items,
            public.debezium_signal;
    END IF;
END
$$;
