-- Retail source schema for PostgreSQL.
-- The source model intentionally stays small; pipeline scale comes from row volume.

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMPTZ NOT NULL,
    status VARCHAR(30) NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_order_date
    ON orders(order_date);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON order_items(product_id);
