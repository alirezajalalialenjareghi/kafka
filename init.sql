CREATE TABLE IF NOT EXISTS orders (
    event_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    quantity INTEGER NOT NULL,
    unit_price BIGINT NOT NULL,
    total_amount BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_order_id
ON orders(order_id);

CREATE INDEX IF NOT EXISTS idx_orders_event_time
ON orders(event_time);
