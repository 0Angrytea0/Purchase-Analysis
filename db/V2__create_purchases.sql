CREATE TABLE IF NOT EXISTS purchases (
    purchase_id  SERIAL PRIMARY KEY,
    client_id    INT NOT NULL REFERENCES clients(client_id),
    product_id   INT NOT NULL REFERENCES products(product_id),
    quantity     INT NOT NULL DEFAULT 1,
    purchased_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
