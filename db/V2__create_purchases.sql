CREATE TABLE IF NOT EXISTS purchases (
    purchase_id  SERIAL PRIMARY KEY,
    client_id    INT NOT NULL REFERENCES clients(client_id),
    product_id   INT NOT NULL REFERENCES products(product_id),
    quantity     INT NOT NULL DEFAULT 1,
    purchased_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

TRUNCATE TABLE purchases
  RESTART IDENTITY CASCADE;

INSERT INTO purchases (client_id, product_id, quantity, purchased_at) VALUES
  (1, 1, 2, '2025-03-01 13:00:00'),
  (2, 2, 1, '2025-03-02 15:30:00');


CREATE TABLE IF NOT EXISTS raw_load_log (
  filename   TEXT PRIMARY KEY,
  loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
