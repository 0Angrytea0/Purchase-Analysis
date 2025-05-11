CREATE TABLE IF NOT EXISTS clients (
    client_id   SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) UNIQUE,
    signup_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id    SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    company_name VARCHAR(200) NOT NULL,
    email        VARCHAR(255) UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id  SERIAL PRIMARY KEY,
    seller_id   INT NOT NULL REFERENCES sellers(seller_id) ON DELETE CASCADE,
    name        VARCHAR(200) NOT NULL,
    price       NUMERIC(12,2) NOT NULL,
    quantity     INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

TRUNCATE TABLE products, sellers, clients
  RESTART IDENTITY CASCADE;

INSERT INTO clients (first_name, last_name, email, signup_date) VALUES
  ('John',    'Doe',     'john.doe@example.com',    '2025-01-10 08:30:00'),
  ('Jane',    'Smith',   'jane.smith@example.com',  '2025-02-15 14:45:00'),
  ('Bob',     'Johnson', 'bob.johnson@example.com', '2025-03-20 09:15:00');

INSERT INTO sellers (first_name, last_name, company_name, email, created_at) VALUES
  ('Alice',   'Williams', 'Alice Co',   'alice@example.com',   '2025-01-05 11:00:00'),
  ('Charlie', 'Brown',    'Charlie LLC','charlie@example.com', '2025-02-12 16:20:00');

INSERT INTO products (seller_id, name, price, quantity, created_at) VALUES
  (1, 'Widget A', 19.99, 100, '2025-01-06 10:00:00'),
  (2, 'Gadget B', 29.99, 200, '2025-02-13 12:30:00');
