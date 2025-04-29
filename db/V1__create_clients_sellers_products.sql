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
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
