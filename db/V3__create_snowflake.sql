CREATE TABLE IF NOT EXISTS dim_clients (
    client_id   INT PRIMARY KEY,               
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) UNIQUE,
    signup_date TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id    INT PRIMARY KEY,             
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    company_name VARCHAR(200) NOT NULL,
    email        VARCHAR(255) UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id   INT PRIMARY KEY,              
    seller_id    INT REFERENCES dim_sellers(seller_id) ON DELETE SET NULL,
    name         VARCHAR(200) NOT NULL,
    price        NUMERIC(12,2) NOT NULL,
    quantity     INT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_purchases (
    purchase_id   INT PRIMARY KEY,             
    product_id    INT REFERENCES dim_products(product_id) ON DELETE SET NULL,
    client_id     INT REFERENCES dim_clients(client_id) ON DELETE SET NULL,
    seller_id     INT REFERENCES dim_sellers(seller_id) ON DELETE SET NULL,  
    quantity      INT NOT NULL,
    price         NUMERIC(12,2) NOT NULL,
    full_price    NUMERIC(12,2) NOT NULL,
    purchased_at  TIMESTAMPTZ NOT NULL
);
