CREATE TABLE IF NOT EXISTS purchases_stream
(
    purchase_id   INT,
    client_id     INT,
    product_id    INT,
    purchased_at DateTime,
    price         Decimal(10,2),
    full_price    Decimal(10,2),
    quantity      UInt32
) ENGINE = MergeTree()
ORDER BY (purchased_at, purchase_id);


CREATE TABLE IF NOT EXISTS purchases_batch
(
    purchase_id   INT,
    client_id     INT,
    product_id    INT,
    purchased_at  DateTime,
    quantity      UInt32
) ENGINE = MergeTree()
ORDER BY (purchased_at, purchase_id);

CREATE TABLE IF NOT EXISTS clients_batch
(
    client_id           INT,
    first_name          String,
    last_name           String,
    email               String,
    signup_date         DateTime
) ENGINE = MergeTree()
ORDER BY (client_id);

CREATE TABLE IF NOT EXISTS sellers_batch
(
    seller_id       INT,
    first_name      String,
    last_name       String,
    company_name    String,
    email           String,
    created_at      DateTime
) ENGINE = MergeTree()
ORDER BY (seller_id);

CREATE TABLE IF NOT EXISTS products_batch
(
    product_id  INT,
    seller_id   INT,
    name        String,
    price       Decimal(10,2),
    quantity    UInt32,
    created_at  DateTime
) ENGINE = MergeTree()
ORDER BY (product_id);

