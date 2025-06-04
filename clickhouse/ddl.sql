CREATE DATABASE IF NOT EXISTS purchase_analysis;

CREATE TABLE IF NOT EXISTS purchase_analysis.purchases_stream
(
    purchase_id   UInt32,
    client_id     UInt32,
    product_id    UInt32,
    purchased_at  DateTime,
    price         Decimal(10,2),
    full_price    Decimal(10,2),
    quantity      UInt32
) ENGINE = MergeTree()
ORDER BY (purchased_at, purchase_id);
