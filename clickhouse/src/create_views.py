import os
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB", "purchase_analysis")

DDLS = [
    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_sales_by_day
    ENGINE = SummingMergeTree()
    PARTITION BY date
    ORDER BY (date)
    AS
    SELECT
        toDate(purchased_at)                 AS date,
        sum(price * quantity)                AS revenue,
        sum(quantity)                        AS total_quantity,
        count()                              AS purchase_count
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY date
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_sales_by_month
    ENGINE = SummingMergeTree()
    PARTITION BY month
    ORDER BY (month)
    AS
    SELECT
        toStartOfMonth(purchased_at)         AS month,
        sum(price * quantity)                AS revenue,
        sum(quantity)                        AS total_quantity,
        count()                              AS purchase_count
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY month
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_avg_receipt_by_day
    ENGINE = SummingMergeTree()
    PARTITION BY date
    ORDER BY (date)
    AS
    SELECT
        toDate(purchased_at)                 AS date,
        sum(price * quantity) / count()      AS avg_receipt
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY date
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_sales_by_hour
    ENGINE = SummingMergeTree()
    PARTITION BY date
    ORDER BY (date, hour)
    AS
    SELECT
        toDate(purchased_at)                 AS date,
        toHour(purchased_at)                 AS hour,
        sum(price * quantity)                AS revenue,
        sum(quantity)                        AS total_quantity,
        count()                              AS purchase_count
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY date, hour
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_top_product_revenue
    ENGINE = AggregatingMergeTree()
    ORDER BY (product_id)
    AS
    SELECT
        product_id,
        sumState(price * quantity)           AS revenue_state
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY product_id
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_top_client_revenue
    ENGINE = AggregatingMergeTree()
    ORDER BY (client_id)
    AS
    SELECT
        client_id,
        sumState(price * quantity)           AS revenue_state
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY client_id
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_repeat_purchase_count
    ENGINE = AggregatingMergeTree()
    ORDER BY (client_id)
    AS
    SELECT
        client_id,
        countState()                         AS purchase_count_state
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY client_id
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_first_purchase_date
    ENGINE = AggregatingMergeTree()
    ORDER BY (client_id)
    AS
    SELECT
        client_id,
        minState(purchased_at)               AS first_purchase_ts_state
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY client_id
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_purchase_frequency_days
    ENGINE = AggregatingMergeTree()
    ORDER BY (client_id)
    AS
    SELECT
        client_id,
        uniqState(toDate(purchased_at))      AS active_days_state
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY client_id
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_quantity_distribution
    ENGINE = SummingMergeTree()
    ORDER BY (quantity)
    AS
    SELECT
        quantity,
        count()                              AS count_purchases
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY quantity
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_daily_unique_clients
    ENGINE = SummingMergeTree()
    PARTITION BY date
    ORDER BY (date)
    AS
    SELECT
        toDate(purchased_at)                 AS date,
        uniqExact(client_id)                 AS unique_clients
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY date
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_hourly_revenue
    ENGINE = SummingMergeTree()
    ORDER BY (hour)
    AS
    SELECT
        toHour(purchased_at)                 AS hour,
        sum(price * quantity)                AS revenue
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY hour
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_revenue_by_weekday
    ENGINE = SummingMergeTree()
    ORDER BY (weekday)
    AS
    SELECT
        toDayOfWeek(purchased_at)            AS weekday,  -- 1=Monday … 7=Sunday
        sum(price * quantity)                AS revenue
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY weekday
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_avg_quantity_per_day
    ENGINE = SummingMergeTree()
    PARTITION BY date
    ORDER BY (date)
    AS
    SELECT
        toDate(purchased_at)                 AS date,
        avg(quantity)                        AS avg_quantity
    FROM {CLICKHOUSE_DB}.purchases_stream
    GROUP BY date
    """,

    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {CLICKHOUSE_DB}.mv_max_single_purchase
    ENGINE = MergeTree()
    PARTITION BY date
    ORDER BY (total, purchase_id)
    AS
    SELECT
        purchase_id,
        toDate(purchased_at)                 AS date,
        price * quantity                     AS total
    FROM {CLICKHOUSE_DB}.purchases_stream
    """
]

def main():
    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        secure=False
    )

    for ddl in DDLS:
        try:
            client.command(ddl.strip())
            print("Витрина успешно создана/обновлена.")
        except Exception as e:
            print("Ошибка при создании витрины:", e)

if __name__ == "__main__":
    main()
