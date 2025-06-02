import os
import pandas as pd
import streamlit as st
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

client = get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "purchase_analysis"),
    secure=False
)

st.set_page_config(layout="wide")
st.title("Dashboard по материализованным представлениям ClickHouse")


def query_clickhouse(sql: str) -> pd.DataFrame:
    return client.query_df(sql)


st.header("1. Выручка по дням")
df_sales_by_day = query_clickhouse("""
    SELECT
        toDate(date)                AS date,
        toFloat64(revenue)          AS revenue,
        toUInt64(total_quantity)    AS total_quantity,
        toUInt64(purchase_count)    AS purchase_count
    FROM purchase_analysis.mv_sales_by_day
    ORDER BY date DESC
    LIMIT 30
""")
st.dataframe(df_sales_by_day, use_container_width=True)
st.line_chart(df_sales_by_day.set_index("date")["revenue"])


st.header("2. Динамика покупок по месяцам")
df_sales_by_month = query_clickhouse("""
    SELECT
        toDate(month)               AS month,
        toFloat64(revenue)          AS revenue,
        toUInt64(total_quantity)    AS total_quantity,
        toUInt64(purchase_count)    AS purchase_count
    FROM purchase_analysis.mv_sales_by_month
    ORDER BY month DESC
    LIMIT 24
""")
st.dataframe(df_sales_by_month, use_container_width=True)
st.line_chart(df_sales_by_month.set_index("month")["revenue"])


st.header("3. Средний чек по дням")
df_avg_receipt = query_clickhouse("""
    SELECT
        toDate(date)                AS date,
        toFloat64(avg_receipt)      AS avg_receipt
    FROM purchase_analysis.mv_avg_receipt_by_day
    ORDER BY date DESC
    LIMIT 30
""")
st.dataframe(df_avg_receipt, use_container_width=True)
st.line_chart(df_avg_receipt.set_index("date")["avg_receipt"])


st.header("4. Продажи по часам (между днями)")
df_sales_by_hour = query_clickhouse("""
    SELECT
        toDate(date)                AS date,
        toUInt8(hour)               AS hour,
        toFloat64(revenue)          AS revenue,
        toUInt64(total_quantity)    AS total_quantity,
        toUInt64(purchase_count)    AS purchase_count
    FROM purchase_analysis.mv_sales_by_hour
    ORDER BY date DESC, hour ASC
    LIMIT 200
""")
st.dataframe(df_sales_by_hour, use_container_width=True)

st.subheader("Суммарная выручка по часовому интервалу (за всё время)")
df_hour_total = query_clickhouse("""
    SELECT
        toUInt8(hour)               AS hour,
        toFloat64(sum(revenue))     AS revenue
    FROM purchase_analysis.mv_sales_by_hour
    GROUP BY hour
    ORDER BY hour
""")
st.bar_chart(df_hour_total.set_index("hour")["revenue"])


st.header("5. Топ-10 товаров по выручке")
df_top_products = query_clickhouse("""
    SELECT
        CAST(product_id AS String)               AS product_id,
        toFloat64(sumMerge(revenue_state))       AS revenue
    FROM purchase_analysis.mv_top_product_revenue
    GROUP BY product_id
    ORDER BY revenue DESC
    LIMIT 10
""")
st.table(df_top_products)


st.header("6. Топ-10 клиентов по выручке")
df_top_clients = query_clickhouse("""
    SELECT
        CAST(client_id AS String)                AS client_id,
        toFloat64(sumMerge(revenue_state))       AS revenue
    FROM purchase_analysis.mv_top_client_revenue
    GROUP BY client_id
    ORDER BY revenue DESC
    LIMIT 10
""")
st.table(df_top_clients)


st.header("7. Повторные покупки (ТОП-10 клиентов по числу транзакций)")
df_repeat_count = query_clickhouse("""
    SELECT
        CAST(client_id AS String)                  AS client_id,
        toUInt64(countMerge(purchase_count_state)) AS purchase_count
    FROM purchase_analysis.mv_repeat_purchase_count
    GROUP BY client_id
    ORDER BY purchase_count DESC
    LIMIT 10
""")
st.table(df_repeat_count)


st.header("8. Дата первой покупки клиента (ТОП-10 по ранней дате)")
df_first_purchase = query_clickhouse("""
    SELECT
        CAST(client_id AS String)                   AS client_id,
        toDate(minMerge(first_purchase_ts_state))   AS first_purchase_date
    FROM purchase_analysis.mv_first_purchase_date
    GROUP BY client_id
    ORDER BY first_purchase_date ASC
    LIMIT 10
""")
st.table(df_first_purchase)


st.header("9. Активные дни клиента (ТОП-10 по уникальным дням)")
df_active_days = query_clickhouse("""
    SELECT
        CAST(client_id AS String)                  AS client_id,
        toUInt64(uniqMerge(active_days_state))     AS active_days
    FROM purchase_analysis.mv_purchase_frequency_days
    GROUP BY client_id
    ORDER BY active_days DESC
    LIMIT 10
""")
st.table(df_active_days)


st.header("10. Распределение покупок по количеству товаров в корзине")
df_quantity_dist = query_clickhouse("""
    SELECT
        toUInt64(quantity)                   AS quantity,
        toUInt64(count_purchases)            AS count_purchases
    FROM purchase_analysis.mv_quantity_distribution
    ORDER BY quantity ASC
""")
st.bar_chart(df_quantity_dist.set_index("quantity")["count_purchases"])



st.header("11. Уникальные клиенты в день")
df_daily_unique = query_clickhouse("""
    SELECT
        toDate(purchased_at)               AS date,
        uniqExact(client_id)               AS unique_clients
    FROM purchase_analysis.purchases_stream
    GROUP BY date
    ORDER BY date DESC
    LIMIT 30
""")
st.dataframe(df_daily_unique, use_container_width=True)
st.bar_chart(df_daily_unique.set_index("date")["unique_clients"])


st.header("12. Суммарная выручка по часам (за всё время)")
df_hourly_revenue = query_clickhouse("""
    SELECT
        toHour(purchased_at)                 AS hour,
        toFloat64(sum(price * quantity))     AS revenue
    FROM purchase_analysis.purchases_stream
    GROUP BY hour
    ORDER BY hour
""")
st.dataframe(df_hourly_revenue, use_container_width=True)
st.line_chart(df_hourly_revenue.set_index("hour")["revenue"])


st.header("13. Выручка по дням недели")
df_revenue_by_weekday = query_clickhouse("""
    SELECT
        toUInt8(toDayOfWeek(purchased_at))   AS weekday,
        toFloat64(sum(price * quantity))     AS revenue
    FROM purchase_analysis.purchases_stream
    GROUP BY weekday
    ORDER BY weekday
""")
weekday_map = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
df_revenue_by_weekday["weekday_name"] = df_revenue_by_weekday["weekday"].map(weekday_map)
st.dataframe(df_revenue_by_weekday, use_container_width=True)
st.bar_chart(df_revenue_by_weekday.set_index("weekday_name")["revenue"])


st.header("14. Среднее количество товаров в корзине по дням")
df_avg_quantity = query_clickhouse("""
    SELECT
        toDate(purchased_at)               AS date,
        toFloat64(avg(quantity))           AS avg_quantity
    FROM purchase_analysis.purchases_stream
    GROUP BY date
    ORDER BY date DESC
    LIMIT 30
""")
st.dataframe(df_avg_quantity, use_container_width=True)
st.line_chart(df_avg_quantity.set_index("date")["avg_quantity"])



st.header("15. Самая дорогая единичная покупка (ТОП-10 по total)")
df_max_single = query_clickhouse("""
    SELECT
        CAST(purchase_id AS String)             AS purchase_id,
        toDate(purchased_at)                    AS date,
        toFloat64(price * quantity)             AS total
    FROM purchase_analysis.purchases_stream
    ORDER BY total DESC
    LIMIT 10
""")
st.table(df_max_single)