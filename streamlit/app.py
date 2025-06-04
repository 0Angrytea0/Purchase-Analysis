import os
import streamlit as st
import pandas as pd
from clickhouse_driver import Client

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT_TCP", 9000))

VITRINES = {
    "mart_sales_by_day":                   "SELECT sale_date, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_day GROUP BY sale_date ORDER BY sale_date DESC",
    "mart_sales_by_month":                 "SELECT sale_month, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_month GROUP BY sale_month ORDER BY sale_month DESC",
    "mart_sales_by_year":                  "SELECT sale_year, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_year GROUP BY sale_year ORDER BY sale_year DESC",
    "mart_sales_by_product":               "SELECT product_id, name, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_product GROUP BY product_id, name ORDER BY total_revenue DESC",
    "mart_sales_by_client":                "SELECT client_id, client_name, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_client GROUP BY client_id, client_name ORDER BY total_revenue DESC",
    "mart_sales_by_seller":                "SELECT seller_id, seller_name, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_sales_by_seller GROUP BY seller_id, seller_name ORDER BY total_revenue DESC",
    "mart_avg_order_value_by_day":         "SELECT sale_date, sum(total_orders) AS total_orders, sum(total_revenue) AS total_revenue FROM mart_avg_order_value_by_day GROUP BY sale_date ORDER BY sale_date DESC",
    "mart_unique_clients_per_day":         "SELECT sale_date, sum(unique_clients) AS unique_clients FROM mart_unique_clients_per_day GROUP BY sale_date ORDER BY sale_date DESC",
    "mart_unique_products_per_day":        "SELECT sale_date, sum(unique_products) AS unique_products FROM mart_unique_products_per_day GROUP BY sale_date ORDER BY sale_date DESC",
    "mart_seller_revenue_by_month":        "SELECT seller_id, sale_month, sum(total_quantity) AS total_quantity, sum(total_revenue) AS total_revenue FROM mart_seller_revenue_by_month GROUP BY seller_id, sale_month ORDER BY sale_month DESC",
    "mart_client_revenue_by_month":        "SELECT client_id, sale_month, sum(total_orders) AS total_orders, sum(total_revenue) AS total_revenue FROM mart_client_revenue_by_month GROUP BY client_id, sale_month ORDER BY sale_month DESC",
    "mart_product_quantity_sold_by_month": "SELECT product_id, sale_month, sum(total_quantity) AS total_quantity FROM mart_product_quantity_sold_by_month GROUP BY product_id, sale_month ORDER BY sale_month DESC",
    "mart_top_products_by_year":           "SELECT sale_year, product_id, name, sum(total_revenue) AS total_revenue FROM mart_top_products_by_year GROUP BY sale_year, product_id, name ORDER BY sale_year DESC, total_revenue DESC",
    "mart_top_clients_by_year":            "SELECT sale_year, client_id, client_name, sum(total_revenue) AS total_revenue FROM mart_top_clients_by_year GROUP BY sale_year, client_id, client_name ORDER BY sale_year DESC, total_revenue DESC",
    "mart_client_purchase_frequency":      "SELECT client_id, client_name, sum(total_orders) AS total_orders, sum(total_revenue) AS total_revenue FROM mart_client_purchase_frequency GROUP BY client_id, client_name ORDER BY total_revenue DESC"
}

@st.cache_resource
def get_clickhouse_client():
    return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

@st.cache_data(ttl=300)
def query_clickhouse(sql: str) -> pd.DataFrame:
    client = get_clickhouse_client()
    data = client.execute(sql)
    cols = [col[0] for col in client.execute(f"DESCRIBE TABLE ({sql})")]
    return pd.DataFrame(data, columns=cols)

def plot_time_series(df: pd.DataFrame, date_col: str, value_col: str, title: str):
    ts_df = df.copy()
    ts_df[date_col] = pd.to_datetime(ts_df[date_col])
    ts_df = ts_df.sort_values(date_col).set_index(date_col)
    st.line_chart(ts_df[value_col], use_container_width=True)
    st.write(f"**{title}**")

def main():
    st.set_page_config(page_title="Аналитика продаж", layout="wide")
    st.title("Витрины продаж")

    vitrine_names = list(VITRINES.keys())
    selected = st.selectbox(
        "Выберите витрину",
        options=vitrine_names,
        index=len(vitrine_names) - 1,
        help="Можно пролистать список и выбрать любую витрину."
    )

    try:
        df = query_clickhouse(VITRINES[selected])
    except Exception as e:
        st.error(f"Ошибка запроса: {e}")
        return

    for col in ["total_quantity", "total_revenue", "total_orders", "unique_clients", "unique_products"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    st.subheader(f"Витрина: {selected}")
    st.write(f"Количество строк: {len(df)}")
    st.dataframe(df, use_container_width=True)

    if selected == "mart_sales_by_day":
        plot_time_series(df, "sale_date", "total_revenue", "Выручка по дням")
        plot_time_series(df, "sale_date", "total_quantity", "Количество продаж по дням")

    elif selected == "mart_sales_by_month":
        df_plot = df.copy()
        df_plot["sale_month"] = pd.to_datetime(df_plot["sale_month"] + "-01")
        df_plot = df_plot.sort_values("sale_month").set_index("sale_month")
        st.line_chart(df_plot["total_revenue"], use_container_width=True)
        st.write("**Выручка по месяцам**")
        st.line_chart(df_plot["total_quantity"], use_container_width=True)
        st.write("**Количество продаж по месяцам**")

    elif selected == "mart_sales_by_year":
        fig_df = df.sort_values("sale_year").set_index("sale_year")
        st.bar_chart(fig_df["total_revenue"], use_container_width=True)
        st.write("**Выручка по годам**")
        st.bar_chart(fig_df["total_quantity"], use_container_width=True)
        st.write("**Количество продаж по годам**")

    elif selected == "mart_sales_by_product":
        top10 = df.head(10)
        if not top10.empty:
            st.bar_chart(top10.set_index("name")["total_revenue"], use_container_width=True)
            st.write("**Топ-10 товаров по выручке**")

    elif selected == "mart_sales_by_client":
        top10c = df.head(10)
        if not top10c.empty:
            st.bar_chart(top10c.set_index("client_name")["total_revenue"], use_container_width=True)
            st.write("**Топ-10 клиентов по выручке**")

    elif selected == "mart_sales_by_seller":
        top10s = df.head(10)
        if not top10s.empty:
            st.bar_chart(top10s.set_index("seller_name")["total_revenue"], use_container_width=True)
            st.write("**Топ-10 продавцов по выручке**")

    elif selected == "mart_avg_order_value_by_day":
        aov_df = df.copy()
        aov_df["avg_order_value"] = aov_df["total_revenue"] / aov_df["total_orders"]
        plot_time_series(aov_df, "sale_date", "avg_order_value", "Средний чек по дням")

    elif selected == "mart_unique_clients_per_day":
        plot_time_series(df, "sale_date", "unique_clients", "Уникальные клиенты по дням")

    elif selected == "mart_unique_products_per_day":
        plot_time_series(df, "sale_date", "unique_products", "Уникальные товары по дням")

    elif selected == "mart_seller_revenue_by_month":
        df_plot = df.copy()
        df_plot["sale_month"] = pd.to_datetime(df_plot["sale_month"] + "-01")
        monthly = df_plot.groupby("sale_month")["total_revenue"].sum().reset_index()
        monthly = monthly.set_index("sale_month").sort_index()
        st.line_chart(monthly["total_revenue"], use_container_width=True)
        st.write("**Суммарная выручка всех продавцов по месяцам**")

    elif selected == "mart_client_revenue_by_month":
        df_plot = df.copy()
        df_plot["sale_month"] = pd.to_datetime(df_plot["sale_month"] + "-01")
        monthly = df_plot.groupby("sale_month")["total_revenue"].sum().reset_index()
        monthly = monthly.set_index("sale_month").sort_index()
        st.line_chart(monthly["total_revenue"], use_container_width=True)
        st.write("**Суммарная выручка всех клиентов по месяцам**")

    elif selected == "mart_product_quantity_sold_by_month":
        df_plot = df.copy()
        df_plot["sale_month"] = pd.to_datetime(df_plot["sale_month"] + "-01")
        monthly = df_plot.groupby("sale_month")["total_quantity"].sum().reset_index()
        monthly = monthly.set_index("sale_month").sort_index()
        st.line_chart(monthly["total_quantity"], use_container_width=True)
        st.write("**Общее количество проданных товаров по месяцам**")

    elif selected == "mart_top_products_by_year":
        df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce")
        last_year = df["sale_year"].max()
        subset = df[df["sale_year"] == last_year]
        if not subset.empty:
            top10 = subset.sort_values("total_revenue", ascending=False).head(10)
            st.bar_chart(top10.set_index("name")["total_revenue"], use_container_width=True)
            st.write(f"**Топ-10 товаров по выручке за {last_year}**")

    elif selected == "mart_top_clients_by_year":
        df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce")
        last_year = df["sale_year"].max()
        subset = df[df["sale_year"] == last_year]
        if not subset.empty:
            top10c = subset.sort_values("total_revenue", ascending=False).head(10)
            st.bar_chart(top10c.set_index("client_name")["total_revenue"], use_container_width=True)
            st.write(f"**Топ-10 клиентов по выручке за {last_year}**")

    elif selected == "mart_client_purchase_frequency":
        df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce")
        top_clients = df.sort_values("total_revenue", ascending=False).head(10)
        if not top_clients.empty:
            st.bar_chart(top_clients.set_index("client_name")["total_revenue"], use_container_width=True)
            st.write("**10 самых лояльных клиентов по выручке**")

if __name__ == "__main__":
    main()
