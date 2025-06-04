import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def create_spark_session(app_name: str = "ReportsToClickHouse"):
    return (
        SparkSession.builder
            .appName(app_name)
            .getOrCreate()
    )


def read_postgres_table(spark: SparkSession, table_name: str):
    pg_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:" 
        f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    pg_props = {
        "user": os.getenv("POSTGRES_USER", ""),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "driver": "org.postgresql.Driver"
    }

    return spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", table_name) \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()


def write_to_clickhouse(df, table_name: str, create_table_options: str):
    ch_url = f"jdbc:clickhouse://{os.getenv('CLICKHOUSE_HOST','clickhouse')}:8123/default"
    ch_driver = "com.clickhouse.jdbc.ClickHouseDriver"

    df.write \
      .format("jdbc") \
      .mode("overwrite") \
      .option("url", ch_url) \
      .option("dbtable", table_name) \
      .option("driver", ch_driver) \
      .option("createTableOptions", create_table_options) \
      .save()


if __name__ == "__main__":
    spark = create_spark_session()

    df_clients        = read_postgres_table(spark, "dim_clients")
    df_sellers        = read_postgres_table(spark, "dim_sellers")
    df_products       = read_postgres_table(spark, "dim_products")
    df_fact_purchases = read_postgres_table(spark, "fact_purchases")

    fact     = df_fact_purchases.alias("p")
    clients  = df_clients.alias("c")
    sellers  = df_sellers.alias("s")
    products = df_products.alias("pr")
   
    mart_sales_by_day = (
        fact
        .groupBy(F.to_date("purchased_at").alias("sale_date"))
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_sales_by_day,
        "mart_sales_by_day",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_date"
    )

    mart_sales_by_month = (
        fact
        .withColumn("sale_month", F.date_format("purchased_at", "yyyy-MM"))
        .groupBy("sale_month")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_sales_by_month,
        "mart_sales_by_month",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_month"
    )

    mart_sales_by_year = (
        fact
        .withColumn("sale_year", F.year("purchased_at"))
        .groupBy("sale_year")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_sales_by_year,
        "mart_sales_by_year",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_year"
    )

    mart_sales_by_product = (
        fact
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
        .join(products.select("product_id", "name"), "product_id", "left")
        .select("product_id", "name", "total_quantity", "total_revenue")
    )
    write_to_clickhouse(
        mart_sales_by_product,
        "mart_sales_by_product",
        "ENGINE = AggregatingMergeTree() ORDER BY product_id"
    )

    mart_sales_by_client = (
        fact
        .groupBy("client_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
        .join(
            clients.selectExpr("client_id", "first_name || ' ' || last_name AS client_name"),
            "client_id", "left"
        )
        .select("client_id", "client_name", "total_quantity", "total_revenue")
    )
    write_to_clickhouse(
        mart_sales_by_client,
        "mart_sales_by_client",
        "ENGINE = AggregatingMergeTree() ORDER BY client_id"
    )

    mart_sales_by_seller = (
        fact
        .groupBy("seller_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
        .join(
            sellers.selectExpr("seller_id", "first_name || ' ' || last_name AS seller_name"),
            "seller_id", "left"
        )
        .select("seller_id", "seller_name", "total_quantity", "total_revenue")
    )
    write_to_clickhouse(
        mart_sales_by_seller,
        "mart_sales_by_seller",
        "ENGINE = AggregatingMergeTree() ORDER BY seller_id"
    )

    mart_avg_order_value_by_day = (
        fact
        .groupBy(F.to_date("purchased_at").alias("sale_date"))
        .agg(
            F.count("purchase_id").alias("total_orders"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_avg_order_value_by_day,
        "mart_avg_order_value_by_day",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_date"
    )

    mart_unique_clients_per_day = (
        fact
        .groupBy(F.to_date("purchased_at").alias("sale_date"))
        .agg(
            F.countDistinct("client_id").alias("unique_clients")
        )
    )
    write_to_clickhouse(
        mart_unique_clients_per_day,
        "mart_unique_clients_per_day",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_date"
    )

    mart_unique_products_per_day = (
        fact
        .groupBy(F.to_date("purchased_at").alias("sale_date"))
        .agg(
            F.countDistinct("product_id").alias("unique_products")
        )
    )
    write_to_clickhouse(
        mart_unique_products_per_day,
        "mart_unique_products_per_day",
        "ENGINE = AggregatingMergeTree() ORDER BY sale_date"
    )

    mart_seller_revenue_by_month = (
        fact
        .withColumn("sale_month", F.date_format("purchased_at", "yyyy-MM"))
        .groupBy("seller_id", "sale_month")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_seller_revenue_by_month,
        "mart_seller_revenue_by_month",
        "ENGINE = AggregatingMergeTree() ORDER BY (seller_id, sale_month)"
    )

    mart_client_revenue_by_month = (
        fact
        .withColumn("sale_month", F.date_format("purchased_at", "yyyy-MM"))
        .groupBy("client_id", "sale_month")
        .agg(
            F.count("purchase_id").alias("total_orders"),
            F.sum("full_price").alias("total_revenue")
        )
    )
    write_to_clickhouse(
        mart_client_revenue_by_month,
        "mart_client_revenue_by_month",
        "ENGINE = AggregatingMergeTree() ORDER BY (client_id, sale_month)"
    )

    mart_product_quantity_sold_by_month = (
        fact
        .withColumn("sale_month", F.date_format("purchased_at", "yyyy-MM"))
        .groupBy("product_id", "sale_month")
        .agg(
            F.sum("quantity").alias("total_quantity")
        )
    )
    write_to_clickhouse(
        mart_product_quantity_sold_by_month,
        "mart_product_quantity_sold_by_month",
        "ENGINE = AggregatingMergeTree() ORDER BY (product_id, sale_month)"
    )

    mart_top_products_by_year = (
        fact
        .withColumn("sale_year", F.year("purchased_at"))
        .groupBy("sale_year", "product_id")
        .agg(
            F.sum("full_price").alias("total_revenue")
        )
        .join(
            products.select("product_id", "name"),
            "product_id", "left"
        )
        .select("sale_year", "product_id", "name", "total_revenue")
    )
    write_to_clickhouse(
        mart_top_products_by_year,
        "mart_top_products_by_year",
        "ENGINE = AggregatingMergeTree() ORDER BY (sale_year, product_id)"
    )

    mart_top_clients_by_year = (
        fact
        .withColumn("sale_year", F.year("purchased_at"))
        .groupBy("sale_year", "client_id")
        .agg(
            F.sum("full_price").alias("total_revenue")
        )
        .join(
            clients.selectExpr("client_id", "first_name || ' ' || last_name AS client_name"),
            "client_id", "left"
        )
        .select("sale_year", "client_id", "client_name", "total_revenue")
    )
    write_to_clickhouse(
        mart_top_clients_by_year,
        "mart_top_clients_by_year",
        "ENGINE = AggregatingMergeTree() ORDER BY (sale_year, client_id)"
    )

    mart_client_purchase_frequency = (
        fact
        .groupBy("client_id")
        .agg(
            F.count("purchase_id").alias("total_orders"),
            F.sum("full_price").alias("total_revenue")
        )
        .join(
            clients.selectExpr("client_id", "first_name || ' ' || last_name AS client_name"),
            "client_id", "left"
        )
        .select("client_id", "client_name", "total_orders", "total_revenue")
    )
    write_to_clickhouse(
        mart_client_purchase_frequency,
        "mart_client_purchase_frequency",
        "ENGINE = AggregatingMergeTree() ORDER BY client_id"
    )

    spark.stop()
