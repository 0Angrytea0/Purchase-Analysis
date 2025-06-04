import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, DecimalType, TimestampType

def create_spark_session(app_name: str = "StageToPostgres"):
    bucket = os.getenv("MINIO_BUCKET", "")
    return (
        SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", ""))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.create.bucket", "true")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
            .config("spark.sql.catalog.iceberg_catalog.warehouse", f"s3a://{bucket}/iceberg_warehouse")
            .getOrCreate()
    )

def read_iceberg_table(spark: SparkSession, table_name: str):
    return spark.read.format("iceberg").load(f"iceberg_catalog.stage.{table_name}")

def read_clickhouse_purchases(spark: SparkSession):
    clickhouse_options = {
        "url": f"jdbc:clickhouse://{os.getenv('CLICKHOUSE_HOST','clickhouse')}:8123",
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable": "purchase_analysis.purchases_stream"
    }
    return spark.read.format("jdbc").options(**clickhouse_options).load()

def write_dim_to_postgres(df, table_name: str):
    pg_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:" 
        f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    df.write \
      .format("jdbc") \
      .option("url", pg_url) \
      .option("dbtable", table_name) \
      .option("user", os.getenv("POSTGRES_USER", "")) \
      .option("password", os.getenv("POSTGRES_PASSWORD", "")) \
      .option("driver", "org.postgresql.Driver") \
      .option("truncate", "true") \
      .option("cascadeTruncate", "true") \
      .mode("overwrite") \
      .save()

def write_fact_to_postgres(df, table_name: str):
    pg_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:" 
        f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    df.write \
      .format("jdbc") \
      .option("url", pg_url) \
      .option("dbtable", table_name) \
      .option("user", os.getenv("POSTGRES_USER", "")) \
      .option("password", os.getenv("POSTGRES_PASSWORD", "")) \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()

def main():
    spark = create_spark_session("StageToPostgres")

    df_clients = (
        read_iceberg_table(spark, "clients")
          .select("client_id", "first_name", "last_name", "email", "signup_date")
          .dropDuplicates(["client_id"])
    )
    df_sellers = (
        read_iceberg_table(spark, "sellers")
          .select("seller_id", "first_name", "last_name", "company_name", "email", "created_at")
          .dropDuplicates(["seller_id"])
    )
    df_products = (
        read_iceberg_table(spark, "products")
          .withColumn("created_at", to_timestamp(col("created_at")))
          .select("product_id", "seller_id", "name", "price", "quantity", "created_at")
          .dropDuplicates(["product_id"])
    )

    write_dim_to_postgres(df_clients, "dim_clients")
    write_dim_to_postgres(df_sellers, "dim_sellers")
    write_dim_to_postgres(df_products, "dim_products")

    df_purchases_raw = read_clickhouse_purchases(spark)
    df_purchases = (
        df_purchases_raw
          .withColumn("purchased_at", col("purchased_at").cast(TimestampType()))
          .withColumn("price",        col("price").cast(DecimalType(10,2)))
          .withColumn("full_price",   col("full_price").cast(DecimalType(10,2)))
          .withColumn("quantity",     col("quantity").cast(IntegerType()))
          .select("purchase_id", "client_id", "product_id", "quantity", "price", "full_price", "purchased_at")
          .dropDuplicates(["purchase_id"])
    )

    df_clients_keyed = df_clients.select(col("client_id").alias("c_client_id"))
    df_products_keyed = df_products.select(
        col("product_id").alias("p_product_id"),
        col("seller_id")
    )

    joined = (
        df_purchases.alias("p")
          .join(
            df_clients_keyed,
            col("p.client_id") == col("c_client_id"),
            how="left"
          )
          .withColumn("client_id_final", col("c_client_id").cast(IntegerType()))

          .join(
            df_products_keyed,
            col("p.product_id") == col("p_product_id"),
            how="left"
          )
          .select(
              col("purchase_id"),
              col("client_id_final").alias("client_id"),
              col("p.product_id").alias("product_id"),
              col("seller_id"),
              col("quantity"),
              col("price"),
              col("full_price"),
              col("p.purchased_at").alias("purchased_at")
          )
    )

    fact_df = joined.select(
        "purchase_id",
        "client_id",
        "product_id",
        "seller_id",
        "quantity",
        "price",
        "full_price",
        "purchased_at"
    )
    write_fact_to_postgres(fact_df, "fact_purchases")

    spark.stop()

if __name__ == "__main__":
    main()
