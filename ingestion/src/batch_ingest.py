# ingestion/src/batch_ingest.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def build_spark_session():
    bucket = os.getenv("MINIO_BUCKET")
    return (
        SparkSession.builder
            .appName("BatchIngestIceberg")
            .config(
                "spark.jars",
                "/opt/bitnami/spark/jars/postgresql-42.6.0.jar,"
                "/opt/bitnami/spark/jars/iceberg-spark3-runtime-0.13.2.jar"
            )
            .config("spark.hadoop.hadoop.security.authentication", "simple")
            .config("spark.hadoop.hadoop.security.authorization", "false")
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.create.bucket", "true")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
            .config(
                "spark.sql.catalog.iceberg_catalog.warehouse",
                f"s3a://{bucket}/iceberg_warehouse"
            )
            .getOrCreate()
    )

def read_postgres_table(spark, table_name: str):
    url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    props = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    return spark.read.jdbc(url=url, table=table_name, properties=props)

def main():
    spark = build_spark_session()


    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.stage")

    df_clients = read_postgres_table(spark, "clients")
    df_sellers = read_postgres_table(spark, "sellers")

    df_raw_products = read_postgres_table(spark, "products") \
        .withColumn("created_at", to_timestamp(col("created_at")))

    df_stage_clients = df_clients.select(
        "client_id", "first_name", "last_name", "email", "signup_date"
    )

    df_stage_sellers = df_sellers.select(
        "seller_id", "first_name", "last_name", "company_name", "email", "created_at"
    )

    df_stage_products = df_raw_products.select(
        "seller_id", "name", "price", "quantity", "created_at"
    )

    df_stage_clients.writeTo("iceberg_catalog.stage.clients").createOrReplace()
    df_stage_sellers.writeTo("iceberg_catalog.stage.sellers").createOrReplace()
    df_stage_products.writeTo("iceberg_catalog.stage.products").createOrReplace()
    spark.stop()

if __name__ == "__main__":
    main()
