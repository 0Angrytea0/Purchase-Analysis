from pyspark.sql import SparkSession
import os

def create_spark_session(app_name: str):
    return (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")  
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar,"
                                "/opt/spark/jars/iceberg-spark3-runtime-0.13.2.jar,"
                                "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar")
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
            .config("spark.sql.catalog.iceberg_catalog.warehouse", f"s3a://{os.getenv('MINIO_BUCKET')}/iceberg_warehouse")
            .config("spark.executor.cores", "2")
            .config("spark.cores.max", "4")
            .getOrCreate()
    )
