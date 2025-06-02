from spark_config import create_spark_session
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

def get_clickhouse_config():

    load_dotenv()
    return {
        "url": f"jdbc:clickhouse://{os.getenv('CLICKHOUSE_HOST', 'localhost')}:8123",
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

def process_iceberg_tables():

    spark = create_spark_session("IcebergToClickHouse")
    clickhouse_config = get_clickhouse_config()


    tables = {
        "clients": "purchase_analysis.clients_batch",
        "products": "purchase_analysis.products_batch", 
        "purchases": "purchase_analysis.purchases_batch",
        "sellers": "purchase_analysis.sellers_batch"
    }


    current_date = datetime.now().date()
    yesterday = (current_date - timedelta(days=1)).isoformat()

    for source_table, target_table in tables.items():
        print(f"Обработка таблицы {source_table}")
        

        if source_table == "purchases":
   
            df = (
                spark.read
                     .format("iceberg")
                     .load(f"iceberg_catalog.stage.{source_table}")
                     .where(f"date(purchased_at) = '{yesterday}'")
            )
        else:
  
            df = (
                spark.read
                     .format("iceberg")
                     .load(f"iceberg_catalog.stage.{source_table}")
            )


        df.write \
          .format("jdbc") \
          .options(**clickhouse_config) \
          .option("dbtable", target_table) \
          .mode("append") \
          .save()

        print(f"Таблица {source_table} успешно загружена в {target_table}")

if __name__ == "__main__":
    process_iceberg_tables()
