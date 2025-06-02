import os
from dotenv import load_dotenv
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from spark_config import create_spark_session

load_dotenv()

CLICKHOUSE_OPTIONS = {
    "url": f"jdbc:clickhouse://{os.getenv('CLICKHOUSE_HOST', 'clickhouse')}:8123",
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
    "dbtable": "purchase_analysis.purchases_stream",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def write_to_clickhouse(df, epoch_id):
    print(f"Writing batch {epoch_id} to ClickHouse")
    print("DataFrame schema:")
    df.printSchema()
    print("DataFrame content:")
    df.show(truncate=False)
    
    try:
        df.write \
            .format("jdbc") \
            .options(**CLICKHOUSE_OPTIONS) \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {epoch_id}")
    except Exception as e:
        print(f"Error writing to ClickHouse: {str(e)}")
        raise

def get_kafka_config():
    kafka_config = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": os.getenv("KAFKA_TOPIC_PURCHASES", "purchases"),
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }
    print("Kafka configuration:", kafka_config)
    return kafka_config

def process_kafka_stream():
    spark = create_spark_session("KafkaToClickHouse")
    
    print("Starting Kafka stream processing")
    print("Kafka config:", get_kafka_config())
    
    schema = StructType([
        StructField("purchase_id", IntegerType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("purchased_at", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("full_price", DecimalType(10, 2), True),
        StructField("quantity", IntegerType(), True)
    ])

    print("Starting to read from Kafka...")
    kafka_stream = (
        spark.readStream
             .format("kafka")
             .options(**get_kafka_config())
             .load()
    )

    print("Kafka stream schema:")
    kafka_stream.printSchema()

    print("Setting up JSON parsing...")
    parsed_stream = (
        kafka_stream
          .selectExpr("CAST(value AS STRING) as json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
          .withColumn("purchased_at", to_timestamp(col("purchased_at")))
    )

    print("Parsed stream schema:")
    parsed_stream.printSchema()

    query = (
        parsed_stream.writeStream
              .foreachBatch(write_to_clickhouse)
              .outputMode("append")
              .start()
    )

    print("Stream processing started")
    query.awaitTermination()

if __name__ == "__main__":
    process_kafka_stream()
