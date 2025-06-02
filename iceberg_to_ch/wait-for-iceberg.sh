set -e

echo "Жду появления таблицы clients в Iceberg..."

ICEBERG_SQL_CONF="
  --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.iceberg_catalog.type=hadoop
  --conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://${MINIO_BUCKET}/iceberg_warehouse
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}
  --conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER}
  --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD}
  --conf spark.hadoop.fs.s3a.path.style.access=true
"

while true; do
    spark-sql --master spark://spark-master:7077 $ICEBERG_SQL_CONF \
      -e "SHOW TABLES IN iceberg_catalog.stage;" | grep -w clients && break
    sleep 10
done

echo "Таблица найдена! Запускаю процесс загрузки в ClickHouse"

exec spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.hadoop.security.authentication=simple \
      --conf spark.hadoop.security.authorization=false \
      --conf spark.jars.ivy=/home/spark/.ivy2 \
      --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.iceberg_catalog.type=hadoop \
      --conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://${MINIO_BUCKET}/iceberg_warehouse \
      --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
      --conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} \
      --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --jars /opt/bitnami/spark/jars/clickhouse-jdbc-0.4.6.jar,/opt/bitnami/spark/jars/postgresql-42.6.0.jar,/opt/bitnami/spark/jars/iceberg-spark3-runtime-0.13.2.jar \
      src/iceberg_to_clickhouse.py
