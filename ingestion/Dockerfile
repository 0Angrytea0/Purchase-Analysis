FROM bitnami/spark:3.3.1
WORKDIR /app
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion/src/ ./src/
COPY drivers/*.jar /opt/bitnami/spark/jars/
COPY ingestion/config/core-site.xml /opt/bitnami/spark/conf/core-site.xml
ENTRYPOINT ["sh", "-c", "\
  while true; do \
    spark-submit --master spark://spark-master:7077 \
      --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar,/opt/bitnami/spark/jars/iceberg-spark3-runtime-0.13.2.jar \
      src/batch_ingest.py && \
    echo \"[info] Успешный выгрузка в Iceberg — ждём 24 часа...\"; \
    sleep 86400; \
  done"]
