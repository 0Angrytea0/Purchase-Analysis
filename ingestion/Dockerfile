FROM bitnami/spark:3.3.1
WORKDIR /app

# Установим Python-зависимости
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код batch-джобы
COPY ingestion/src/ ./src/

# JDBC-драйверы
COPY drivers/*.jar /opt/bitnami/spark/jars/

# Запуск spark-submit
CMD ["spark-submit","--master","spark://spark-master:7077","src/batch_ingest.py"]
