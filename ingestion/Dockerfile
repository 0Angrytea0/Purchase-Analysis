FROM bitnami/spark:3.3.1
WORKDIR /app

# Установим Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код batch-джобы
COPY src/ ./src/

# (Опционально) JDBC-драйвер для Postgres
# COPY drivers/postgresql.jar /opt/bitnami/spark/jars/

# Запуск spark-submit
CMD ["spark-submit","--master","spark://spark-master:7077","src/batch_ingest.py"]
