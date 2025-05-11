FROM bitnami/spark:3.3.1
WORKDIR /app

COPY streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/src/ ./src/

COPY drivers/*.jar /opt/bitnami/spark/jars/

CMD ["spark-submit","--master","spark://spark-master:7077","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1","src/stream_process.py"]
