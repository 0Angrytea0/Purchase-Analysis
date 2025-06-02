FROM bitnami/spark:3.3.1

USER root
WORKDIR /app

COPY iceberg_to_ch/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY iceberg_to_ch/src/ ./src/

COPY drivers/clickhouse-jdbc-0.4.6.jar        /opt/bitnami/spark/jars/
COPY drivers/postgresql-42.6.0.jar             /opt/bitnami/spark/jars/
COPY drivers/iceberg-spark3-runtime-0.13.2.jar /opt/bitnami/spark/jars/

COPY ingestion/config/core-site.xml /opt/bitnami/spark/conf/

RUN echo "spark:x:1001:1001:Spark user:/home/spark:/bin/bash" >> /etc/passwd && \
    mkdir -p /home/spark && \
    mkdir -p /home/spark/.ivy2 && \
    chown -R 1001:1001 /home/spark
COPY iceberg_to_ch/wait-for-iceberg.sh wait-for-iceberg.sh
RUN chmod +x wait-for-iceberg.sh
RUN chown -R 1001:1001 /app
ENV HOME=/home/spark \
    PYTHONPATH=/app/src

USER 1001

ENTRYPOINT ["./wait-for-iceberg.sh"]
