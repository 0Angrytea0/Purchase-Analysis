FROM apache/spark:3.3.1
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN echo "spark:x:1001:1001:Spark user:/home/spark:/bin/bash" >> /etc/passwd && \
    mkdir -p /home/spark && chown -R 1001:1001 /home/spark
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R 1001:1001 /home/spark/.ivy2
COPY drivers/postgresql-42.6.0.jar               /opt/spark/jars/
COPY drivers/iceberg-spark3-runtime-0.13.2.jar   /opt/spark/jars/
COPY drivers/clickhouse-jdbc-0.4.6.jar           /opt/spark/jars/
WORKDIR /app
COPY streaming/requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
ENV HOME=/home/spark
USER 1001
COPY streaming/src/ ./src/
COPY ingestion/config/core-site.xml /opt/spark/conf/
ENTRYPOINT ["bash", "-lc", "\
  sleep 15 && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages \
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
    --conf spark.hadoop.security.authentication=simple \
    --conf spark.hadoop.security.authorization=false \
    --conf spark.jars.ivy=/home/spark/.ivy2 \
    --jars /opt/spark/jars/postgresql-42.6.0.jar,\
/opt/spark/jars/iceberg-spark3-runtime-0.13.2.jar,\
/opt/spark/jars/clickhouse-jdbc-0.4.6.jar \
    src/kafka_to_clickhouse.py \
"]
