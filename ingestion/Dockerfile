FROM bitnami/spark:3.3.1
WORKDIR /app
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion/src/ ./src/
COPY drivers/*.jar /opt/bitnami/spark/jars/
COPY ingestion/config/core-site.xml /opt/bitnami/spark/conf/core-site.xml
ENTRYPOINT ["spark-submit","--master", "spark://spark-master:7077","--conf", "spark.jars.ivy=/tmp/.ivy2","src/batch_ingest.py"]
