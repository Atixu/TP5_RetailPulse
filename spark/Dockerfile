# Spark 3.5 (dropDuplicatesWithinWatermark) + connecteur Kafka + pilote JDBC PostgreSQL.
# Les jars sont téléchargés au build : aucun accès réseau (Ivy) nécessaire au démarrage.
FROM apache/spark:3.5.6-python3

ARG SPARK_VERSION=3.5.6
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; cd /opt/spark/jars; \
    for url in \
      "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "$MAVEN/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
      "$MAVEN/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    do wget -q "$url"; done

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/retailpulse
COPY transforms.py sink.py streaming_job.py ./
COPY tests ./tests

RUN mkdir -p /checkpoints && chown -R spark:spark /checkpoints /opt/retailpulse
USER spark

ENV PYTHONUNBUFFERED=1 SPARK_MASTER=local[2] SPARK_DRIVER_MEMORY=1g \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
CMD ["sh", "-c", "/opt/spark/bin/spark-submit --master \"$SPARK_MASTER\" --driver-memory \"$SPARK_DRIVER_MEMORY\" --conf spark.ui.port=4040 /opt/retailpulse/streaming_job.py"]
