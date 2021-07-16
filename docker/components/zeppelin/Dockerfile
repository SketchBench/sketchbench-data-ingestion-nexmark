FROM docker.io/apache/zeppelin:0.9.0

ARG SPARK_VERSION=3.0.2
ARG HADOOP_VERSION=3.2

ENV SPARK_HOME=/opt/spark
ENV SPARKSUBMITOPTIONS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.2"
USER 0

# Spark with HADOOP
RUN mkdir -p /tmp/spark && \
    curl --retry 2 -L -s -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /tmp/spark && \
    mv /tmp/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm -rf /tmp/spark*

USER 1000
