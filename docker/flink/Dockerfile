FROM flink:1.19-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir apache-flink==1.19.0
RUN pip3 install --no-cache-dir \
    confluent-kafka \
    fastavro \
    requests \
    clickhouse-driver

# Kafka SQL connector
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar

# Avro + Schema Registry connector
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.0/flink-sql-avro-confluent-registry-1.19.0.jar

# ClickHouse Flink connector
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/com/clickhouse/flink/flink-connector-clickhouse-1.17/0.1.3/flink-connector-clickhouse-1.17-0.1.3-all.jar

# S3 plugin for MinIO checkpoints
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

WORKDIR /opt/flink