FROM flink:1.19-scala_2.12-java11

# Install JDK (required for pemja/PyFlink), Python and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        python3 \
        python3-pip \
        python3-dev \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for pemja
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Install PyFlink and dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.19.0 \
    confluent-kafka \
    fastavro \
    requests \
    clickhouse-driver

# Copy S3 plugin for MinIO compatibility
RUN mkdir -p /opt/flink/plugins/s3 && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3/
