FROM flink:1.19-scala_2.12

# Install Python and PyFlink dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install apache-flink==1.19.0 \
                 confluent-kafka \
                 fastavro \
                 requests

# Copy S3 plugin for MinIO compatibility
RUN mkdir -p /opt/flink/plugins/s3 && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3/
