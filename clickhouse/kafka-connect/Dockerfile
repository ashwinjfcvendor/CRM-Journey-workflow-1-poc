# kafka-connect/Dockerfile
FROM confluentinc/cp-kafka-connect:7.5.0

# Switch to root to install required tools
USER root

# Install required tools
RUN yum install -y wget unzip && rm -rf /var/cache/yum

# Download and unzip ClickHouse Kafka Connect plugin
RUN yum install -y wget unzip && rm -rf /var/cache/yum && \
    wget https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.1.3/clickhouse-kafka-connect-v1.1.3.zip && \
    unzip clickhouse-kafka-connect-v1.1.3.zip

# Switch back to the non-root user for security
USER appuser

# Run the base image's entrypoint script
CMD ["/etc/confluent/docker/run"]
