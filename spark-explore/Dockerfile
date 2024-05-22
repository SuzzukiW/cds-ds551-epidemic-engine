FROM docker.io/bitnami/spark:3.2.1

# Download and install Kafka integration library
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar \
    && mv spark-sql-kafka-0-10_2.12-3.2.1.jar /opt/bitnami/spark/jars/