FROM flink:1.20.0-scala_2.12-java17

# Iceberg's FlinkCatalogFactory unconditionally touches org.apache.hadoop.conf.Configuration
# at runtime, and Flink resolves org.apache.hadoop.* classes via its own (parent) classloader
# regardless of what's bundled in the user jar. These lightweight "client" jars (not the full
# Hadoop distribution) put that class on Flink's own classpath.
RUN curl -fSL -o /opt/flink/lib/hadoop-client-api-3.3.6.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar && \
    curl -fSL -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

WORKDIR /opt/flink-job

COPY target/sensor-flink-job-1.0-SNAPSHOT.jar /opt/flink-job/
