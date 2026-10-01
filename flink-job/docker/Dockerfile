FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B package -DskipTests

FROM flink:1.20.0-scala_2.12-java17

# Iceberg's FlinkCatalogFactory unconditionally touches org.apache.hadoop.conf.Configuration
# at runtime, and Flink resolves org.apache.hadoop.* classes via its own (parent) classloader
# regardless of what's bundled in the user jar. These lightweight "client" jars (not the full
# Hadoop distribution) put that class on Flink's own classpath.
RUN curl -fSL -o /opt/flink/lib/hadoop-client-api-3.3.6.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar && \
    curl -fSL -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# Prometheus metrics reporter - not bundled in the base image's opt/, must be added to lib/
# so Flink picks it up (reporters aren't loaded via the plugins/ mechanism).
RUN curl -fSL -o /opt/flink/lib/flink-metrics-prometheus-1.20.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.0/flink-metrics-prometheus-1.20.0.jar

WORKDIR /opt/flink-job

COPY --from=build /build/target/sensor-flink-job-1.0-SNAPSHOT.jar /opt/flink-job/
