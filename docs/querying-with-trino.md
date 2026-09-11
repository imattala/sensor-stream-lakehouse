# Querying the lakehouse with Trino

The Iceberg tables (`sensor_readings_raw`, `sensor_readings_1m_agg`, ...) can be
queried with real SQL via [Trino](https://trino.io), instead of one-off
PyIceberg/DuckDB scripts. This runs as a plain process on the host — it's not
part of `docker-compose.yml`, since it's a query tool for exploring the
lakehouse rather than something the pipeline itself depends on.

## Requirements

- Java 25 (Trino's hard minimum as of Trino 476). If your system Java is
  older, download a JDK 25 build (e.g.
  [Eclipse Temurin](https://adoptium.net/)) separately and point `JAVA_HOME`
  at it only when running Trino — no need to change your system default.
- `sensor-postgres` and `sensor-minio` running (`docker compose up -d postgres
  minio`). Kafka/Flink aren't needed just to query existing tables.

## Install

```bash
# Trino server
curl -fSL -o trino-server-476.tar.gz \
  https://repo1.maven.org/maven2/io/trino/trino-server/476/trino-server-476.tar.gz
tar xzf trino-server-476.tar.gz

# Trino CLI
curl -fSL -o trino-cli \
  https://repo1.maven.org/maven2/io/trino/trino-cli/476/trino-cli-476-executable.jar
chmod +x trino-cli
```

## Configure

Inside `trino-server-476/etc/`:

`node.properties`:
```properties
node.environment=dev
node.id=sensor-lakehouse-trino
node.data-dir=/path/to/trino-server-476/var
```

`jvm.config`: use the [default Trino template](https://trino.io/docs/current/installation/deployment.html#jvm-config).

`config.properties`:
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
```

`catalog/iceberg.properties` — points at the same JDBC catalog (Postgres) and
S3-compatible storage (MinIO) the Flink job uses:
```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://localhost:5432/iceberg_catalog
iceberg.jdbc-catalog.connection-user=iceberg
iceberg.jdbc-catalog.connection-password=iceberg
iceberg.jdbc-catalog.catalog-name=iceberg_catalog
iceberg.jdbc-catalog.default-warehouse-dir=s3://lakehouse/

fs.native-s3.enabled=true
s3.endpoint=http://localhost:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

`iceberg.jdbc-catalog.catalog-name` must match the `catalog_name` value Flink's
`JdbcCatalog` persisted in Postgres — check with:
```sql
SELECT DISTINCT catalog_name FROM iceberg_tables;
```

## Run

```bash
# start
JAVA_HOME=/path/to/jdk-25 /path/to/trino-server-476/bin/launcher start

# stop
JAVA_HOME=/path/to/jdk-25 /path/to/trino-server-476/bin/launcher stop

# connect
java -jar /path/to/trino-cli --server localhost:8080 --catalog iceberg --schema default
```

```sql
SHOW TABLES;
SELECT count(*) FROM sensor_readings_raw;
SELECT device_id, metric, window_start, avg_value, reading_count
FROM sensor_readings_1m_agg
ORDER BY window_start DESC
LIMIT 5;
```
