package dev.ibrahim.sensors.job;

import dev.ibrahim.sensors.config.PropertiesLoader;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Properties;

public abstract class FlinkJobBase {

    protected static StreamTableEnvironment createEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Cluster-level execution.checkpointing.interval isn't picked up by SQL/Table jobs
        // submitted via the CLI, and Iceberg's sink only commits files at checkpoint
        // boundaries, so this must be enabled explicitly here.
        env.enableCheckpointing(30_000L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().set("table.exec.state.ttl", "2 h");
        return tableEnv;
    }

    protected static Properties loadKafkaProps() throws IOException {
        return PropertiesLoader.load("/opt/flink-job/config/kafka.properties");
    }

    protected static Properties loadIcebergProps() throws IOException {
        return PropertiesLoader.load("/opt/flink-job/config/iceberg.properties");
    }

    protected static void createCatalog(StreamTableEnvironment tableEnv, Properties p) {
        tableEnv.executeSql(String.format(
                "CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH ( " +
                        "'type'                = 'iceberg', " +
                        "'catalog-impl'        = 'org.apache.iceberg.jdbc.JdbcCatalog', " +
                        "'uri'                 = '%s', " +
                        "'jdbc.user'           = '%s', " +
                        "'jdbc.password'       = '%s', " +
                        "'warehouse'           = '%s', " +
                        "'io-impl'             = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                        "'s3.endpoint'         = '%s', " +
                        "'client.region'       = '%s', " +
                        "'s3.access-key-id'     = '%s', " +
                        "'s3.secret-access-key' = '%s', " +
                        "'s3.path-style-access' = 'true')",
                p.getProperty("uri"),
                p.getProperty("jdbc.user"),
                p.getProperty("jdbc.password"),
                p.getProperty("warehouse"),
                p.getProperty("s3.endpoint"),
                p.getProperty("s3.region"),
                p.getProperty("s3.access-key"),
                p.getProperty("s3.secret-key")));
    }
}
