package dev.ibrahim.sensors.job;

import dev.ibrahim.sensors.ddl.iceberg.SensorIcebergDDL;
import dev.ibrahim.sensors.ddl.kafka.SensorKafkaDDL;
import dev.ibrahim.sensors.utils.TableUtils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class SensorPipelineJob extends FlinkJobBase {

    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = createEnvironment();
        Properties kafkaProps = loadKafkaProps();
        Properties icebergProps = loadIcebergProps();

        createCatalog(tableEnv, icebergProps);

        String bs = kafkaProps.getProperty("bootstrap.servers");
        String sr = kafkaProps.getProperty("schema.registry.url");

        // ── Kafka source ──────────────────────────────────────────────────
        TableUtils.recreateTable(tableEnv, SensorKafkaDDL.rawSource(bs, sr));

        // ── Iceberg sinks ─────────────────────────────────────────────────
        TableUtils.recreateTable(tableEnv, SensorIcebergDDL.createRawTable());
        TableUtils.recreateTable(tableEnv, SensorIcebergDDL.createAggTable());

        // ── Raw passthrough + 1-min windowed aggregation ─────────────────
        TableUtils.insertAll(tableEnv,
                SensorIcebergDDL.insertRawToIceberg(),
                SensorIcebergDDL.insertAggToIceberg());
    }
}
