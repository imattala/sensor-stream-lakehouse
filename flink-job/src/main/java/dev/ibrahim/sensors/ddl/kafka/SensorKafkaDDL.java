package dev.ibrahim.sensors.ddl.kafka;

public class SensorKafkaDDL {

    public static String rawSource(String bs, String sr) {
        return "CREATE TABLE IF NOT EXISTS kafka_sensor_readings_source (\n" +
                "  `reading_id` STRING,\n" +
                "  `device_id`  STRING,\n" +
                "  `metric`     STRING,\n" +
                "  `value`      DOUBLE,\n" +
                "  `unit`       STRING,\n" +
                "  `timestamp`  TIMESTAMP(3),\n" +
                "  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '30' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'sensor-readings',\n" +
                "  'properties.bootstrap.servers' = '" + bs + "',\n" +
                "  'properties.group.id' = 'flink-sensor-raw-source',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'avro-confluent',\n" +
                "  'avro-confluent.url' = '" + sr + "'\n" +
                ")";
    }
}
