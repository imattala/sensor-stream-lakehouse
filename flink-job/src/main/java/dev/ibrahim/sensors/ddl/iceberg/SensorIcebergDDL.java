package dev.ibrahim.sensors.ddl.iceberg;

public class SensorIcebergDDL {

    public static String createRawTable() {
        return "CREATE TABLE IF NOT EXISTS `iceberg_catalog`.`default`.`sensor_readings_raw` (\n" +
                "  `reading_id` STRING,\n" +
                "  `device_id`  STRING,\n" +
                "  `metric`     STRING,\n" +
                "  `value`      DOUBLE,\n" +
                "  `unit`       STRING,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  `day`        STRING\n" +
                ") PARTITIONED BY (`day`) " +
                "WITH (\n" +
                "  'format-version' = '2',\n" +
                "  'write.format.default' = 'parquet',\n" +
                "  'write.distribution-mode' = 'hash',\n" +
                "  'write.upsert.enabled' = 'false'\n" +
                ")";
    }

    public static String createAggTable() {
        return "CREATE TABLE IF NOT EXISTS `iceberg_catalog`.`default`.`sensor_readings_1m_agg` (\n" +
                "  `device_id`      STRING,\n" +
                "  `metric`         STRING,\n" +
                "  `window_start`   TIMESTAMP(3),\n" +
                "  `window_end`     TIMESTAMP(3),\n" +
                "  `avg_value`      DOUBLE,\n" +
                "  `min_value`      DOUBLE,\n" +
                "  `max_value`      DOUBLE,\n" +
                "  `reading_count`  BIGINT,\n" +
                "  `day`            STRING\n" +
                ") PARTITIONED BY (`day`) " +
                "WITH (\n" +
                "  'format-version' = '2',\n" +
                "  'write.format.default' = 'parquet',\n" +
                "  'write.distribution-mode' = 'hash',\n" +
                "  'write.upsert.enabled' = 'false'\n" +
                ")";
    }

    public static String insertAggToIceberg() {
        return "INSERT INTO `iceberg_catalog`.`default`.`sensor_readings_1m_agg`\n" +
                "SELECT\n" +
                "  `device_id`,\n" +
                "  `metric`,\n" +
                "  `window_start`,\n" +
                "  `window_end`,\n" +
                "  AVG(`value`) AS `avg_value`,\n" +
                "  MIN(`value`) AS `min_value`,\n" +
                "  MAX(`value`) AS `max_value`,\n" +
                "  COUNT(*) AS `reading_count`,\n" +
                "  DATE_FORMAT(`window_start`, 'yyyy-MM-dd') AS `day`\n" +
                "FROM TABLE(\n" +
                "  TUMBLE(TABLE kafka_sensor_readings_source, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)\n" +
                ")\n" +
                "GROUP BY `device_id`, `metric`, `window_start`, `window_end`";
    }

    public static String insertRawToIceberg() {
        return "INSERT INTO `iceberg_catalog`.`default`.`sensor_readings_raw`\n" +
                "SELECT\n" +
                "  `reading_id`,\n" +
                "  `device_id`,\n" +
                "  `metric`,\n" +
                "  `value`,\n" +
                "  `unit`,\n" +
                "  `timestamp` AS `event_time`,\n" +
                "  DATE_FORMAT(`timestamp`, 'yyyy-MM-dd') AS `day`\n" +
                "FROM kafka_sensor_readings_source";
    }
}
