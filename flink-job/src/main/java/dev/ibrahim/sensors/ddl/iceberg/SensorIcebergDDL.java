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
