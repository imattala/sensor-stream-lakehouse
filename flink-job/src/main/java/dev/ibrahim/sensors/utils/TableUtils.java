package dev.ibrahim.sensors.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableUtils {
    public static void recreateTable(StreamTableEnvironment tableEnv, String createStmt) {
        tableEnv.executeSql(createStmt);
    }

    public static void insertData(StreamTableEnvironment tableEnv, String insertStmt) {
        tableEnv.executeSql(insertStmt);
    }
}
