package dev.ibrahim.sensors.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableUtils {
    public static void recreateTable(StreamTableEnvironment tableEnv, String createStmt) {
        tableEnv.executeSql(createStmt);
    }

    public static void insertData(StreamTableEnvironment tableEnv, String insertStmt) {
        tableEnv.executeSql(insertStmt);
    }

    // Runs multiple INSERT statements as one Flink job (one StatementSet) instead of
    // separate executeSql calls, so sinks reading the same Kafka source share a single
    // scan/consumer group rather than each spinning up a competing consumer on it.
    public static void insertAll(StreamTableEnvironment tableEnv, String... insertStmts) {
        var stmtSet = tableEnv.createStatementSet();
        for (String insertStmt : insertStmts) {
            stmtSet.addInsertSql(insertStmt);
        }
        stmtSet.execute();
    }
}
