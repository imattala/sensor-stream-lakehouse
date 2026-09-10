package dev.ibrahim.sensors.ddl;

public class SensorAnomalyRules {

    // Mirrors generator/generate.py's METRICS ranges: null values and out-of-range
    // spikes are the anomalies this SQL can catch (late events still pass through,
    // since "late" isn't a property of the row itself).
    public static final String IS_MALFORMED =
            "`value` IS NULL\n" +
            "  OR (`metric` = 'TEMPERATURE' AND (`value` < 15.0 OR `value` > 30.0))\n" +
            "  OR (`metric` = 'HUMIDITY'    AND (`value` < 30.0 OR `value` > 70.0))\n" +
            "  OR (`metric` = 'VIBRATION'   AND (`value` < 0.0  OR `value` > 5.0))\n" +
            "  OR (`metric` = 'BATTERY'     AND (`value` < 20.0 OR `value` > 100.0))";
}
