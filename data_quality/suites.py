"""Expectation suite definitions for the sensor lakehouse tables.

Kept separate from run_checkpoint.py so the same suite definitions can be
exercised in test_suite_detects_anomalies.py without touching the real
Iceberg tables.
"""
from datetime import datetime, timedelta, timezone

import great_expectations as gx

# Mirrors flink-job's SensorAnomalyRules / generator/generate.py's METRICS ranges.
METRIC_RANGES = {
    "TEMPERATURE": (15.0, 30.0),
    "HUMIDITY": (30.0, 70.0),
    "VIBRATION": (0.0, 5.0),
    "BATTERY": (20.0, 100.0),
}


def build_raw_suite(freshness_minutes: int) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="sensor_readings_raw_suite")

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="reading_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="reading_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="device_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="value"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="metric", value_set=list(METRIC_RANGES.keys())))

    for metric, (lo, hi) in METRIC_RANGES.items():
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column="value", min_value=lo, max_value=hi,
            row_condition=f'metric == "{metric}"', condition_parser="pandas",
        ))

    freshness_cutoff = datetime.now(timezone.utc) - timedelta(minutes=freshness_minutes)
    suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
        column="event_time", min_value=freshness_cutoff.replace(tzinfo=None), max_value=None))

    return suite


def build_agg_suite(freshness_minutes: int) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="sensor_readings_1m_agg_suite")

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="device_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="metric"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="window_start"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="reading_count", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="avg_value", column_B="min_value", or_equal=True))
    suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="max_value", column_B="avg_value", or_equal=True))
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["device_id", "metric", "window_start"]))

    freshness_cutoff = datetime.now(timezone.utc) - timedelta(minutes=freshness_minutes)
    suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
        column="window_end", min_value=freshness_cutoff.replace(tzinfo=None), max_value=None))

    return suite
