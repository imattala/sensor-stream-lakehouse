"""Proves the GE suites actually catch bad data, rather than just running.

sensor_readings_raw no longer contains anomalies by the time GE would see it
(M4's DLQ routing already strips them), so this validates a synthetic batch
built to mirror generator/generate.py's exact anomaly shapes (null value,
out-of-range spike, duplicate reading_id, invalid metric) against the same
suite definitions used in run_checkpoint.py, and asserts the specific
expectations meant to catch each anomaly actually fail.
"""
import sys
from datetime import datetime, timedelta, timezone

import great_expectations as gx
import pandas as pd

from suites import build_agg_suite, build_raw_suite

# A GE context must exist before any ExpectationSuite is constructed.
CONTEXT = gx.get_context(mode="ephemeral")


def validate(df: pd.DataFrame, suite: gx.ExpectationSuite, name: str):
    data_source = CONTEXT.data_sources.add_pandas(f"{name}_ds")
    asset = data_source.add_dataframe_asset(name="test_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("test_batch")
    return batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)


def failed_expectation_types(result) -> set:
    return {r.expectation_config.type for r in result.results if not r.success}


def check(condition: bool, message: str):
    status = "OK" if condition else "FAIL"
    print(f"  [{status}] {message}")
    return condition


def test_raw_suite_catches_anomalies() -> bool:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = pd.DataFrame([
        # clean row
        {"reading_id": "r1", "device_id": "device-001", "metric": "TEMPERATURE",
         "value": 22.0, "unit": "celsius", "event_time": now},
        # null_value anomaly
        {"reading_id": "r2", "device_id": "device-002", "metric": "TEMPERATURE",
         "value": None, "unit": "celsius", "event_time": now},
        # out_of_range_spike anomaly (TEMPERATURE hi=30, spike well beyond)
        {"reading_id": "r3", "device_id": "device-003", "metric": "TEMPERATURE",
         "value": 180.0, "unit": "celsius", "event_time": now},
        # duplicate anomaly - same reading_id delivered twice
        {"reading_id": "r4", "device_id": "device-004", "metric": "HUMIDITY",
         "value": 40.0, "unit": "percent", "event_time": now},
        {"reading_id": "r4", "device_id": "device-004", "metric": "HUMIDITY",
         "value": 40.0, "unit": "percent", "event_time": now},
    ])

    result = validate(df, build_raw_suite(freshness_minutes=60), "raw")
    failed = failed_expectation_types(result)
    print("test_suite_raw_suite_catches_anomalies:")
    ok = True
    ok &= check(not result.success, "overall validation reports failure")
    ok &= check("expect_column_values_to_not_be_null" in failed, "null_value anomaly caught")
    ok &= check("expect_column_values_to_be_between" in failed, "out_of_range_spike anomaly caught")
    ok &= check("expect_column_values_to_be_unique" in failed, "duplicate reading_id caught")
    return ok


def test_raw_suite_catches_freshness_gap() -> bool:
    stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=5)
    df = pd.DataFrame([
        {"reading_id": "r1", "device_id": "device-001", "metric": "TEMPERATURE",
         "value": 22.0, "unit": "celsius", "event_time": stale},
    ])

    result = validate(df, build_raw_suite(freshness_minutes=60), "raw_freshness")
    failed = failed_expectation_types(result)
    print("test_raw_suite_catches_freshness_gap:")
    ok = True
    ok &= check(not result.success, "overall validation reports failure")
    ok &= check("expect_column_max_to_be_between" in failed, "stale batch (freshness gap) caught")
    return ok


def test_agg_suite_catches_anomalies() -> bool:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = pd.DataFrame([
        # clean row
        {"device_id": "device-001", "metric": "TEMPERATURE", "window_start": now - timedelta(minutes=1),
         "window_end": now, "avg_value": 22.0, "min_value": 20.0, "max_value": 24.0, "reading_count": 30},
        # min > avg (impossible / corrupted aggregate)
        {"device_id": "device-002", "metric": "HUMIDITY", "window_start": now - timedelta(minutes=1),
         "window_end": now, "avg_value": 40.0, "min_value": 50.0, "max_value": 60.0, "reading_count": 20},
        # duplicate window key
        {"device_id": "device-003", "metric": "BATTERY", "window_start": now - timedelta(minutes=1),
         "window_end": now, "avg_value": 80.0, "min_value": 70.0, "max_value": 90.0, "reading_count": 15},
        {"device_id": "device-003", "metric": "BATTERY", "window_start": now - timedelta(minutes=1),
         "window_end": now, "avg_value": 81.0, "min_value": 71.0, "max_value": 91.0, "reading_count": 15},
    ])

    result = validate(df, build_agg_suite(freshness_minutes=60), "agg")
    failed = failed_expectation_types(result)
    print("test_agg_suite_catches_anomalies:")
    ok = True
    ok &= check(not result.success, "overall validation reports failure")
    ok &= check("expect_column_pair_values_a_to_be_greater_than_b" in failed, "min > avg anomaly caught")
    ok &= check("expect_compound_columns_to_be_unique" in failed, "duplicate window key caught")
    return ok


def test_agg_suite_catches_freshness_gap() -> bool:
    # ExpectColumnMaxToBeBetween checks the max across the WHOLE batch, so a
    # gap only shows up when the entire batch is stale - a single stale row
    # mixed with fresh ones would correctly NOT fail this check.
    stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=5)
    df = pd.DataFrame([
        {"device_id": "device-004", "metric": "VIBRATION", "window_start": stale - timedelta(minutes=1),
         "window_end": stale, "avg_value": 2.0, "min_value": 1.0, "max_value": 3.0, "reading_count": 10},
    ])

    result = validate(df, build_agg_suite(freshness_minutes=60), "agg_freshness")
    failed = failed_expectation_types(result)
    print("test_agg_suite_catches_freshness_gap:")
    ok = True
    ok &= check(not result.success, "overall validation reports failure")
    ok &= check("expect_column_max_to_be_between" in failed, "stale batch (freshness gap) caught")
    return ok


if __name__ == "__main__":
    results = [
        test_raw_suite_catches_anomalies(),
        test_raw_suite_catches_freshness_gap(),
        test_agg_suite_catches_anomalies(),
        test_agg_suite_catches_freshness_gap(),
    ]
    if all(results):
        print("\nAll anomaly-detection checks passed.")
        sys.exit(0)
    print("\nSome anomaly-detection checks did NOT behave as expected.")
    sys.exit(1)
