"""Runs the Great Expectations suites against the live Iceberg tables and
writes a JSON report plus HTML Data Docs under data_quality/gx/.

Usage:
    python3 run_checkpoint.py [--freshness-minutes N]

Requires sensor-postgres and sensor-minio (docker compose up -d postgres minio).
"""
import argparse
import json
import os
import sys

import great_expectations as gx

from suites import build_agg_suite, build_raw_suite

DATA_QUALITY_DIR = os.path.dirname(os.path.abspath(__file__))

CATALOG_PROPERTIES = {
    "uri": "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_catalog",
    "warehouse": "s3://lakehouse/",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
}
CATALOG_NAME = "iceberg_catalog"

TABLES = [
    ("sensor_readings_raw", "default.sensor_readings_raw", build_raw_suite),
    ("sensor_readings_1m_agg", "default.sensor_readings_1m_agg", build_agg_suite),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--freshness-minutes", type=int, default=60,
                         help="how stale the newest row is allowed to be")
    args = parser.parse_args()

    context = gx.get_context(mode="file", project_root_dir=DATA_QUALITY_DIR)
    data_source = context.data_sources.add_or_update_pandas("iceberg_lakehouse")

    overall_success = True
    report = {"tables": {}}

    for table_name, table_identifier, build_suite in TABLES:
        asset = data_source.add_iceberg_asset(
            name=f"{table_name}_asset",
            table_identifier=table_identifier,
            catalog_name=CATALOG_NAME,
            catalog_properties=CATALOG_PROPERTIES,
        )
        batch_def = asset.add_batch_definition(name=f"{table_name}_batch")

        suite = context.suites.add_or_update(build_suite(args.freshness_minutes))
        validation_definition = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(name=f"{table_name}_validation", data=batch_def, suite=suite)
        )
        checkpoint = context.checkpoints.add_or_update(
            gx.Checkpoint(name=f"{table_name}_checkpoint", validation_definitions=[validation_definition])
        )

        result = checkpoint.run()
        overall_success = overall_success and result.success
        report["tables"][table_name] = result.describe_dict()

        status = "PASSED" if result.success else "FAILED"
        print(f"[{status}] {table_name}")

    report["success"] = overall_success

    report_path = os.path.join(DATA_QUALITY_DIR, "report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nJSON report: {report_path}")

    data_docs_sites = context.build_data_docs()
    for site_name, url in data_docs_sites.items():
        print(f"HTML report ({site_name}): {url}")

    sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()
