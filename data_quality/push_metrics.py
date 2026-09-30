"""Pushes GE checkpoint pass/fail results as OTLP metrics to the OTel
Collector, so data-quality status shows up on the same Grafana dashboard as
Kafka/Flink infra health (M6).

This is a one-shot script, not a long-running process, so it uses a short
export interval and forces a flush via MeterProvider.shutdown() before
exiting rather than relying on the reader's periodic export to fire in time.
"""
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource


def push_checkpoint_results(results: dict, otlp_endpoint: str = "localhost:4317") -> None:
    exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(
        resource=Resource.create({"service.name": "sensor-lakehouse-data-quality"}),
        metric_readers=[reader],
    )
    meter = provider.get_meter("data_quality.ge_checkpoint")

    success_gauge = meter.create_gauge(
        "ge_checkpoint_success",
        description="1 if the Great Expectations checkpoint passed, 0 if it failed",
    )
    for table_name, success in results.items():
        success_gauge.set(1 if success else 0, attributes={"table": table_name})

    provider.shutdown()
