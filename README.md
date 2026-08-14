# sensor-stream-lakehouse

A streaming data pipeline for synthetic IoT sensor data: Kafka → Flink SQL → Apache Iceberg, with a formal data-quality layer (Great Expectations) and observability (Prometheus/Grafana/OpenTelemetry).

Built as a portfolio project, one milestone at a time. See open issues/PRs for progress.

## Architecture

```
sensor-generator (Python, Avro)
        │
        ▼
   Kafka + Schema Registry  (KRaft mode)
        │
        ▼
   Flink SQL job (Java, StreamTableEnvironment)
     ├─ raw passthrough  ──────────────► Iceberg: sensor_readings_raw
     ├─ 1-min tumbling window agg ─────► Iceberg: sensor_readings_1m_agg
     └─ malformed / out-of-range ──────► Kafka: sensor_readings_dlq
        │                                        (JDBC catalog + MinIO/S3)
        ▼
  Great Expectations validation (reads Iceberg via PyIceberg → pandas)
     null-rate, range, freshness/gap, duplicate-key checks → JSON/HTML report
        │
        ▼
  Prometheus + Grafana + OTel Collector
     Kafka lag, Flink job health, + GE pass/fail pushed as custom metrics
```

## Status

Work in progress — see the [issues](../../issues) for the milestone breakdown.

- [x] M1 — Ingest: docker-compose + sensor generator
- [x] M2 — Stream to lake: Flink SQL raw passthrough to Iceberg
- [ ] M3 — Windowed aggregation
- [ ] M4 — Dead-letter handling
- [ ] M5 — Formal data quality (Great Expectations)
- [ ] M6 — Observability
- [ ] M7 — Polish

## Running it

```
docker compose up -d
```

(Full instructions land as each milestone completes.)

### Local UIs

| Service            | URL                          |
|---------------------|-------------------------------|
| Kafka UI (Kafbat)   | http://localhost:8085         |
| Flink dashboard     | http://localhost:8082         |
| MinIO console        | http://localhost:9001         |
| Schema Registry     | http://localhost:8081/subjects |

## License

MIT
