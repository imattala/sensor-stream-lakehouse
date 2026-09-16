# Data quality

A Great Expectations suite validating `sensor_readings_raw` and
`sensor_readings_1m_agg` directly from Iceberg (via GE's native Iceberg
asset, backed by PyIceberg) — no separate export step.

- `suites.py` — the expectation suites (single source of truth; rebuilt fresh
  on every run, nothing under `gx/` needs to be hand-edited or committed)
- `run_checkpoint.py` — validates the live tables, writes `report.json` and
  an HTML Data Docs site under `gx/uncommitted/data_docs/`
- `test_suite_detects_anomalies.py` — proves the suites actually catch bad
  data, using synthetic batches mirroring `generator/generate.py`'s anomaly
  types (raw data no longer contains real anomalies by the time it reaches
  here, since M4's DLQ routing already strips nulls/out-of-range values)

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Run

Requires `sensor-postgres` and `sensor-minio` running
(`docker compose up -d postgres minio` from the repo root).

```bash
.venv/bin/python3 run_checkpoint.py [--freshness-minutes N]   # default 60
.venv/bin/python3 test_suite_detects_anomalies.py
```

`run_checkpoint.py` exits non-zero if any table fails validation, so it can
be wired into a scheduler/CI step later.

## Known finding

`sensor_readings_raw`'s uniqueness check on `reading_id` currently fails
against live data: the generator injects a small rate of genuine duplicate
deliveries (`P_DUPLICATE`), and M4's DLQ routing only filters null/
out-of-range values, not duplicates. This is the suite correctly catching a
real gap rather than a bug in the suite - deduplication isn't in this
milestone's scope.
