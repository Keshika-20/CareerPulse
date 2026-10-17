# CareerPulse Runbook

This runbook describes the implementation currently present in the repository. It does not describe Kafka-to-Spark streaming, HBase persistence, or a distributed deployment; those integrations are not implemented.

## Prerequisites

- Python 3.10 or newer
- For optional Spark batch aggregation only: a compatible PySpark and Java installation
- Network access to the configured public Greenhouse/Lever company-board endpoints

Adapter, normalization, SQLite lifecycle, analytics, and dashboard-data tests use local fixtures and mocked HTTP responses; they do not need live API access or a JVM.

## Run the test suite

Run the focused non-Spark tests:

```powershell
python -m pytest .\tests\test_generator.py .\tests\test_ingestion_sources.py .\tests\test_ingestion_sqlite.py .\tests\test_ingestion_service.py .\tests\test_analytics.py -q
```

The tests in `tests\test_streaming.py` that exercise `process_batch()` create a real local Spark session. Run them separately when Java/PySpark is configured:

```powershell
python -m pytest .\tests\test_streaming.py -q
```

The full suite combines both groups:

```powershell
python -m pytest -q
```

## Configure public source accounts

From the repository root:

```powershell
Copy-Item .\config\ingestion.example.json .\data\ingestion.local.json
```

Edit the copied file and set `enabled` to `true` only for board accounts you are authorized to query. Use the documented Greenhouse board token or Lever site identifier and the company name. For Lever, `region` is `global` or `eu`. The copied file is under the Git-ignored `data/` directory; do not commit account configuration.

Configuration fields:

- `sqlite_path`: durable snapshots, lifecycle events, run stats, and optional raw pages
- `raw_retention_days`: defaults to 7; set to `0` to disable raw-response retention
- `request_timeout_seconds`: per-request timeout
- `max_retries`: bounded retries for rate limits, server errors, and network failures
- `spark`: settings used only by the explicit Spark batch option
- `sources`: configured Greenhouse and Lever company accounts

For source endpoint and consistency limitations, normalized fields, lifecycle rules, and retention details, see [ingestion documentation](./ingestion.md).

## Run ingestion

From the repository root:

```powershell
python -m src.ingestion.service --config .\data\ingestion.local.json
```

This runs source requests, normalizes valid listings, and atomically applies each complete source-account scan to SQLite. It reports per-account counts for new, modified, unchanged, and closed listings. A failed fetch or unverified scan is reported and does not advance that account's snapshot or absence evidence. The command exits nonzero if a configured account fails.

To additionally perform real Spark DataFrame aggregation over the lifecycle events created by each successful scan:

```powershell
python -m src.ingestion.service --config .\data\ingestion.local.json --spark-batch
```

This flag starts Spark explicitly. Spark startup or processing failures remain visible. The SQLite source-state transaction commits before the optional Spark batch step; a subsequent Spark failure does not undo that committed ingestion run.

## Run local demonstrations

Generate synthetic job events:

```powershell
python .\src\generator\job_generator.py --count 10 --output .\data\raw\job_events.jsonl
```

Run the existing local synthetic pipeline:

```powershell
python .\scripts\run_pipeline.py
```

That script uses synthetic events and the existing local Parquet/JSONL helper. It does not run the public-source ingestion service.

The Kafka producer is also separate:

```powershell
python .\src\streaming\kafka_producer.py --bootstrap-servers localhost:9092 --topic job_events --count 25
```

It can publish generated events when a broker is available; without `kafka-python`, it prints JSON to stdout. There is no CareerPulse Kafka consumer connected to the ingestion service.

## Run the dashboard

The dashboard reads active snapshots and lifecycle events from `.\data\processed\ingestion.sqlite3` if the database exists. To point it at another configured database:

```powershell
$env:CAREERPULSE_SQLITE_PATH = ".\data\processed\ingestion.sqlite3"
python -m streamlit run .\src\dashboard\app.py
```

When no database exists, the dashboard says it is showing synthetic demo data. It does not silently use demo records when an existing database is invalid.

The dashboard provides Overview, Job explorer, Pipeline control, and Lifecycle events views. The explorer supports text search and company/source/location filters, listing details, original-posting links, and CSV export. Pipeline control can launch the normal ingestion service using the selected configuration and shows recent per-account run statistics. The lifecycle view filters persisted events and supports CSV export.

The dashboard reads the SQLite database in read-only mode. It reports salary information only when the source provides it; unknown salary is not treated as zero, and salary ranges are not converted to an invented scalar or midpoint. The dashboard does not automatically discover all employers: it displays listings from configured and enabled public company boards only.

## Current integration boundaries

- Greenhouse and Lever public company-board fetching, normalization, and per-account SQLite lifecycle snapshots are implemented.
- Spark is used only for explicit local batch event aggregation. No Structured Streaming source/sink is wired.
- Kafka producer, in-memory HBase-shaped storage, and Parquet helper remain independent examples and are not downstream sinks for the public ingestion service.
- No global job search, LinkedIn/Indeed access, ML prediction, or production deployment is implemented.
