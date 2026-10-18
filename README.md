# CareerPulse

**Real-Time Job Posting Change & Hiring Trend Analytics**

CareerPulse watches a stream of job-posting events and works out how hiring is changing. Each event is classified as new, modified, duplicate or closed by comparing it with the job's stored state. The pipeline keeps every job's history and produces hiring-trend analytics, a top-k trending-company ranking and a demand forecast.

Stack: Python · Apache Kafka · PySpark Structured Streaming · HBase · Parquet · DuckDB · Spark SQL · Spark RDD · Spark MLlib · Streamlit

## Architecture

```text
Python job-event simulator ──┐
Public board ingestion       ├──► Kafka topic job-events (keyed by job_id)
  (Greenhouse / Lever --kafka)┘            │
                                           ▼
                     PySpark Structured Streaming
        parse JSON → validate → clean → enrich (category) → watermark
                 │                                     │
   foreachBatch: change detection vs HBase      sliding event-time windows
                 │                                     │
      ┌──────────┴───────────┐                         │
      ▼                      ▼                         ▼
  HBase job_history     Parquet lake: events / quarantine / window_counts
  + company_stats                │
                                 ▼
          Spark SQL trends · Spark RDD top-k · MLlib demand forecast
                                 │  (analytics/*.parquet)
                                 ▼
                  DuckDB queries ──► Streamlit dashboard
```

See [docs/architecture.md](./docs/architecture.md) for the data model and design decisions.

## What is implemented

| Requirement | Implementation |
|---|---|
| Semi-structured data | JSON job events ([src/generator/job_generator.py](./src/generator/job_generator.py)); a stateful simulator produces realistic lifecycles and optional malformed records |
| Streaming data | Kafka producer keyed by `job_id` ([src/streaming/kafka_producer.py](./src/streaming/kafka_producer.py)) → Structured Streaming ([src/streaming/spark_kafka_consumer.py](./src/streaming/spark_kafka_consumer.py)) |
| ETL and validation | Spark-native schema, missing-value, type, event and timestamp checks; whitespace cleaning; title → category enrichment. Rejected records go to a Parquet quarantine with an error code |
| Change detection | Each event is compared with the job's HBase state to give NEW / MODIFIED (with changed fields) / CLOSED / DUPLICATE ([src/streaming/change_detection.py](./src/streaming/change_detection.py)) |
| HBase | `job_history` stores versioned `info` cells and a wide `events:<epoch_ms>#<seq>` history row; `company_stats` holds atomic counters ([src/storage/hbase_store.py](./src/storage/hbase_store.py)) |
| Parquet | Date-partitioned event lake, quarantine and window aggregates, written atomically ([src/storage/parquet_store.py](./src/storage/parquet_store.py)) |
| Time-series / streaming windows | Watermarked sliding windows (5 min every 1 min) over event time, with HyperLogLog++ distinct jobs and companies per window; timestamped HBase cell versions |
| Spark SQL | Hiring trends by company, location and category; average salary by category; new jobs in the last N minutes; modified and closed counts ([src/analytics/spark_analytics.py](./src/analytics/spark_analytics.py)) |
| Spark RDD | Top-k companies and top-k *trending* companies via map → filter → reduceByKey → takeOrdered |
| ML | Spark MLlib linear regression forecasting next-bucket NEW postings per category from lag features, compared against a naive baseline ([src/analytics/demand_model.py](./src/analytics/demand_model.py)) |
| Near-duplicate detection | Spark MLlib MinHash LSH over character shingles finds open postings that are near-duplicates (including cross-source re-posts) without an O(n²) all-pairs scan ([src/analytics/near_duplicates.py](./src/analytics/near_duplicates.py)) |
| Distributed, replay-safe processing | Micro-batches are repartitioned by `job_id` and processed on executors in parallel (one writer per job). Deterministic event IDs and an HBase applied-event ledger make crash replays exactly-once in effect. See [architecture §11](./docs/architecture.md#11-concurrency-parallelism-and-performance) |
| DuckDB | Dashboard query layer over the Parquet lake ([src/analytics/duckdb_queries.py](./src/analytics/duckdb_queries.py)) |
| Dashboard | Streamlit **Streaming analytics** page: live feed, KPIs, windows, trends, quarantine, Spark outputs, HBase job lookup |

Real public job boards (Greenhouse, Lever) can be ingested into SQLite and published into the same Kafka topic. See [docs/ingestion.md](./docs/ingestion.md).

## Quick start

### Local run (no Kafka or Docker)

This uses the same ETL, change detection, HBase (embedded emulator) and Parquet code as the streaming job, fed in micro-batches from the simulator:

```powershell
pip install -r requirements.txt
python scripts/run_pipeline.py --count 3000 --backfill-hours 24 --reset --spark
```

### Full streaming pipeline (Docker)

```powershell
docker compose --profile pipeline up -d --build        # Kafka + HBase + Spark streaming job
python -m src.streaming.kafka_producer --backfill-hours 24 --count 3000 --stream --rate 5 --invalid-rate 0.03
docker compose --profile analytics run --rm spark-analytics   # Spark SQL + RDD + MLlib
```

### Dashboard

```powershell
$env:CAREERPULSE_HBASE_MODE = "thrift"   # when the Docker HBase is running; omit for the local emulator
python -m streamlit run .\src\dashboard\app.py
```

The dashboard requires OIDC sign-in. See [docs/authentication-and-big-data.md](./docs/authentication-and-big-data.md).

Full instructions, including running Spark natively and on Windows, are in [docs/runbook.md](./docs/runbook.md).

## Example event

```json
{"job_id": "J1024", "company": "Google", "title": "Software Engineer", "location": "Bangalore",
 "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T16:30:00Z", "event": "NEW"}
```

## Dashboard workspace

- **Overview:** active listings from public-board ingestion, lifecycle totals, company distribution, recent changes.
- **Streaming analytics:** new jobs in the last 5 minutes, modified, closed, duplicate and rejected counts, a live event feed, streaming windows, trends by company, location and category, average salary by category, the validation quarantine, the Spark RDD top-k and trending companies, the MLlib demand forecast with error metrics, and an HBase job-history lookup.
- **Job explorer:** search, filter and export active ingested jobs.
- **Big Data operations:** pipeline stage overview, storage inventory, and ingestion and Spark batch controls (admin).
- **Pipeline control** (admin/analyst): run ingestion and inspect per-account scans.
- **Lifecycle events:** filterable audit trail of ingested NEW, MODIFIED and CLOSED events.

## Repository layout

```text
src/
  generator/job_generator.py      job events + stateful lifecycle simulator
  streaming/kafka_producer.py     Kafka producer (batch, backfill, continuous)
  streaming/spark_kafka_consumer.py  Structured Streaming job
  streaming/etl.py                record-level clean/validate/enrich rules
  streaming/change_detection.py   NEW / MODIFIED / CLOSED / DUPLICATE detection
  streaming/pipeline.py           micro-batch: detection -> HBase -> Parquet
  storage/hbase_store.py          HBase model (happybase or embedded emulator)
  storage/parquet_store.py        Parquet lake
  analytics/spark_analytics.py    Spark SQL + RDD analytics
  analytics/demand_model.py       Spark MLlib demand forecast
  analytics/run_analytics.py      batch analytics CLI
  analytics/duckdb_queries.py     DuckDB queries for the dashboard
  ingestion/                      Greenhouse / Lever ingestion into SQLite
  dashboard/                      Streamlit app
scripts/run_pipeline.py           local end-to-end run
docker/spark/                     Linux Spark image
compose.yaml                      Kafka, HBase, Spark streaming and analytics
```

## Performance

Measured with [scripts/benchmark.py](./scripts/benchmark.py); details and method are in [docs/benchmarks.md](./docs/benchmarks.md).

| Measure | Result |
|---|---|
| Local pipeline throughput | 244 → 1,979 events/s (8.1×) |
| Kafka producer throughput | 440 → 7,362 events/s (16.7×, pipelined sends) |
| Streaming handler scaling on real HBase | 673 → 2,881 events/s from 1 → 8 partitions (4.3×) |
| End-to-end latency, 10 s / 1 s trigger | p95 10.5 s / 5.6 s |
| Crash and replay audit | 7,880 Kafka records = 7,770 processed + 110 quarantined, 0 duplicates |

## Tests

```powershell
python -m pytest -q
```

The Spark tests run on in-memory DataFrames, so they need Java and PySpark but no Kafka broker or Hadoop winutils.

## Scope

This is the medium-scope version: simulated events plus optional public company boards. There is no large-scale crawler, microservice architecture, MinHash/LSH deduplication or deep-learning model.
