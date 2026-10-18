# CareerPulse

**Real-Time Job Posting Change & Hiring Trend Analytics**

CareerPulse watches a stream of job-posting events and works out how hiring is changing. Each event is classified as new, modified, duplicate or closed by comparing it with the job's stored state. The pipeline keeps every job's history and produces hiring-trend analytics, a top-k trending-company ranking and a demand forecast.

Stack: Python · Apache Kafka · PySpark Structured Streaming · HBase · Parquet · DuckDB · Spark SQL · Spark RDD · Spark MLlib · Streamlit

## Architecture

```text
 Sources                         Ingest                  Stream processing (Spark Structured Streaming)
 Job-event simulator ─┐                                  parse JSON → validate → standardise → enrich
 (nested requirements,├─► Kafka job-events ─────────────► (category, skills from free text)
  free-text desc.)    │   partitioned log,                      │                     │
 Greenhouse / Lever ──┘   keyed by job_id         foreachBatch, parallel per job_id   sliding event-time windows
 (ingestion --kafka)                                            │                     │
             ┌────────────────────┬─────────────────────┬───────┘          ┌──────────┴──────────┐
             ▼                    ▼                     ▼                  ▼                     ▼
      Cold storage         HBase (wide column)    Analytical store    window_counts      window_category
      raw/ (every msg)     job_history,           Parquet events/,    (+ HLL distinct)   (new, closed, avg salary)
             │             company_stats          quarantine/                │                     │
             │ batch job                                │                   │   surge detection   │
             └──► data-quality report ──► Spark SQL · Spark RDD · MLlib     ▼                     ▼
                                          (analytics/*.parquet)       Monitoring & notification: alerts/ +
                                                     │                Kafka job-alerts + webhook
                                                     ▼                       │
                                  DuckDB ──► Streamlit dashboard     alert_consumer (real-time app)
```

See [docs/architecture.md](./docs/architecture.md) for the data model and design decisions, and [docs/requirements-coverage.md](./docs/requirements-coverage.md) for how each course requirement is met and demonstrated.

## What is implemented

| Requirement | Implementation |
|---|---|
| Semi-structured and unstructured data | JSON events with a nested `requirements` object (skills array, education) and a free-text `description` ([src/generator/job_generator.py](./src/generator/job_generator.py)); a stateful simulator produces realistic lifecycles, inconsistent spellings and optional malformed records |
| Streaming data | Kafka producer keyed by `job_id` ([src/streaming/kafka_producer.py](./src/streaming/kafka_producer.py)) → Structured Streaming ([src/streaming/spark_kafka_consumer.py](./src/streaming/spark_kafka_consumer.py)) |
| ETL and validation | Spark-native schema, missing-value, type, event and timestamp checks; standardisation of whitespace, case, timestamps (UTC), employment type and work mode; title → category; skills extracted from free text and alias-normalised (`py` → python, `k8s` → kubernetes). Rejected records go to a Parquet quarantine with an error code |
| Change detection | Each event is compared with the job's HBase state to give NEW / MODIFIED (with changed fields) / CLOSED / DUPLICATE ([src/streaming/change_detection.py](./src/streaming/change_detection.py)) |
| HBase | `job_history` stores versioned `info` cells and a wide `events:<epoch_ms>#<seq>` history row; `company_stats` holds atomic counters ([src/storage/hbase_store.py](./src/storage/hbase_store.py)) |
| Cold storage + analytical store | Raw archive of every incoming message, date-partitioned processed events, quarantine, window aggregates and alerts, all Parquet and written atomically ([src/storage/parquet_store.py](./src/storage/parquet_store.py)); a Spark batch job replays the raw archive into a daily data-quality report ([src/analytics/raw_quality.py](./src/analytics/raw_quality.py)) |
| Time-series / streaming windows | Watermarked sliding windows (5 min every 1 min) over event time: counts with HyperLogLog++ distinct jobs and companies per event type, and per-category new postings, closures, average salary and active companies; timestamped HBase cell versions |
| Monitoring & notification | Hiring-surge alerts (window count vs Poisson-floored baseline) and data-quality alerts (rejection rate), de-duplicated by alert ID, written to the lake, published to the `job-alerts` Kafka topic and an optional webhook ([src/streaming/alerts.py](./src/streaming/alerts.py)); `alert_consumer` is a real-time app tailing the topic |
| Spark SQL | Hiring trends by company, location and category; average salary by category and by skill; most requested skills (`explode` over the skills array); work-mode and employment-type mix; new jobs in the last N minutes; modified and closed counts ([src/analytics/spark_analytics.py](./src/analytics/spark_analytics.py)) |
| Spark RDD | Top-k companies and top-k *trending* companies via map → filter → reduceByKey → takeOrdered |
| ML | Spark MLlib linear regression forecasting next-bucket NEW postings per category from lag features, compared against a naive baseline ([src/analytics/demand_model.py](./src/analytics/demand_model.py)) |
| Near-duplicate detection | Spark MLlib MinHash LSH over character shingles finds open postings that are near-duplicates (including cross-source re-posts) without an O(n²) all-pairs scan ([src/analytics/near_duplicates.py](./src/analytics/near_duplicates.py)) |
| Distributed, replay-safe processing | Micro-batches are repartitioned by `job_id` and processed on executors in parallel (one writer per job). Deterministic event IDs and an HBase applied-event ledger make crash replays exactly-once in effect. See [architecture §11](./docs/architecture.md#11-concurrency-parallelism-and-performance) |
| DuckDB | Dashboard query layer over the Parquet lake ([src/analytics/duckdb_queries.py](./src/analytics/duckdb_queries.py)) |
| Verification | `scripts/verify_pipeline.py` reconciles Kafka offsets, raw archive, processed and quarantined records (nothing lost, nothing duplicated) and checks HBase state against the lake |
| Dashboard | Streamlit **Streaming analytics** page: alerts, live feed, KPIs, streaming and category windows, skills, trends, quarantine and cold-storage stats, Spark outputs, HBase job lookup |

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
docker compose --profile pipeline up -d --build        # Kafka + HBase + Spark streaming job + alert consumer
python -m src.streaming.kafka_producer --backfill-hours 2 --count 6000 --surge-title "Data Engineer" --stream --rate 10 --invalid-rate 0.03
docker logs -f careerpulse-alert-consumer                     # alerts arriving in real time
docker compose --profile analytics run --rm spark-analytics   # Spark SQL + RDD + MLlib + cold-storage batch job
python scripts/verify_pipeline.py --kafka localhost:9092 --hbase-mode thrift   # reconciliation
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
  generator/job_generator.py      job events + stateful lifecycle simulator (enriched fields, surges)
  streaming/kafka_producer.py     Kafka producer (batch, backfill, continuous) + topic creation
  streaming/spark_kafka_consumer.py  Structured Streaming job (ETL, foreachBatch, windows, alerts)
  streaming/etl.py                record-level clean/validate/standardise/enrich rules
  streaming/change_detection.py   NEW / MODIFIED / CLOSED / DUPLICATE detection, replay-safe
  streaming/pipeline.py           micro-batch: raw archive -> detection -> HBase -> Parquet
  streaming/alerts.py             surge + data-quality alerts -> lake, Kafka, webhook
  streaming/alert_consumer.py     real-time app tailing the job-alerts topic
  storage/hbase_store.py          HBase model (happybase or embedded emulator)
  storage/parquet_store.py        Parquet lake (raw, events, quarantine, windows, alerts)
  analytics/spark_analytics.py    Spark SQL + RDD analytics
  analytics/demand_model.py       Spark MLlib demand forecast
  analytics/near_duplicates.py    Spark MLlib MinHash LSH near-duplicate postings
  analytics/raw_quality.py        batch job over cold storage: daily data quality
  analytics/run_analytics.py      batch analytics CLI
  analytics/duckdb_queries.py     DuckDB queries for the dashboard
  ingestion/                      Greenhouse / Lever ingestion into SQLite
  dashboard/                      Streamlit app
scripts/run_pipeline.py           local end-to-end run
scripts/verify_pipeline.py        reconciliation: Kafka, raw, processed, quarantine, HBase
scripts/benchmark.py              throughput, scaling and latency benchmarks
docker/spark/                     Linux Spark image
compose.yaml                      Kafka, HBase, Spark streaming, alert consumer, analytics
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
