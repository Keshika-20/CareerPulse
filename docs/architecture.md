# CareerPulse Architecture

CareerPulse processes a continuous stream of job-posting events. It classifies each event against the job's stored state, keeps an operational history in HBase and an analytical history in Parquet, and serves hiring-trend analytics, a trending-company ranking and a demand forecast through Streamlit.

## 1. End-to-end flow

```text
 Sources                      Transport             Processing                         Storage                     Analytics / serving
 ───────                      ─────────             ──────────                         ───────                     ───────────────────
 JobEventSimulator  ─┐
  (stateful, demand  │                      Spark Structured Streaming
   cycles, malformed ├─► Kafka job-events ─► 1 parse JSON (all fields as strings)
   records)          │   key = job_id        2 validate: malformed / missing / event /
 Public boards ──────┘   3 partitions          salary / timestamp → validation_error
  ingestion --kafka                          3 clean whitespace, normalise event, cast
  (SQLite outbox)                            4 enrich: title → category
                                             5a foreachBatch ───────────────────────► HBase job_history, company_stats
                                                 change detection vs HBase           Parquet events/ (by event_date)
                                                                                     Parquet quarantine/
                                             5b watermark + sliding window ─────────► Parquet window_counts/
                                                                                              │
                                                     Spark SQL trends · RDD top-k · MLlib ◄───┘ ─► Parquet analytics/
                                                                                              │
                                                                     DuckDB over Parquet ─► Streamlit
```

## 2. Event model

Producer payload (semi-structured JSON):

```json
{"job_id": "J1024", "company": "Google", "title": "Software Engineer", "location": "Bangalore",
 "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T16:30:00Z", "event": "NEW"}
```

`experience` and `salary` may be null (public boards often omit them). `event_type` is accepted as an alias of `event`. Public-board events also carry `source` and salary-range fields, which are ignored downstream.

The producer's `event` field is a claim. Apart from `CLOSED`, it is overridden by change detection.

## 3. Kafka

- Topic `job-events`, 3 partitions. Messages are keyed by `job_id`, so every event of a posting lands on one partition and is consumed in order. Change detection depends on this ordering.
- Producers:
  - `src/streaming/kafka_producer.py`: batch, backfill over N hours, or a continuous stream at a fixed rate.
  - `src.ingestion.service --kafka`: public-board lifecycle events, queued in a SQLite outbox inside the scan transaction and marked published only after a broker acknowledgement (at-least-once).
- Compose exposes `localhost:9092` to host clients and `kafka:29092` to containers.

## 4. Structured Streaming job (`src/streaming/spark_kafka_consumer.py`)

### ETL and validation (`apply_etl`)

Every JSON field is parsed as a string, so type errors become explicit validation errors instead of silent nulls. The rules, checked in order:

| Code | Rule |
|---|---|
| `malformed_json` | not a JSON object (via `columnNameOfCorruptRecord`) |
| `missing_fields:<list>` | `job_id`, `company`, `title`, `location`, `timestamp` or `event` null/blank |
| `invalid_event` | event not in NEW / MODIFIED / CLOSED / DUPLICATE |
| `invalid_salary` | present but not a finite non-negative number (`try_cast`, ANSI-safe) |
| `invalid_timestamp` | not parseable as a timestamp |

Cleaning collapses whitespace, trims text, uppercases the event and casts salary and event time. Enrichment derives `category` from the title with ordered regex rules (`CATEGORY_RULES` in `src/common/models.py`), which compile both to Python `re` and to Spark `rlike`.

`src/streaming/etl.py` implements the same rules in Python for the local runner. `tests/test_spark_jobs.py::test_spark_etl_matches_python_rules` checks the two stay identical.

### Change detection and storage (`foreachBatch`)

Each micro-batch is ordered by event time, then Kafka partition and offset, and handed to `StreamPipeline.process_valid_records`:

```text
detect_change(previous_state, record):
  claimed CLOSED            → DUPLICATE if already closed, else CLOSED
  no previous state         → NEW
  previously CLOSED         → NEW (re-posted)
  tracked field changed     → MODIFIED  (company, title, location, experience, salary)
  otherwise                 → DUPLICATE
```

State is read from HBase once per job per batch and kept current in memory. That way several events for one job inside a micro-batch see each other. Rejected rows are written to the quarantine dataset with their raw payload and error code. If an idle HBase Thrift connection has dropped, the handler reconnects once and retries the batch.

### Streaming windows

Valid events are watermarked on event time (default 10 minutes) and counted per claimed event type over 5-minute windows sliding every minute, in update mode. Results are upserted into `window_counts/window_counts.parquet`.

## 5. HBase data model (`src/storage/hbase_store.py`)

```text
Table job_history                                     row key = job_id
  info:company  info:title  info:location  info:experience  info:salary  info:category
  info:status   info:first_seen  info:last_seen  info:last_event  info:last_event_time  info:fingerprint
      ↳ JSON-encoded values, cell timestamp = event time, VERSIONS = 10.
        Only changed cells are rewritten, so a cell's versions are its change history.
  events:<epoch_ms:013d>#<seq>   → JSON {event, changed_fields, salary, title, location, ...}
      ↳ one column per NEW / MODIFIED / CLOSED: a wide, time-ordered row per job

Table company_stats                                   row key = company
  stats:NEW  stats:MODIFIED  stats:CLOSED  stats:DUPLICATE   (atomic counters)
```

Wide-column design notes:

- Point lookups by `job_id` serve change detection.
- History is co-located with state, so one row read returns a job's whole lifecycle.
- Fixed-width timestamp qualifiers sort chronologically.
- Counters avoid read-modify-write races.

`HBaseJobStore` accepts any object implementing the small happybase API subset it uses:

- `happybase.Connection` to a real HBase Thrift server (`--hbase-mode thrift`, used in Docker).
- `LocalHBaseConnection`, an embedded emulation backed by SQLite. It models tables, column families, timestamped versions trimmed to each family's limit, row scans and counters, so the pipeline and tests run without a cluster.

## 6. Parquet lake (`src/storage/parquet_store.py`)

```text
data/lake/
  events/event_date=YYYY-MM-DD/part-b<batch>-<id>.parquet   processed events with detected + claimed event
  quarantine/event_date=YYYY-MM-DD/...                      rejected raw payloads + error code
  window_counts/window_counts.parquet                       streaming window aggregates
  analytics/*.parquet, summary.json                         Spark SQL / RDD / ML outputs
  _status/pipeline_status.json                              last batch + running totals
```

Schemas are explicit, so all-null columns keep their types. Each file is written to a hidden temporary name and atomically renamed, so concurrent DuckDB readers never see partial files. Hive-style `event_date` directories allow partition pruning.

## 7. Batch analytics (`src/analytics/`)

- **Spark SQL** (`spark_analytics.HIRING_TREND_QUERIES`):
  - event counts
  - NEW jobs in the last N minutes of event time
  - top companies by NEW
  - hourly trends by company, location and category
  - category × location hiring
  - average and median salary by category
  - modified, closed and duplicate counts per company
- **Spark RDD**:
  - `top_k_companies_rdd`: `map → reduceByKey → takeOrdered`.
  - `trending_companies_rdd`: `max` (action) to find the latest event, then `map → filter → reduceByKey → map → takeOrdered`. It compares NEW postings in the latest window with the previous one (`momentum = recent − previous`, plus growth %).
- **MLlib demand forecast** (`demand_model.py`):
  - Builds a zero-filled (category × time bucket) grid with lag-1/2/3 NEW counts, a rolling mean, the previous bucket's closures, average salary and hour-of-day sine/cosine.
  - Pipeline: `StringIndexer → OneHotEncoder → VectorAssembler → LinearRegression`.
  - Time-based 80/20 split; reports RMSE, MAE and R² against a naive "same as last bucket" baseline, then forecasts each category's next bucket. It refuses to train with fewer than 12 labelled rows.
- `run_analytics.py` runs all three and writes `analytics/*.parquet`. Timestamps are formatted in UTC before collecting, to avoid PySpark's implicit local-time conversion.

Spark reads the lake with `spark.read.parquet` when its Hadoop filesystem layer works. On Windows without winutils, it reads through pyarrow and Arrow instead.

## 8. Serving (`src/analytics/duckdb_queries.py`, `src/dashboard/`)

DuckDB queries the Parquet files in place, with no JVM or load step, so the Streamlit page can auto-refresh while the stream appends files. "New jobs in the last 5 minutes" uses `processed_at` (when the pipeline received the event). The Spark SQL variant measures back from the newest event time instead, which suits backfilled data.

HBase is read through the same store class: Thrift, or the emulator file opened read-only.

## 9. Runtime topology

| Component | Local runner | Docker (`--profile pipeline`) |
|---|---|---|
| Event source | simulator, in process | host producer → Kafka |
| Stream processing | `StreamPipeline` micro-batches | `spark-streaming` container (Structured Streaming, `local[2]`) |
| HBase | embedded emulator | `harisekhon/hbase:2.1` via Thrift |
| Lake | `./data/lake` | `./data/lake` (bind mount) |
| Analytics | native Spark | `spark-analytics` container or native |

The Spark image (`docker/spark/Dockerfile`) is Python 3.12 + OpenJDK 17 + PySpark 4.2. The Kafka connector (`spark-sql-kafka-0-10_2.13`, matching the PySpark version) is fetched at startup via `spark.jars.packages` and cached in a volume.

## 10. Delivery semantics and limits

- Kafka → Spark is checkpointed. A crash replays uncommitted micro-batches, so HBase and Parquet writes are at-least-once. A replayed event is re-classified against already-updated state (typically as DUPLICATE), and HBase counters may over-count by the replayed batch.
- `foreachBatch` collects each micro-batch to the driver for the HBase lookups. That's fine for this medium-scale project. A larger deployment would partition HBase access with `foreachPartition` and batch gets.
- Micro-batches create many small Parquet files. Periodic compaction would be the next step.
- Public-board ingestion also keeps its own SQLite snapshot store with closure-by-absence rules (see [ingestion.md](./ingestion.md)). The streaming pipeline treats those events like any other source.
