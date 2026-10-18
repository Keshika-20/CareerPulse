# CareerPulse Architecture

CareerPulse processes a continuous stream of job-posting events. It classifies each event against the job's stored state, archives every raw message, keeps an operational history in HBase and an analytical history in Parquet, raises hiring-surge and data-quality alerts, and serves hiring-trend analytics, a trending-company ranking and a demand forecast through Streamlit. [requirements-coverage.md](./requirements-coverage.md) maps each course requirement to this design.

## 1. End-to-end flow

```text
 Sources                      Ingest                Stream processing                  Storage / outputs
 ───────                      ──────                ─────────────────                  ─────────────────
 JobEventSimulator  ─┐
  (lifecycles, nested│                      Spark Structured Streaming
   requirements, free├─► Kafka job-events ─► 1 parse JSON (fields as strings; nested object isolated)
   text, surges)     │   key = job_id        2 validate → validation_error code
 Public boards ──────┘   3 partitions        3 standardise: whitespace, case, UTC, employment type, work mode
  ingestion --kafka                          4 enrich: title → category; skills = declared + found in text
  (SQLite outbox)                            5a foreachBatch, repartitioned by job_id, in parallel:
                                                 raw archive ─────────────────────────► Parquet raw/ (cold storage)
                                                 change detection vs HBase ───────────► HBase job_history, company_stats
                                                                                        Parquet events/, quarantine/
                                                 rejection rate ──► data-quality alert ┐
                                             5b sliding windows per event type ───────► Parquet window_counts/
                                             5c sliding windows per category ─────────► Parquet window_category/
                                                 surge detection ──► hiring-surge alert┤
                                                                                       ▼
                                                     alerts/ + Kafka job-alerts + webhook ──► alert_consumer
 Batch: raw/ ──► same Spark ETL ──► data-quality report ─┐
        events/ ──► Spark SQL · RDD top-k · MLlib ───────┴──► Parquet analytics/ ──► DuckDB ──► Streamlit
```

## 2. Event model

Producer payload (semi-structured JSON):

```json
{"job_id": "J1024", "company": "Google", "title": "Software Engineer", "location": "Bangalore",
 "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T16:30:00Z", "event": "NEW"}
```

`experience` and `salary` may be null (public boards often omit them). `event_type` is accepted as an alias of `event`. Public-board events also carry `source` and salary-range fields, which are ignored downstream.

Simulated postings add optional, higher-dimensional attributes, deliberately messy so that standardisation has real work to do:

```json
{"employment_type": "full time", "work_mode": "WFH",
 "requirements": {"skills": ["PySpark", "py", "SQL"], "education": "B.Tech"},
 "description": "Build pipelines on Kafka and Apache Spark; Airflow is a plus."}
```

These never cause a rejection. After ETL they become `employment_type` (`Full-time`), `work_mode` (`Remote`), `education`, `skills` (`["airflow", "kafka", "python", "spark", "sql"]`) and `skill_count`.

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

Cleaning collapses whitespace, trims text, uppercases the event and casts salary and event time.

Standardisation maps employment-type and work-mode spellings to canonical values with a letters-only key (`FT`/`full time` → `Full-time`, `on-site` → `Onsite`, `WFH` → `Remote`; unknown → `Other`). A posting located in "Remote" with no work mode is inferred as `Remote`.

Enrichment derives `category` from the title with ordered regex rules (`CATEGORY_RULES`). It also builds `skills`: declared skills are lower-cased and alias-resolved (`SKILL_ALIASES`: `py` → python, `k8s` → kubernetes), unioned with skills recognised in the unstructured description (`SKILL_PATTERNS`), then de-duplicated and sorted. All rules live in `src/common/models.py`, and the patterns compile both to Python `re` and to Spark `rlike`.

`requirements` is captured as raw JSON text and parsed in a second, isolated `from_json`. A malformed optional object then only nulls that field instead of making Spark flag the whole record as corrupt.

`src/streaming/etl.py` implements the same rules in Python for the local runner. `tests/test_spark_jobs.py::test_spark_etl_matches_python_rules` checks the two stay identical.

### Change detection and storage (`foreachBatch`)

Each micro-batch is hash-repartitioned by `job_id` and sorted within partitions by event time, then Kafka partition and offset. Every partition runs `StreamPipeline.process_partition` on an executor in parallel, and only small summaries return to the driver. Because all events of a job land in one partition, each job has exactly one writer per batch (see §11):

```text
detect_change(previous_state, record):
  claimed CLOSED            → DUPLICATE if already closed, else CLOSED
  no previous state         → NEW
  previously CLOSED         → NEW (re-posted)
  tracked field changed     → MODIFIED  (company, title, location, experience, salary)
  otherwise                 → DUPLICATE
```

Each partition multi-gets the state and applied-event ledger of all its jobs in one HBase call, keeps state current in memory (so several events for one job in a batch see each other), then writes all mutations in bulk. Rejected rows go to the quarantine dataset with their raw payload and error code. Each Python worker reuses one HBase connection across tasks and batches. If an idle Thrift connection has dropped, the partition reconnects and retries, which is safe because every write is idempotent.

### Streaming windows and alerts

Valid events are watermarked on event time (default 10 minutes) and aggregated per claimed event type over 5-minute windows sliding every minute, in update mode. Each window row has the event `count` plus `unique_jobs` and `unique_companies`. Exact `countDistinct` is not supported in streaming aggregations, so the distinct counts use HyperLogLog++ (`approx_count_distinct`, 2% relative standard deviation, `DISTINCT_RSD`). Its state per window is a fixed-size sketch, whatever the event volume. Results are upserted into `window_counts/window_counts.parquet`. Window files written before the distinct columns existed still read, with nulls in those columns.

A third query aggregates per **category** over the same sliding windows: new postings, closures, all events, average advertised salary (NEW and MODIFIED) and active companies (HLL). This is the job-market counterpart of the brief's "active rides and average fare per neighbourhood in the last 5 minutes". Rows are upserted into `window_category/window_category.parquet`, with state versioned like the other window query (`category-v<N>` checkpoint).

**Monitoring and notification** (`src/streaming/alerts.py`):

- *Hiring surge*: after each category-window update, every window aligned to 5 minutes is compared with that category's previous 12 aligned windows. It alerts when the observed count is at least `max(5, 2 × mean, mean + 3σ)`, with σ² floored at the mean (Poisson), so quiet categories don't flag blips.
- *Data quality*: a micro-batch with more than 20% of at least 20 records rejected raises an alert.
- Alert IDs are deterministic (`surge:<category>:<window>`, `quality:<source>:<batch>`), so updates and replays never re-fire an alert.
- New alerts are appended to `alerts/`, published to the `job-alerts` Kafka topic (keyed by alert ID), and POSTed to an optional webhook (Slack-style `{"text": ...}`) to trigger workflows. Delivery failures are logged and never stop the pipeline.
- `src/streaming/alert_consumer.py` is the real-time app on the other end of the topic.

The job creates its Kafka topics before subscribing (`ensure_topics`). Spark's Kafka source fails on a missing topic, so a consumer started before the first producer would otherwise crash.

## 5. HBase data model (`src/storage/hbase_store.py`)

```text
Table job_history                                     row key = job_id
  info:company  info:title  info:location  info:experience  info:salary  info:category
  info:status   info:first_seen  info:last_seen  info:last_event  info:last_event_time  info:fingerprint
      ↳ JSON-encoded values, cell timestamp = event time, VERSIONS = 10.
        Only changed cells are rewritten, so a cell's versions are its change history.
  events:<epoch_ms:013d>#<event_id>   → JSON {event, changed_fields, salary, title, location, ...}
      ↳ one column per processed event (DUPLICATE included): a wide, time-ordered row per job,
        and the job's applied-event ledger for replay detection

Table company_stats                                   row key = company
  stats:NEW  stats:MODIFIED  stats:CLOSED  stats:DUPLICATE   (atomic counters)
```

Wide-column design notes:

- Point lookups by `job_id` serve change detection.
- History is co-located with state, so one row read returns a job's whole lifecycle.
- Fixed-width timestamp qualifiers sort chronologically.
- Counters use server-side atomic increments, aggregated per (company, event) per partition.
- Writes are batched: all cells of one event share its timestamp and go in one mutation batch, so an event costs one round trip instead of four (get, state put, history put, counter).

`HBaseJobStore` accepts any object implementing the small happybase API subset it uses:

- `happybase.Connection` to a real HBase Thrift server (`--hbase-mode thrift`, used in Docker).
- `LocalHBaseConnection`, an embedded emulation backed by SQLite. It models tables, column families, timestamped versions trimmed to each family's limit, row scans, batches and counters, so the pipeline and tests run without a cluster. Writes use `BEGIN IMMEDIATE` transactions, so counters stay atomic even when several processes share the file. A partition's writes commit in one transaction.

## 6. Parquet lake (`src/storage/parquet_store.py`)

```text
data/lake/
  raw/event_date=YYYY-MM-DD/part-<source>-b<batch>-p<partition>.parquet      cold storage: every message as received
  events/event_date=YYYY-MM-DD/part-<source>-b<batch>-p<partition>.parquet   processed events
  quarantine/event_date=YYYY-MM-DD/...                      rejected raw payloads + error code
  window_counts/window_counts.parquet                       window aggregates per event type
  window_category/window_category.parquet                   window metrics per category
  alerts/event_date=YYYY-MM-DD/...                          monitoring alerts
  analytics/*.parquet, summary.json                         Spark SQL / RDD / ML outputs
  _status/pipeline_status.json                              last batch + running totals
```

The raw archive stores the payload, its Kafka topic, partition, offset and broker timestamp. It is partitioned by broker time, not processing time, so a replayed batch lands in the same file. Every raw, processed and quarantined row carries the same `event_id`, which is what makes end-to-end reconciliation possible.

Schemas are explicit, so all-null columns keep their types. Each file is written to a hidden temporary name and atomically renamed, so concurrent DuckDB readers never see partial files. On Windows the rename retries briefly if a reader holds the target open. File names are deterministic per (source, batch, partition), so a replayed batch overwrites its own files instead of appending duplicates. The streaming source label includes the query ID from the checkpoint, so a new checkpoint never overwrites an old query's files. Hive-style `event_date` directories allow partition pruning.

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
- **Near-duplicate postings** (`near_duplicates.py`, Spark MLlib `MinHashLSH`):
  - Takes the latest state of every open job (DUPLICATE events ignored, jobs whose latest event is CLOSED dropped). It normalises company, title, location and experience (lower-case, punctuation stripped, `sr`/`jr`/`eng`/`mgr`/`dev`/`ml` expanded) into character 3-gram shingles, hashed into a binary `HashingTF` vector.
  - Comparing every pair is O(n²). Instead, MinHash LSH with 5 hash tables only compares postings that collide in at least one table. A pair with Jaccard similarity s becomes a candidate with probability 1 − (1 − s)^5 (0.9997 at s = 0.8). Every candidate's exact Jaccard distance is computed, so reported pairs are never false positives.
  - Reports pairs with similarity ≥ 0.85 (`--near-duplicate-similarity`), with a `same_source` flag for cross-source re-posts (Greenhouse vs Lever). The 0.85 default excludes postings that differ only in experience band, which score about 0.8. Writes up to 500 pairs to `analytics/near_duplicate_postings.parquet`; totals go in `summary.json` under `near_duplicates`. Skip it with `--skip-near-duplicates`.
- **Skills and work attributes** (Spark SQL over the semi-structured array): most requested skills (`LATERAL VIEW explode`), top skills per category (`ROW_NUMBER`), average salary by skill, work-mode × employment-type mix.
- **Batch job over cold storage** (`raw_quality.py`): replays `raw/` through the same `parse_kafka_values` + `apply_etl` as the stream, as a batch, into `analytics/data_quality_daily`. It gives daily received / valid / rejected counts, rejections per error code, distinct jobs, and valid events with skills. Because the ETL code is shared, changed rules can be re-run over history.
- `run_analytics.py` runs all of these and writes `analytics/*.parquet`. Timestamps are formatted in UTC before collecting, to avoid PySpark's implicit local-time conversion.

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

- **Kafka producer**: `acks=all` with idempotence enabled, so broker retries never duplicate or reorder a job's events. Sends are pipelined (linger 5 ms, 64 KB batches, gzip) and acknowledged once per batch. The ingestion outbox still confirms every event's acknowledgement before marking it published.
- **Kafka → Spark** is checkpointed, so a crash replays the uncommitted micro-batch with the same offsets.
- **Effect on HBase and Parquet is exactly-once** for replays:
  - Event IDs are the Kafka (topic, partition, offset), or a content hash for the local runner.
  - A replayed event is found in the job's ledger, and its original classification is reused without touching state or counters.
  - Parquet files for that batch and partition are overwritten with the same name.
  - The status file ignores a batch ID it has already counted.
- **Remaining window**: HBase counters are not transactional with the row mutations. A crash after a partition's row writes but before its counter increments leaves counters short by that partition. The Parquet lake is the authoritative count.
- Micro-batches create many small Parquet files. Compaction would need a table format with atomic commits (Delta Lake or Iceberg) to avoid readers briefly seeing both old and compacted files, so it is left as a next step.
- Public-board ingestion also keeps its own SQLite snapshot store with closure-by-absence rules (see [ingestion.md](./ingestion.md)). The streaming pipeline treats those events like any other source.

## 11. Concurrency, parallelism and performance

**Single writer per key.**
- Kafka messages are keyed by `job_id`, and every micro-batch is repartitioned by `job_id`, so one task owns all events of a job in a batch.
- HBase rows are therefore never written concurrently, and read-then-write per row needs no locks or check-and-put.
- The only cells shared across tasks are company counters, which use atomic increments.
- `test_parallel_partitions_match_sequential_processing` checks that 4 parallel partitions give exactly the 1-partition result.

**Races found and fixed:**
- *Counter lost updates*: the emulator's increment was read-then-write under a thread lock only. Four processes doing 150 increments each ended at 300 instead of 600. It is now one `BEGIN IMMEDIATE` transaction, and `test_counter_increments_are_atomic_across_processes` holds it at 600.
- *Concurrent table creation*: executors starting together raced to create HBase tables (`AlreadyExists` on a real cluster). The driver now creates the tables before the first batch, and losing the race is treated as success.
- *History overwrites*: history qualifiers used a per-process sequence number, so two writers could produce the same column. They now embed the globally unique event ID.

**Performance work** (numbers in [benchmarks.md](./benchmarks.md)):
- Bulk multi-get of states.
- One mutation batch per event timestamp.
- Per-partition counter aggregation.
- One transaction per partition in the emulator.
- Cached column-family metadata.
- Index-friendly version trimming.
- Pipelined Kafka sends.
- A connection reused per worker process.
- `maxOffsetsPerTrigger` bounds the micro-batch size (backpressure).
- `takeOrdered` gives the RDD top-k without a full sort.
- The analytics `cache()` the event DataFrame that all queries share.

## 12. Verification

`scripts/verify_pipeline.py` reconciles the whole run and exits non-zero on any failure:

- Every ID is unique within the raw archive, processed events and quarantine.
- No message is both processed and rejected.
- The raw archive equals processed ∪ quarantined: nothing silently dropped, nothing processed without being archived.
- With `--kafka`: every offset in the topic is in the raw archive.
- With an HBase store: every processed job exists in HBase, and its status matches its latest non-duplicate event.

On the end-to-end Docker run (real Kafka and HBase), all checks passed: 6,400 offsets = 6,400 archived = 6,077 processed + 323 quarantined, and 2,282 consistent HBase jobs. The check also caught a real bug during development: local-runner IDs collided for byte-identical messages.
