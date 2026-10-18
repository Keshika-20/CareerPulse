# Requirements Coverage

This maps every requirement in the course brief (*Big Data and Modern Databases – Final Package*) and the CareerPulse project description to where it is implemented and how to demonstrate it.

Command shorthands:

- **local**: `python scripts/run_pipeline.py --count 3000 --backfill-hours 24 --reset --spark`
- **docker**: `docker compose --profile pipeline up -d --build`, then the producer command in the [runbook](./runbook.md)

## 1. Required data, database, engines and language

| Requirement | Implementation | Demonstrate |
|---|---|---|
| Semi-structured data | JSON events with optional fields, a nested `requirements` object (skills array, education) and inconsistent spellings ([job_generator.py](../src/generator/job_generator.py)); Greenhouse/Lever API JSON ([ingestion/](../src/ingestion/)) | `python -m src.generator.job_generator --simulate --count 3` then open `data/raw/job_events.jsonl` |
| Streaming data | Kafka topic `job-events` → Spark Structured Streaming, 1–10 s micro-batches ([spark_kafka_consumer.py](../src/streaming/spark_kafka_consumer.py)) | docker; `docker logs -f careerpulse-spark-streaming` |
| Time-series data | Event-time sliding windows with watermarks; HBase cells versioned by event time; hourly trend tables; lag-feature demand forecast | Dashboard "Streaming windows" and "Category metrics"; HBase lookup shows `info:salary` versions |
| DB: HBase | Real HBase 2.1 over Thrift in Docker; wide-column `job_history` + `company_stats` ([hbase_store.py](../src/storage/hbase_store.py)) | HBase UI at http://localhost:16010; dashboard HBase job lookup |
| Processing: Apache Spark, Kafka | PySpark 4.2 (Structured Streaming, SQL, RDD, MLlib); Kafka 4.1 (KRaft) | docker |
| Language: Python | All code is Python | — |

## 2. Reference architecture diagram

| Diagram box | CareerPulse component |
|---|---|
| Sources: business apps (structured) | Greenhouse / Lever company job boards ([ingestion/](../src/ingestion/)), published with `python -m src.ingestion.service --kafka` |
| Sources: unstructured (sensors, clickstream) | Free-text job descriptions, mined for skills in Spark; malformed and inconsistent records from the simulator |
| Ingest: highly scalable, distributed, partitioned log | Kafka topic with 3 partitions, keyed by `job_id`, idempotent producer with `acks=all` ([kafka_producer.py](../src/streaming/kafka_producer.py)) |
| Stream processing: real-time processing of events | Structured Streaming ETL; parallel per-partition change detection; watermarked sliding windows |
| Cold storage: raw data for archival | `data/lake/raw/`: every message exactly as received, valid or not, with its Kafka position ([parquet_store.py](../src/storage/parquet_store.py) `append_raw`) |
| Data pipelines, ETL jobs | Streaming ETL ([spark_kafka_consumer.py](../src/streaming/spark_kafka_consumer.py) `apply_etl`) and ingestion ETL (normalisation of Greenhouse/Lever into one schema) |
| Batch jobs (cold → analytical) | [raw_quality.py](../src/analytics/raw_quality.py) replays the raw archive through the same Spark ETL into `analytics/data_quality_daily`; [run_analytics.py](../src/analytics/run_analytics.py) batch analytics |
| Analytical data store: structured, ready for querying | Parquet lake (`events/`, windows, `analytics/`) queried by DuckDB ([duckdb_queries.py](../src/analytics/duckdb_queries.py)) |
| Machine learning: ML models | MLlib demand forecast ([demand_model.py](../src/analytics/demand_model.py)) and MinHash LSH near-duplicates ([near_duplicates.py](../src/analytics/near_duplicates.py)) |
| Monitoring & notification: trigger alerts, publish messages, trigger workflows | Hiring-surge and data-quality alerts → `alerts/`, Kafka topic `job-alerts`, optional webhook (`--alert-webhook`) ([alerts.py](../src/streaming/alerts.py)) |
| Reporting & visualisation: dashboards | Streamlit **Streaming analytics** page ([streaming_view.py](../src/dashboard/streaming_view.py)) |
| Real-time apps | [alert_consumer.py](../src/streaming/alert_consumer.py) (also the `careerpulse-alert-consumer` container) and the dashboard's auto-refreshing live feed |

## 3. "You are expected to"

| Expectation | How it is met |
|---|---|
| High-volume, high-dimensional data domain from multiple sources | Job postings from the simulator (thousands of events per minute on demand) and two public board APIs. Each event has about 15 attributes plus a variable-length skills array (30 canonical skills), so after one-hot encoding of categories and skills the feature space has dozens of dimensions |
| ETL pipelines, data mapping and integration | Greenhouse and Lever payloads mapped to one normalised schema ([normalize.py](../src/ingestion/normalize.py)) and integrated with simulated events in one Kafka topic; streaming ETL into HBase and Parquet |
| Standardisation | Whitespace and case; timestamps to UTC; employment type (`FT`, `full time` → `Full-time`); work mode (`WFH` → `Remote`, inferred from a Remote location); skill aliases (`py` → `python`, `k8s` → `kubernetes`); title → category |
| Sorting | `sortWithinPartitions` by job, event time and Kafka offset (ordering needed for change detection); RDD `takeOrdered`; SQL `ORDER BY` / `ROW_NUMBER` ranking |
| Aggregation | Windowed streaming aggregates, Spark SQL group-bys, RDD `reduceByKey`, HBase counters |
| Validation | 5 rules with error codes; rejected records quarantined, never dropped silently. The Spark and Python rules are kept identical by a parity test |
| Verification | [verify_pipeline.py](../scripts/verify_pipeline.py) reconciles Kafka offsets, raw archive, processed and quarantined records and HBase state; ingestion verifies scan completeness; 143 automated tests |
| Produce an ML model / data analytics | MLlib forecast (RMSE vs naive baseline), MinHash LSH, 13 Spark SQL tables, RDD rankings |

## 4. "Your project should demonstrate"

| Item | Where | Demonstrate |
|---|---|---|
| Spark RDD, transformations, actions | [spark_analytics.py](../src/analytics/spark_analytics.py): `map`, `filter`, `reduceByKey` (transformations); `max`, `takeOrdered`, `isEmpty`, `collect` (actions); `mapPartitionsWithIndex` in the streaming handler | `python -m src.analytics.run_analytics` prints the RDD top-k |
| Spark SQL & ML libraries | 13 SQL queries incl. `LATERAL VIEW explode`, window functions, `percentile_approx`; MLlib `Pipeline`, `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `LinearRegression`, `RegressionEvaluator`, `MinHashLSH`, `HashingTF` | Dashboard "Spark batch analytics" |
| Real-time streaming data processing | Structured Streaming with 3 concurrent queries (events, windows by event type, windows by category) | docker; p95 latency 5.6 s at a 1 s trigger ([benchmarks.md](./benchmarks.md)) |
| Wide column data model | HBase `job_history`: versioned `info` family plus a wide `events:<ts>#<event_id>` column per event; `company_stats` counters | Dashboard HBase lookup: state, `info:salary` versions, event row |
| Sliding window metrics (sample project) | Per category in the last 5 minutes: new postings, closures, average salary, active companies (the job-market analogue of "active rides and average fare per neighborhood") | Dashboard "Category metrics · last 5 minutes" |
| Data warehouse for analytical querying | DuckDB over the Parquet lake | Dashboard; `python -c "from src.analytics.duckdb_queries import LakeQueries; print(LakeQueries('data/lake').kpis())"` |

## 5. Project description (CareerPulse spec)

| Objective | Status |
|---|---|
| Generate realistic job events in semi-structured JSON | Done: simulator plus the exact example schema from `generate_job_event` |
| Stream through Kafka | Done |
| Validate and process with Structured Streaming | Done |
| Detect new, modified, closed and duplicate postings | Done: stateful, replay-safe |
| HBase operational history | Done |
| Parquet historical/raw data | Done: raw archive + processed lake |
| Spark SQL hiring trends | Done |
| Spark RDD top-k / trending companies | Done |
| Simple ML for demand / relevance | Done: demand forecast; near-duplicates as an extra |
| Streamlit dashboard | Done |
| Example analytics (new jobs in last 5 min, top companies, trends by company/location/category, average salary by category, modified/closed counts, RDD top-k, ML prediction) | All on the dashboard |

## 6. End-to-end evidence (Docker run)

From a verified run on real Kafka and HBase:

- 6,400 Kafka records = 6,400 archived = 6,077 processed + 323 quarantined, with 0 duplicates; 2,282 HBase jobs consistent with the lake. `verify_pipeline.py` reported ALL CHECKS PASSED.
- An injected Data Engineer surge raised `HIRING_SURGE: 92 new postings in 5 min vs baseline 14.4`, which the alert consumer received over Kafka.
- A burst of malformed records raised `DATA_QUALITY` alerts (28–40% rejected).
- The demand model scored RMSE 23.3 vs naive baseline 27.9.
