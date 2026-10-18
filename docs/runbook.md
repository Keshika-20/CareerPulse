# CareerPulse Runbook

## Prerequisites

- Python 3.10+ and `pip install -r requirements.txt`
- Java 17 or 21 for anything that starts Spark (tests, analytics, streaming)
- Docker Desktop for Kafka, HBase and the Linux Spark container

Spark's Python workers are started with the same interpreter as the driver (`src/common/spark_session.py`), so machines with several Pythons installed don't hit `PYTHON_VERSION_MISMATCH`.

### Spark on Windows

Spark reads and writes local files through Hadoop, which on Windows needs `winutils.exe`. What that means for each part:

| Task | Native Windows without winutils | Alternative |
|---|---|---|
| Tests, `scripts/run_pipeline.py --spark`, `src.analytics.run_analytics` | Works: Parquet is read with pyarrow and handed to Spark via Arrow | — |
| Structured Streaming job (needs checkpoints) | Fails fast with a clear message | `docker compose --profile pipeline up -d --build`, or install winutils (`HADOOP_HOME`, or `tools/hadoop/bin/winutils.exe`) |

On Linux and macOS everything runs natively.

## 1. Local end-to-end run (no Kafka or Docker)

```powershell
python scripts/run_pipeline.py --count 3000 --backfill-hours 24 --invalid-rate 0.03 --reset --spark
```

What it does:

1. The simulator writes 3,000 lifecycle events spread over the last 24 hours to `data/raw/job_events.jsonl`, about 3% of them malformed.
2. Events go through `StreamPipeline` in micro-batches. This is the same code the Spark job calls in `foreachBatch`: validation, change detection against HBase, HBase writes and Parquet lake appends.
3. It prints DuckDB KPIs over `data/lake`.
4. `--spark` runs Spark SQL, RDD and MLlib analytics into `data/lake/analytics`.

HBase defaults to the embedded emulator in `data/hbase/careerpulse_hbase.sqlite3`. Pass `--hbase-mode thrift` to use a real HBase.

## 2. Streaming pipeline with Docker

```powershell
docker compose --profile pipeline up -d --build
```

This starts:

- `kafka`: KRaft broker. Host clients use `localhost:9092`; containers use `kafka:29092`.
- `hbase`: HBase 2.1 with Thrift on `localhost:9090` and the master UI at http://localhost:16010. If those host ports are taken (Prometheus also uses 9090), set `CAREERPULSE_HBASE_THRIFT_PORT` / `CAREERPULSE_HBASE_UI_PORT` before `up`, and point host clients at the new port with `CAREERPULSE_HBASE_PORT`.
- `spark-streaming`: the Structured Streaming job with 10-second micro-batches, writing to HBase over Thrift and to `./data/lake` (bind-mounted). On its first start it downloads the Spark Kafka connector into a cached volume.

Produce events from the host:

```powershell
# 24 h of history, then a live stream of 5 events/s, with 3% malformed records
python -m src.streaming.kafka_producer --backfill-hours 24 --count 3000 --stream --rate 5 --invalid-rate 0.03
# or real public-board listings
python -m src.ingestion.service --config .\data\ingestion.local.json --kafka
```

Watch the job:

```powershell
docker logs -f careerpulse-spark-streaming   # "Batch N: processed=... rejected=... detected={...}"
```

Run the batch analytics:

```powershell
docker compose --profile analytics run --rm spark-analytics
# or natively:
python -m src.analytics.run_analytics --lake data/lake --bucket-minutes 60
```

Stop everything with `docker compose --profile pipeline down`. Add `-v` to also delete the Kafka, HBase and checkpoint volumes.

### Running the streaming job natively (Linux, macOS, or Windows with winutils)

```powershell
docker compose up -d kafka
python -m src.streaming.spark_kafka_consumer --bootstrap-servers localhost:9092 --hbase-mode local
```

Useful flags:

- `--available-now`: process what is in the topic, then exit.
- `--starting-offsets latest`
- `--trigger-seconds`
- `--watermark`, `--window`, `--slide`
- `--hbase-mode thrift --hbase-host localhost`

## 3. Dashboard

```powershell
python -m streamlit run .\src\dashboard\app.py
```

Sign-in uses OIDC (see [authentication-and-big-data.md](./authentication-and-big-data.md)). Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `CAREERPULSE_LAKE_PATH` | `./data/lake` | Parquet lake read by DuckDB |
| `CAREERPULSE_HBASE_MODE` | `local` | `thrift` to read the Docker HBase |
| `CAREERPULSE_HBASE_HOST` / `_PORT` | `localhost` / `9090` | HBase Thrift server |
| `CAREERPULSE_HBASE_PATH` | `./data/hbase/careerpulse_hbase.sqlite3` | Emulator file, opened read-only |
| `CAREERPULSE_SQLITE_PATH` | `./data/processed/ingestion.sqlite3` | Public-board ingestion database |

The **Streaming analytics** page shows:

- Pipeline status and KPIs
- A live event feed, with optional auto-refresh every 5 s
- Streaming window counts
- Trends by category, company or location (minute, hour or day buckets)
- Top companies and average salary by category
- The validation quarantine
- Spark RDD top-k and trending companies
- The MLlib forecast with RMSE vs a naive baseline
- An HBase job lookup: current state, salary cell versions, and the wide-row event history

## 4. Tests

```powershell
python -m pytest -q
```

- `test_spark_jobs.py`: Spark ETL parity with the Python rules, the `foreachBatch` handler, window logic, Spark SQL, RDD, and MLlib.
- `test_streaming.py`: Spark batch aggregation.

Both start a local Spark session (Java required) but use in-memory DataFrames. Everything else is pure Python.

## 5. Public-board ingestion

See [ingestion.md](./ingestion.md). In short:

```powershell
Copy-Item .\config\ingestion.example.json .\data\ingestion.local.json   # then enable verified boards
python -m src.ingestion.service --config .\data\ingestion.local.json [--spark-batch] [--kafka]
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `PYTHON_VERSION_MISMATCH` | Set `PYSPARK_PYTHON` explicitly, or start Spark through `build_spark_session` |
| `HADOOP_HOME and hadoop.home.dir are unset` | Spark file I/O on Windows. See *Spark on Windows* above |
| `Failed to find data source: kafka` | The connector package could not be downloaded. Check internet access, or set `CAREERPULSE_SCALA_VERSION` if your Spark build is not Scala 2.13 |
| Streaming container restarts with HBase errors | HBase takes about a minute to accept Thrift clients. The job retries the connection for about a minute, and Docker restarts it after that |
| Spark image build times out downloading PySpark (~450 MB) | Put a `pyspark-<version>-py2.py3-none-any.whl` in `docker/spark/wheels/` (git-ignored). The build installs it instead of downloading. `pip cache list --format abspath pyspark` finds one if PySpark was installed locally |
| Dashboard shows no window counts | Windows are emitted as event time advances. Keep the producer streaming |
