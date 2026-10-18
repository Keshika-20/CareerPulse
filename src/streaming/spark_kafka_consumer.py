"""CareerPulse Spark Structured Streaming job.

Kafka topic -> JSON parsing -> validation / cleaning / enrichment (Spark
DataFrame expressions) -> per micro-batch change detection against HBase ->
HBase job state + history, Parquet event lake and quarantine.

A second streaming query computes watermarked sliding-window counts of
incoming event types into the lake's ``window_counts`` dataset.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType
except ImportError as exc:
    raise RuntimeError(
        "Install a compatible PySpark version before starting streaming."
    ) from exc

from src.common.models import CATEGORY_RULES, DEFAULT_CATEGORY, VALID_EVENTS, job_fingerprint
from src.common.spark_session import (
    build_spark_session,
    kafka_connector_package,
    require_hadoop_filesystem,
)
from src.storage import parquet_store
from src.storage.hbase_store import HBaseJobStore
from src.streaming.etl import REQUIRED_NON_NULL, parse_timestamp
from src.streaming.pipeline import StreamPipeline

CORRUPT_COLUMN = "_corrupt_record"

# Every field is read as a string so type problems surface as validation
# errors (e.g. invalid_salary) instead of silently becoming nulls.
EVENT_SCHEMA = StructType(
    [
        StructField(name, StringType(), True)
        for name in (
            "job_id",
            "company",
            "title",
            "location",
            "experience",
            "salary",
            "timestamp",
            "event",
            "event_type",
            "source",
            CORRUPT_COLUMN,
        )
    ]
)

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


def parse_kafka_values(kafka_rows: DataFrame) -> DataFrame:
    """Decode Kafka records into the raw JSON text plus a parsed struct."""
    return (
        kafka_rows
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn(
            "data",
            F.from_json(
                "raw_value",
                EVENT_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COLUMN},
            ),
        )
    )


def _clean(column) -> Any:
    return F.nullif(F.trim(F.regexp_replace(column, r"\s+", " ")), F.lit(""))


def category_column(title) -> Any:
    expression = None
    for category, pattern in CATEGORY_RULES:
        condition = title.rlike(f"(?i){pattern}")
        expression = (
            F.when(condition, F.lit(category))
            if expression is None
            else expression.when(condition, F.lit(category))
        )
    return expression.otherwise(F.lit(DEFAULT_CATEGORY))


def apply_etl(parsed: DataFrame) -> DataFrame:
    """Clean, validate and enrich parsed events.

    Adds ``validation_error`` (null when valid) using the same codes as the
    Python rules in ``src.streaming.etl``.
    """
    data = F.col("data")
    cleaned = (
        parsed
        .withColumn("job_id", _clean(data["job_id"]))
        .withColumn("company", _clean(data["company"]))
        .withColumn("title", _clean(data["title"]))
        .withColumn("location", _clean(data["location"]))
        .withColumn("experience", _clean(data["experience"]))
        .withColumn("source", _clean(data["source"]))
        .withColumn("timestamp", _clean(data["timestamp"]))
        .withColumn("event", F.upper(_clean(F.coalesce(data["event"], data["event_type"]))))
        .withColumn("salary_raw", _clean(data["salary"]))
        .withColumn("salary", F.col("salary_raw").try_cast("double"))
        .withColumn("event_time", F.col("timestamp").try_cast("timestamp"))
        .withColumn("category", category_column(F.col("title")))
    )

    missing = F.concat_ws(
        ",", *[F.when(F.col(field).isNull(), F.lit(field)) for field in REQUIRED_NON_NULL]
    )
    salary = F.col("salary")
    bad_salary = F.col("salary_raw").isNotNull() & (
        salary.isNull() | F.isnan(salary) | (salary < 0) | (F.abs(salary) == float("inf"))
    )
    error = (
        F.when(data.isNull() | data[CORRUPT_COLUMN].isNotNull(), F.lit("malformed_json"))
        .when(missing != "", F.concat(F.lit("missing_fields:"), missing))
        .when(~F.col("event").isin(*sorted(VALID_EVENTS)), F.lit("invalid_event"))
        .when(bad_salary, F.lit("invalid_salary"))
        .when(F.col("event_time").isNull(), F.lit("invalid_timestamp"))
    )
    return (
        cleaned
        .withColumn("validation_error", error)
        .withColumn("event_time_iso", F.date_format("event_time", ISO_FORMAT))
        .drop("data", "salary_raw")
    )


def window_counts(valid_events: DataFrame, *, watermark: str, window: str, slide: str) -> DataFrame:
    """Sliding event-time window counts per incoming event type."""
    return (
        valid_events
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window, slide), F.col("event"))
        .count()
        .select(
            F.date_format("window.start", ISO_FORMAT).alias("window_start"),
            F.date_format("window.end", ISO_FORMAT).alias("window_end"),
            "event",
            "count",
        )
    )


def row_to_record(row: Any) -> Dict[str, Any]:
    event_time = parse_timestamp(row["event_time_iso"])
    record = {
        "job_id": row["job_id"],
        "company": row["company"],
        "title": row["title"],
        "location": row["location"],
        "experience": row["experience"],
        "salary": row["salary"],
        "source": row["source"],
        "timestamp": row["timestamp"],
        "event": row["event"],
        "event_time": event_time.isoformat() if event_time else None,
        "category": row["category"],
    }
    record["fingerprint"] = job_fingerprint(record)
    return record


def make_batch_processor(pipeline_factory: Callable[[], StreamPipeline]) -> Callable[[DataFrame, int], None]:
    """Build the foreachBatch handler that runs change detection and storage."""
    state: Dict[str, Any] = {"pipeline": None}

    def process(batch_df: DataFrame, batch_id: int) -> None:
        rows = (
            batch_df
            .select(
                "raw_value", "validation_error", "job_id", "company", "title", "location",
                "experience", "salary", "source", "timestamp", "event", "event_time_iso",
                "category", "event_time", "kafka_partition", "kafka_offset",
            )
            .orderBy("event_time", "kafka_partition", "kafka_offset")
            .collect()
        )
        if not rows:
            return
        received_at = datetime.now(timezone.utc).isoformat()
        valid: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for row in rows:
            if row["validation_error"] is None:
                valid.append(row_to_record(row))
            else:
                rejected.append({
                    "raw_value": row["raw_value"],
                    "validation_error": row["validation_error"],
                    "received_at": received_at,
                })

        if state["pipeline"] is None:
            state["pipeline"] = pipeline_factory()
        try:
            summary = state["pipeline"].process_valid_records(valid, int(batch_id), rejected=rejected)
        except Exception as exc:
            # Thrift connections to HBase can drop while idle; reconnect once.
            print(f"Batch {batch_id}: storage error ({exc}); reconnecting and retrying.", flush=True)
            state["pipeline"].store.close()
            state["pipeline"] = pipeline_factory()
            summary = state["pipeline"].process_valid_records(valid, int(batch_id), rejected=rejected)
        print(
            f"Batch {batch_id}: processed={summary['processed']} rejected={summary['rejected']} "
            f"detected={summary['detected']}",
            flush=True,
        )

    return process


def make_window_writer(lake_root: str | Path) -> Callable[[DataFrame, int], None]:
    def write(batch_df: DataFrame, _batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if rows:
            parquet_store.merge_window_counts(rows, lake_root)

    return write


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume job events from Kafka with Spark Structured Streaming."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka broker address.")
    parser.add_argument("--topic", default="job-events", help="Kafka topic to consume.")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--checkpoint", default="data/checkpoints/job-events", help="Streaming checkpoint directory.")
    parser.add_argument("--lake", default="data/lake", help="Parquet lake root directory.")
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[*].")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Micro-batch interval.")
    parser.add_argument("--available-now", action="store_true", help="Process everything available, then stop.")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--slide", default="1 minute")
    parser.add_argument("--hbase-mode", choices=["local", "thrift"], default="local")
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-path", default="data/hbase/careerpulse_hbase.sqlite3", help="Emulator file (local mode).")
    parser.add_argument("--parquet-output", default=None, help=argparse.SUPPRESS)  # legacy alias for --lake
    return parser


def main(argv: List[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    lake_root = Path(args.parquet_output or args.lake).resolve()
    checkpoint = Path(args.checkpoint).resolve()
    require_hadoop_filesystem("The Structured Streaming job (checkpointing)")

    def pipeline_factory() -> StreamPipeline:
        store = HBaseJobStore.connect(
            args.hbase_mode,
            host=args.hbase_host,
            port=args.hbase_port,
            path=args.hbase_path,
            retries=12,
        )
        return StreamPipeline(store, lake_root, source="spark-structured-streaming")

    spark = build_spark_session(
        "CareerPulse Structured Streaming",
        args.master,
        packages=[kafka_connector_package()],
    )
    queries = []
    try:
        kafka_rows = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        events = apply_etl(parse_kafka_values(kafka_rows))

        def trigger(writer):
            if args.available_now:
                return writer.trigger(availableNow=True)
            return writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

        queries.append(
            trigger(
                events.writeStream
                .queryName("careerpulse_events")
                .foreachBatch(make_batch_processor(pipeline_factory))
                .option("checkpointLocation", str(checkpoint / "events"))
            ).start()
        )
        valid = events.where(F.col("validation_error").isNull())
        queries.append(
            trigger(
                window_counts(valid, watermark=args.watermark, window=args.window, slide=args.slide)
                .writeStream
                .queryName("careerpulse_window_counts")
                .outputMode("update")
                .foreachBatch(make_window_writer(lake_root))
                .option("checkpointLocation", str(checkpoint / "windows"))
            ).start()
        )

        print(
            f"Structured Streaming is reading '{args.topic}' from {args.bootstrap_servers}; "
            f"lake={lake_root}; hbase={args.hbase_mode}.",
            flush=True,
        )
        if args.available_now:
            for query in queries:
                query.awaitTermination()
        else:
            print("Press Ctrl+C to stop streaming.", flush=True)
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping Spark Structured Streaming...")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
        print("Spark Structured Streaming stopped.")


if __name__ == "__main__":
    main()
