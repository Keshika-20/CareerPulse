"""CareerPulse Spark Structured Streaming job.

Kafka topic -> JSON parsing -> validation / cleaning / standardisation /
enrichment (Spark DataFrame expressions) -> per micro-batch, in parallel per
job_id partition: raw archive (cold storage), change detection against HBase,
HBase job state + history, Parquet event lake and quarantine.

Two more streaming queries compute watermarked sliding windows: counts per
incoming event type (``window_counts``) and per-category hiring metrics
(``window_category``: new postings, closures, average salary, active
companies), which also drive hiring-surge alerts. Data-quality alerts are
raised from micro-batch rejection rates. Alerts go to the lake, the
``job-alerts`` Kafka topic and an optional webhook.
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType
except ImportError as exc:
    raise RuntimeError(
        "Install a compatible PySpark version before starting streaming."
    ) from exc

from src.common.models import (
    CATEGORY_RULES,
    DEFAULT_CATEGORY,
    EMPLOYMENT_TYPES,
    SKILL_ALIASES,
    SKILL_PATTERNS,
    VALID_EVENTS,
    WORK_MODES,
    job_fingerprint,
)
from src.common.spark_session import (
    build_spark_session,
    kafka_connector_package,
    require_hadoop_filesystem,
)
from src.storage import parquet_store
from src.storage.hbase_store import HBaseJobStore
from src.streaming.alerts import AlertConfig, AlertSink, data_quality_alert, detect_surges
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
            "employment_type",
            "work_mode",
            "description",
            # Nested object captured as raw JSON text and parsed separately, so
            # a malformed optional field can never reject an otherwise valid event.
            "requirements",
            CORRUPT_COLUMN,
        )
    ]
)
REQUIREMENTS_SCHEMA = StructType([
    StructField("skills", ArrayType(StringType()), True),
    StructField("education", StringType(), True),
])

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
# Version of the window aggregation's state schema (v2 adds HLL distinct counts).
WINDOW_STATE_VERSION = 2
# Version of the per-category window aggregation's state schema.
CATEGORY_STATE_VERSION = 1

# Relative standard deviation of the HyperLogLog++ distinct counts per window.
DISTINCT_RSD = 0.02


def parse_kafka_values(kafka_rows: DataFrame) -> DataFrame:
    """Decode Kafka records into the raw JSON text plus a parsed struct."""
    return (
        kafka_rows
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            (
                F.date_format(F.col("timestamp"), ISO_FORMAT)
                if "timestamp" in kafka_rows.columns
                else F.date_format(F.current_timestamp(), ISO_FORMAT)
            ).alias("kafka_timestamp"),
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


def _canonical_column(column, mapping: Dict[str, str]) -> Any:
    """Spark form of ``models._canonical``: letters-only lower-case key lookup."""
    lookup = F.create_map(*[F.lit(item) for pair in mapping.items() for item in pair])
    key = F.lower(F.regexp_replace(column, "[^A-Za-z]", ""))
    return F.when(column.isNull(), F.lit(None)).otherwise(
        F.coalesce(F.try_element_at(lookup, key), F.lit("Other"))
    )


def skills_column(declared, description) -> Any:
    """Spark form of ``models.merge_skills``: aliases + skills found in free text."""
    aliases = F.create_map(*[F.lit(item) for pair in SKILL_ALIASES.items() for item in pair])
    normalized = F.transform(
        F.coalesce(declared, F.array().cast("array<string>")),
        lambda skill: F.lower(F.trim(F.regexp_replace(skill, r"\s+", " "))),
    )
    normalized = F.transform(normalized, lambda skill: F.coalesce(F.try_element_at(aliases, skill), skill))
    normalized = F.filter(normalized, lambda skill: skill.isNotNull() & (skill != ""))
    found = F.array_compact(F.array(*[
        F.when(F.coalesce(description, F.lit("")).rlike(f"(?i){pattern}"), F.lit(skill))
        for skill, pattern in SKILL_PATTERNS
    ]))
    return F.array_sort(F.array_distinct(F.concat(normalized, found)))


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
        .withColumn("employment_type", _canonical_column(_clean(data["employment_type"]), EMPLOYMENT_TYPES))
        .withColumn(
            "work_mode",
            F.coalesce(
                _canonical_column(_clean(data["work_mode"]), WORK_MODES),
                F.when(F.lower(F.col("location")) == "remote", F.lit("Remote")),
            ),
        )
        .withColumn("requirements_parsed", F.from_json(data["requirements"], REQUIREMENTS_SCHEMA))
        .withColumn("education", _clean(F.col("requirements_parsed")["education"]))
        .withColumn("skills", skills_column(F.col("requirements_parsed")["skills"], _clean(data["description"])))
        .withColumn("skill_count", F.size("skills"))
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
        .drop("data", "salary_raw", "requirements_parsed")
    )


def window_counts(
    valid_events: DataFrame,
    *,
    watermark: str,
    window: str,
    slide: str,
    distinct_rsd: float = DISTINCT_RSD,
) -> DataFrame:
    """Sliding event-time window counts per incoming event type.

    Besides the event count, each window carries the number of distinct jobs
    and companies. Exact ``countDistinct`` is not supported in streaming
    aggregations, so these use HyperLogLog++ (``approx_count_distinct``):
    fixed-size state per window with a relative standard deviation of
    ``distinct_rsd``.
    """
    return (
        valid_events
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window, slide), F.col("event"))
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.approx_count_distinct("job_id", distinct_rsd).alias("unique_jobs"),
            F.approx_count_distinct("company", distinct_rsd).alias("unique_companies"),
        )
        .select(
            F.date_format("window.start", ISO_FORMAT).alias("window_start"),
            F.date_format("window.end", ISO_FORMAT).alias("window_end"),
            "event",
            "count",
            "unique_jobs",
            "unique_companies",
        )
    )


def category_window_metrics(
    valid_events: DataFrame,
    *,
    watermark: str,
    window: str,
    slide: str,
    distinct_rsd: float = DISTINCT_RSD,
) -> DataFrame:
    """Sliding-window hiring metrics per job category.

    For each window: new postings, closures, all events, average advertised
    salary (NEW and MODIFIED events) and active companies (HyperLogLog++).
    """
    event = F.col("event")
    return (
        valid_events
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window, slide), F.col("category"))
        .agg(
            F.sum(F.when(event == "NEW", 1).otherwise(0)).alias("new_postings"),
            F.sum(F.when(event == "CLOSED", 1).otherwise(0)).alias("closed_postings"),
            F.count(F.lit(1)).alias("events"),
            F.avg(F.when(event.isin("NEW", "MODIFIED"), F.col("salary"))).alias("avg_salary"),
            F.approx_count_distinct("company", distinct_rsd).alias("active_companies"),
        )
        .select(
            F.date_format("window.start", ISO_FORMAT).alias("window_start"),
            F.date_format("window.end", ISO_FORMAT).alias("window_end"),
            "category",
            "new_postings",
            "closed_postings",
            "events",
            "avg_salary",
            "active_companies",
        )
    )


def make_category_window_writer(
    lake_root: str | Path,
    sink: Optional[AlertSink] = None,
    config: AlertConfig = AlertConfig(),
) -> Callable[[DataFrame, int], None]:
    """Upsert category window metrics, then evaluate hiring-surge alerts."""

    def write(batch_df: DataFrame, _batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return
        parquet_store.merge_category_windows(rows, lake_root)
        if sink is None:
            return
        history = parquet_store.read_dataset(lake_root, parquet_store.CATEGORY_WINDOW_DATASET).to_dict("records")
        for alert in sink.publish(detect_surges(rows, history, config)):
            print(f"ALERT {alert['alert_type']}: {alert['message']}", flush=True)

    return write


BATCH_COLUMNS = (
    "raw_value", "validation_error", "job_id", "company", "title", "location",
    "experience", "salary", "source", "timestamp", "event", "event_time_iso",
    "category", "event_time", "employment_type", "work_mode", "education",
    "skills", "skill_count", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp",
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
        "employment_type": row["employment_type"],
        "work_mode": row["work_mode"],
        "education": row["education"],
        "skills": list(row["skills"] or []),
        "skill_count": row["skill_count"],
        # The Kafka position gives every event a replay-stable identity.
        "kafka_topic": row["kafka_topic"],
        "kafka_partition": row["kafka_partition"],
        "kafka_offset": row["kafka_offset"],
    }
    record["fingerprint"] = job_fingerprint(record)
    return record


# One HBase connection per Python worker process, reused across tasks and
# micro-batches (Spark reuses Python workers), instead of one per partition.
_STORE_CACHE: Dict[str, HBaseJobStore] = {}


def _store_key(hbase: Dict[str, Any]) -> str:
    return json.dumps(hbase, sort_keys=True, default=str)


def _cached_store(hbase: Dict[str, Any]) -> HBaseJobStore:
    key = _store_key(hbase)
    if key not in _STORE_CACHE:
        _STORE_CACHE[key] = HBaseJobStore.connect(**hbase)
    return _STORE_CACHE[key]


def _drop_cached_store(hbase: Dict[str, Any]) -> None:
    store = _STORE_CACHE.pop(_store_key(hbase), None)
    if store is not None:
        store.close()


def process_partition_rows(
    index: int,
    rows: Iterable[Any],
    *,
    batch_id: int,
    hbase: Dict[str, Any],
    lake_root: str,
    source: str,
) -> Iterator[Dict[str, Any]]:
    """Executor-side work for one partition of a micro-batch.

    The batch is hash-partitioned by job_id, so this task is the only writer
    for every job it sees and can safely read-then-write those HBase rows.
    """
    received_at = datetime.now(timezone.utc).isoformat()
    valid: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    raw: List[Dict[str, Any]] = []
    for row in rows:
        event_id = f"{row['kafka_topic']}-{row['kafka_partition']}-{row['kafka_offset']}"
        raw.append({
            "event_id": event_id,
            "kafka_topic": row["kafka_topic"],
            "kafka_partition": row["kafka_partition"],
            "kafka_offset": row["kafka_offset"],
            "kafka_timestamp": row["kafka_timestamp"],
            "raw_value": row["raw_value"],
            "received_at": received_at,
        })
        if row["validation_error"] is None:
            valid.append(row_to_record(row))
        else:
            rejected.append({
                "event_id": event_id,
                "raw_value": row["raw_value"],
                "validation_error": row["validation_error"],
                "received_at": received_at,
            })
    if not raw:
        return
    for attempt in range(2):
        try:
            pipeline = StreamPipeline(_cached_store(hbase), lake_root, source=source)
            summary = pipeline.process_partition(
                valid, batch_id, rejected=rejected, raw_records=raw, partition=index
            )
        except Exception:
            # An idle Thrift connection may have been dropped. Retrying is safe
            # because every write in process_partition is idempotent.
            _drop_cached_store(hbase)
            if attempt:
                raise
            continue
        yield summary
        return


def stream_label(checkpoint_dir: Path) -> str:
    """Stable label for this streaming query, taken from its checkpoint metadata.

    Spark batch IDs restart at 0 with a new checkpoint; prefixing output files
    with the query ID keeps a new query from overwriting an old query's files,
    while a restarted query (same checkpoint) keeps overwriting its own.
    """
    try:
        query_id = json.loads((checkpoint_dir / "metadata").read_text(encoding="utf-8"))["id"]
        return f"spark-{query_id[:8]}"
    except (OSError, ValueError, KeyError):
        return "spark"


def make_batch_processor(
    *,
    hbase: Dict[str, Any],
    lake_root: str | Path,
    num_partitions: int,
    source: str | Callable[[], str] = "spark",
    alert_sink: Optional[AlertSink] = None,
    alert_config: AlertConfig = AlertConfig(),
) -> Callable[[DataFrame, int], None]:
    """Build the foreachBatch handler.

    The micro-batch is repartitioned by job_id and each partition is processed
    on an executor in parallel (change detection, HBase writes, Parquet file).
    Only small per-partition summaries return to the driver.
    """
    resolved: Dict[str, str] = {}

    def process(batch_df: DataFrame, batch_id: int) -> None:
        started = time.perf_counter()
        if "source" not in resolved:
            resolved["source"] = source() if callable(source) else source
            # Create the HBase tables once on the driver so executors never race
            # to create them on their first batch.
            HBaseJobStore.connect(**hbase).close()
        run = functools.partial(
            process_partition_rows,
            batch_id=int(batch_id),
            hbase=hbase,
            lake_root=str(lake_root),
            source=resolved["source"],
        )
        summaries = (
            batch_df.select(*BATCH_COLUMNS)
            .repartition(num_partitions, "job_id")
            .sortWithinPartitions("job_id", "event_time", "kafka_partition", "kafka_offset")
            .rdd.mapPartitionsWithIndex(run)
            .collect()
        )
        if not summaries:
            return
        summary = StreamPipeline(None, lake_root, source=resolved["source"]).record_batch(
            summaries, int(batch_id), duration_ms=round((time.perf_counter() - started) * 1000)
        )
        replay = " (replay)" if summary["replay"] else ""
        print(
            f"Batch {batch_id}: processed={summary['processed']} rejected={summary['rejected']} "
            f"partitions={summary['partitions']} duration_ms={summary['duration_ms']} "
            f"detected={summary['detected']}{replay}",
            flush=True,
        )
        if alert_sink is not None:
            for alert in alert_sink.publish([data_quality_alert(summary, alert_config)]):
                print(f"ALERT {alert['alert_type']}: {alert['message']}", flush=True)

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
    parser.add_argument("--partitions", type=int, default=None, help="Parallel partitions per micro-batch (default: Spark default parallelism).")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--slide", default="1 minute")
    parser.add_argument("--alerts-topic", default="job-alerts", help="Kafka topic for monitoring alerts.")
    parser.add_argument("--alert-webhook", default=None, help="Optional webhook URL that receives each alert as JSON.")
    parser.add_argument("--surge-ratio", type=float, default=AlertConfig.surge_ratio)
    parser.add_argument("--max-rejection-rate", type=float, default=AlertConfig.max_rejection_rate)
    parser.add_argument("--hbase-mode", choices=["local", "thrift"], default="local")
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-table-prefix", default="", help="Prefix for HBase table names (isolates environments).")
    parser.add_argument("--hbase-path", default="data/hbase/careerpulse_hbase.sqlite3", help="Emulator file (local mode).")
    parser.add_argument("--parquet-output", default=None, help=argparse.SUPPRESS)  # legacy alias for --lake
    return parser


def main(argv: List[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    lake_root = Path(args.parquet_output or args.lake).resolve()
    checkpoint = Path(args.checkpoint).resolve()
    require_hadoop_filesystem("The Structured Streaming job (checkpointing)")

    hbase = {
        "mode": args.hbase_mode,
        "host": args.hbase_host,
        "port": args.hbase_port,
        "path": str(Path(args.hbase_path).resolve()),
        "table_prefix": args.hbase_table_prefix,
        "retries": 12,
    }

    spark = build_spark_session(
        "CareerPulse Structured Streaming",
        args.master,
        packages=[kafka_connector_package()],
    )
    from src.streaming.kafka_producer import ensure_topics

    # Spark's Kafka source fails on a missing topic, so create both topics
    # first; this removes the start-up race with the first producer.
    ensure_topics(args.bootstrap_servers, [args.topic, args.alerts_topic])
    alert_config = AlertConfig(surge_ratio=args.surge_ratio, max_rejection_rate=args.max_rejection_rate)
    alert_sink = AlertSink(
        lake_root,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.alerts_topic,
        webhook_url=args.alert_webhook,
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
                .foreachBatch(make_batch_processor(
                    hbase=hbase,
                    lake_root=lake_root,
                    num_partitions=args.partitions or spark.sparkContext.defaultParallelism,
                    source=lambda: stream_label(checkpoint / "events"),
                    alert_sink=alert_sink,
                    alert_config=alert_config,
                ))
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
                # Stateful aggregation state is tied to its schema; bump the
                # version whenever window_counts() output changes so the query
                # rebuilds its window state from Kafka instead of failing with
                # STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE.
                .option("checkpointLocation", str(checkpoint / f"windows-v{WINDOW_STATE_VERSION}"))
            ).start()
        )
        queries.append(
            trigger(
                category_window_metrics(valid, watermark=args.watermark, window=args.window, slide=args.slide)
                .writeStream
                .queryName("careerpulse_category_windows")
                .outputMode("update")
                .foreachBatch(make_category_window_writer(lake_root, alert_sink, alert_config))
                .option("checkpointLocation", str(checkpoint / f"category-v{CATEGORY_STATE_VERSION}"))
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
        alert_sink.close()
        spark.stop()
        print("Spark Structured Streaming stopped.")


if __name__ == "__main__":
    main()
