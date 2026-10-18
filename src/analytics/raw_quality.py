"""Batch job over cold storage: daily data-quality report from the raw archive.

The raw archive keeps every incoming message exactly as received. This job
replays it through the *same* Spark ETL used by the streaming job
(``parse_kafka_values`` + ``apply_etl``), as a batch, and aggregates per day:
messages received, valid / rejected counts and rate, rejections per error
code, distinct jobs and how many valid events carried skills. It is the
"cold storage -> batch job -> analytical store" path of the architecture and
lets the ETL rules be re-run over history after they change.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.analytics.spark_analytics import load_dataset
from src.storage import parquet_store

ERROR_CODES = ("malformed_json", "missing_fields", "invalid_event", "invalid_salary", "invalid_timestamp")


def raw_as_kafka_rows(raw: DataFrame) -> DataFrame:
    """Reshape archived rows into the Kafka source schema the ETL expects."""
    return raw.select(
        F.col("raw_value").cast("binary").alias("value"),
        F.coalesce(F.col("kafka_topic"), F.lit("archive")).alias("topic"),
        F.coalesce(F.col("kafka_partition"), F.lit(-1)).cast("int").alias("partition"),
        F.coalesce(F.col("kafka_offset"), F.lit(-1)).cast("long").alias("offset"),
        F.coalesce(F.col("kafka_timestamp"), F.col("received_at")).alias("timestamp"),
    )


def data_quality_daily(raw: DataFrame) -> DataFrame:
    from src.streaming.spark_kafka_consumer import apply_etl, parse_kafka_values

    checked = apply_etl(parse_kafka_values(raw_as_kafka_rows(raw))).withColumn(
        "day", F.substring("kafka_timestamp", 1, 10)
    )
    error_code = F.split(F.col("validation_error"), ":").getItem(0)
    valid = F.col("validation_error").isNull()
    return (
        checked.groupBy("day")
        .agg(
            F.count(F.lit(1)).alias("received"),
            F.sum(F.when(valid, 1).otherwise(0)).alias("valid"),
            F.sum(F.when(~valid, 1).otherwise(0)).alias("rejected"),
            *[F.sum(F.when(error_code == code, 1).otherwise(0)).alias(code) for code in ERROR_CODES],
            F.countDistinct(F.when(valid, F.col("job_id"))).alias("distinct_jobs"),
            F.sum(F.when(valid & (F.col("skill_count") > 0), 1).otherwise(0)).alias("valid_with_skills"),
        )
        .withColumn("rejection_rate", F.round(F.col("rejected") / F.col("received"), 4))
        .orderBy("day")
    )


def run(spark, lake_root: str | Path) -> List[Dict[str, Any]]:
    """Compute the report; returns [] when there is no raw archive yet."""
    if not parquet_store.dataset_files(lake_root, parquet_store.RAW_DATASET):
        return []
    raw = load_dataset(spark, lake_root, parquet_store.RAW_DATASET)
    return [row.asDict() for row in data_quality_daily(raw).collect()]
