
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        coalesce,
        from_json,
        trim,
        upper,
    )
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )
except ImportError as exc:
    raise RuntimeError(
        "Install a compatible PySpark version before starting streaming."
    ) from exc


EVENT_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), True),
        StructField("event", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("company", StringType(), True),
        StructField("title", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("salary", DoubleType(), True),
    ]
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Consume job events from Kafka using Spark Structured Streaming "
            "and optionally save events as Parquet."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka broker address.",
    )
    parser.add_argument(
        "--topic",
        default="job-events",
        help="Kafka topic to consume.",
    )
    parser.add_argument(
        "--checkpoint",
        default="data/checkpoints/spark-job-events",
        help="Directory used by Spark to recover streaming progress.",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="latest",
        help="Read older Kafka messages or only new messages.",
    )
    parser.add_argument(
        "--parquet-output",
        default=None,
        help=(
            "Optional directory for processed job events in Parquet format. "
            "If omitted, only the console aggregation is started."
        ),
    )

    args = parser.parse_args()

    checkpoint = Path(args.checkpoint).resolve()
    checkpoint.mkdir(parents=True, exist_ok=True)

    parquet_path = None
    if args.parquet_output:
        parquet_path = Path(args.parquet_output).resolve()
        parquet_path.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("CareerPulse Kafka Structured Streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = None
    parquet_query = None

    try:
        # 1. Read the Kafka topic as a streaming DataFrame.
        kafka_rows = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                args.bootstrap_servers,
            )
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .load()
        )

        # 2. Convert each Kafka message from JSON into structured columns.
        parsed = (
            kafka_rows
            .selectExpr("CAST(value AS STRING) AS json_value")
            .select(
                from_json(
                    col("json_value"),
                    EVENT_SCHEMA,
                ).alias("data")
            )
            .select("data.*")
        )

        # The ingestion service may use "event", while the demo producer
        # may use "event_type". Accept either field.
        events = (
            parsed
            .withColumn(
                "event_name",
                upper(
                    trim(
                        coalesce(
                            col("event"),
                            col("event_type"),
                        )
                    )
                ),
            )
            .filter(col("event_name").isNotNull())
            .filter(col("event_name") != "")
        )

        # 3. Aggregate event counts for the live console dashboard.
        counts = (
            events
            .groupBy("event_name")
            .count()
        )

        query = (
            counts.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                str(checkpoint),
            )
            .start()
        )

        # 4. Optionally save individual processed events as Parquet.
        # This is a separate streaming query with its own checkpoint.
        if parquet_path is not None:
            parquet_checkpoint = checkpoint / "parquet-sink"
            parquet_checkpoint.mkdir(
                parents=True,
                exist_ok=True,
            )

            parquet_query = (
                events.writeStream
                .format("parquet")
                .outputMode("append")
                .option("path", str(parquet_path))
                .option(
                    "checkpointLocation",
                    str(parquet_checkpoint),
                )
                .start()
            )

            print(
                f"Parquet output enabled: {parquet_path}"
            )
        else:
            print(
                "Parquet output is disabled. "
                "Pass --parquet-output to save processed events."
            )

        print(
            f"Spark Structured Streaming is reading Kafka topic "
            f"'{args.topic}' from {args.bootstrap_servers}."
        )
        print("Press Ctrl+C to stop streaming.")

        # Keep the application running until interrupted or a query fails.
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nStopping Spark Structured Streaming...")

    finally:
        if parquet_query is not None and parquet_query.isActive:
            parquet_query.stop()

        if query is not None and query.isActive:
            query.stop()

        spark.stop()
        print("Spark Structured Streaming stopped.")


if __name__ == "__main__":
    main()
