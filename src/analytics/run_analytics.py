"""Run the CareerPulse batch analytics over the Parquet event lake.

Spark SQL hiring trends (including skills and work-mode mix), Spark RDD top-k /
trending companies, the MLlib demand forecast, MinHash LSH near-duplicate
postings, and a daily data-quality report re-derived from the raw archive (cold
storage). Outputs land in ``<lake>/analytics`` for the dashboard.

    python -m src.analytics.run_analytics --lake data/lake --bucket-minutes 60
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics import spark_analytics
from src.common.spark_session import build_spark_session
from src.storage import parquet_store

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _stringify_timestamps(dataframe):
    """Format timestamps in the (UTC) session time zone before collecting.

    Collecting raw timestamps would convert them to the Python process's
    local time zone without saying so.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    for field in dataframe.schema.fields:
        if isinstance(field.dataType, TimestampType):
            dataframe = dataframe.withColumn(field.name, F.date_format(field.name, ISO_FORMAT))
    return dataframe


def run_all(
    spark,
    lake_root: str | Path,
    *,
    recent_minutes: int = 5,
    top_k: int = 5,
    trend_window_minutes: int = 60,
    bucket_minutes: int = 60,
    with_ml: bool = True,
    with_near_duplicates: bool = True,
    near_duplicate_similarity: float = 0.85,
    with_raw_quality: bool = True,
) -> Dict[str, Any]:
    events = spark_analytics.load_events(spark, lake_root).cache()
    total_events = events.count()

    outputs: Dict[str, List[Dict[str, Any]]] = {}
    for name, frame in spark_analytics.hiring_trends(spark, events, recent_minutes=recent_minutes).items():
        outputs[name] = spark_analytics.to_records(_stringify_timestamps(frame))
    outputs["rdd_top_companies"] = spark_analytics.top_k_companies_rdd(events, k=top_k)
    outputs["rdd_trending_companies"] = spark_analytics.trending_companies_rdd(
        events, k=top_k, window_minutes=trend_window_minutes
    )

    model_result: Optional[Dict[str, Any]] = None
    if with_ml:
        from src.analytics.demand_model import forecast_demand

        model_result = forecast_demand(events, bucket_minutes=bucket_minutes)
        outputs["demand_predictions"] = model_result.get("predictions", [])

    near_duplicates: Optional[Dict[str, Any]] = None
    if with_near_duplicates:
        from src.analytics.near_duplicates import find_near_duplicates

        near_duplicates = find_near_duplicates(events, min_similarity=near_duplicate_similarity)
        outputs["near_duplicate_postings"] = near_duplicates["pairs"]

    if with_raw_quality:
        from src.analytics.raw_quality import run as raw_quality

        outputs["data_quality_daily"] = raw_quality(spark, lake_root)

    for name, records in outputs.items():
        parquet_store.write_analytics_table(records, lake_root, name)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events_analyzed": total_events,
        "recent_minutes": recent_minutes,
        "trend_window_minutes": trend_window_minutes,
        "tables": {name: len(records) for name, records in outputs.items()},
        "demand_model": {k: v for k, v in (model_result or {}).items() if k != "predictions"},
        "near_duplicates": (near_duplicates or {}).get("summary", {}),
    }
    parquet_store.write_json(summary, Path(lake_root) / parquet_store.ANALYTICS_DATASET / "summary.json")
    events.unpersist()
    return {"summary": summary, "outputs": outputs}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Spark SQL / RDD / MLlib analytics over the event lake.")
    parser.add_argument("--lake", default="data/lake")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--recent-minutes", type=int, default=5)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--trend-window-minutes", type=int, default=60)
    parser.add_argument("--bucket-minutes", type=int, default=60, help="Time bucket for the demand model")
    parser.add_argument("--skip-ml", action="store_true")
    parser.add_argument("--skip-near-duplicates", action="store_true")
    parser.add_argument("--skip-raw-quality", action="store_true", help="Skip the batch job over the raw archive")
    parser.add_argument("--near-duplicate-similarity", type=float, default=0.85,
                        help="Minimum Jaccard similarity for two open postings to be reported as near-duplicates")
    args = parser.parse_args(argv)

    spark = build_spark_session("CareerPulse Analytics", args.master)
    try:
        result = run_all(
            spark,
            args.lake,
            recent_minutes=args.recent_minutes,
            top_k=args.top_k,
            trend_window_minutes=args.trend_window_minutes,
            bucket_minutes=args.bucket_minutes,
            with_ml=not args.skip_ml,
            with_near_duplicates=not args.skip_near_duplicates,
            with_raw_quality=not args.skip_raw_quality,
            near_duplicate_similarity=args.near_duplicate_similarity,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()

    summary = result["summary"]
    print(json.dumps(summary, indent=2))
    print("RDD top-k trending companies:")
    for row in result["outputs"]["rdd_trending_companies"]:
        print(f"  {row['rank']}. {row['company']}: recent={row['recent_new']} previous={row['previous_new']} momentum={row['momentum']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
