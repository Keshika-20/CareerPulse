"""Spark SQL hiring-trend analytics and Spark RDD top-k company analysis.

Both operate on the processed (change-detected) event lake written by the
streaming pipeline. Results are small and are saved as Parquet tables under
``<lake>/analytics`` for the DuckDB-backed dashboard.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

from src.common.spark_session import hadoop_filesystem_available
from src.storage import parquet_store

EVENTS_VIEW = "job_events"

# Spark SQL statements over the `job_events` view. `{recent_minutes}` is filled
# in at run time; "recent" is measured back from the newest event in the data.
HIRING_TREND_QUERIES: Dict[str, str] = {
    "event_counts": """
        SELECT event, COUNT(*) AS events
        FROM job_events GROUP BY event ORDER BY events DESC
    """,
    "recent_new_jobs": """
        WITH bounds AS (SELECT MAX(event_time) AS latest FROM job_events)
        SELECT COUNT(*) AS new_jobs,
               {recent_minutes} AS window_minutes,
               MAX(b.latest) AS as_of
        FROM job_events e CROSS JOIN bounds b
        WHERE e.event = 'NEW'
          AND e.event_time > b.latest - INTERVAL {recent_minutes} MINUTES
    """,
    "top_companies_new": """
        SELECT company, COUNT(*) AS new_postings
        FROM job_events WHERE event = 'NEW'
        GROUP BY company ORDER BY new_postings DESC, company LIMIT 10
    """,
    "trend_by_company": """
        SELECT date_trunc('HOUR', event_time) AS bucket, company, COUNT(*) AS new_postings
        FROM job_events WHERE event = 'NEW'
        GROUP BY 1, 2 ORDER BY bucket, company
    """,
    "trend_by_location": """
        SELECT date_trunc('HOUR', event_time) AS bucket, location, COUNT(*) AS new_postings
        FROM job_events WHERE event = 'NEW'
        GROUP BY 1, 2 ORDER BY bucket, location
    """,
    "trend_by_category": """
        SELECT date_trunc('HOUR', event_time) AS bucket, category, COUNT(*) AS new_postings
        FROM job_events WHERE event = 'NEW'
        GROUP BY 1, 2 ORDER BY bucket, category
    """,
    "hiring_by_category_location": """
        SELECT category, location, COUNT(*) AS new_postings
        FROM job_events WHERE event = 'NEW'
        GROUP BY category, location ORDER BY new_postings DESC
    """,
    "avg_salary_by_category": """
        SELECT category,
               ROUND(AVG(salary), 0) AS avg_salary,
               ROUND(percentile_approx(salary, 0.5), 0) AS median_salary,
               COUNT(salary) AS salary_samples
        FROM job_events
        WHERE event IN ('NEW', 'MODIFIED') AND salary IS NOT NULL
        GROUP BY category ORDER BY avg_salary DESC
    """,
    "modified_closed_by_company": """
        SELECT company,
               SUM(CASE WHEN event = 'MODIFIED' THEN 1 ELSE 0 END) AS modified,
               SUM(CASE WHEN event = 'CLOSED' THEN 1 ELSE 0 END) AS closed,
               SUM(CASE WHEN event = 'DUPLICATE' THEN 1 ELSE 0 END) AS duplicates
        FROM job_events GROUP BY company ORDER BY modified + closed DESC, company
    """,
}


def load_events(spark, lake_root: str | Path):
    """Load the processed event lake as a Spark DataFrame.

    Spark reads the Parquet files directly when its local filesystem layer is
    usable. On Windows without winutils, files are read with pyarrow and handed
    to Spark through Arrow instead.
    """
    files = parquet_store.dataset_files(lake_root, parquet_store.EVENTS_DATASET)
    if not files:
        raise FileNotFoundError(
            f"No processed events under {Path(lake_root) / parquet_store.EVENTS_DATASET}. "
            "Run the streaming job or scripts/run_pipeline.py first."
        )
    if hadoop_filesystem_available():
        return spark.read.parquet(*[str(path) for path in files])
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    frame = parquet_store.read_dataset(lake_root, parquet_store.EVENTS_DATASET)
    return spark.createDataFrame(frame)


def hiring_trends(spark, events_df, *, recent_minutes: int = 5) -> Dict[str, Any]:
    """Run every hiring-trend query with Spark SQL; returns name -> DataFrame."""
    events_df.createOrReplaceTempView(EVENTS_VIEW)
    return {
        name: spark.sql(query.format(recent_minutes=int(recent_minutes)))
        for name, query in HIRING_TREND_QUERIES.items()
    }


def top_k_companies_rdd(events_df, k: int = 5) -> List[Dict[str, Any]]:
    """Top-k companies by NEW postings: map -> reduceByKey -> takeOrdered."""
    pairs = (
        events_df.where("event = 'NEW'").select("company").rdd
        .map(lambda row: (row["company"], 1))
    )
    counts = pairs.reduceByKey(lambda left, right: left + right)
    top = counts.takeOrdered(k, key=lambda item: (-item[1], item[0]))
    return [{"rank": index + 1, "company": company, "new_postings": count} for index, (company, count) in enumerate(top)]


def trending_companies_rdd(events_df, k: int = 5, window_minutes: int = 60) -> List[Dict[str, Any]]:
    """Top-k *trending* companies with RDD transformations and actions.

    Momentum compares NEW postings in the latest window with the window just
    before it (both measured back from the newest event): ``momentum = recent -
    previous``, with growth as a percentage of the previous window.
    """
    rows = events_df.where("event = 'NEW'").select("company", "event_time").rdd
    if rows.isEmpty():
        return []
    latest = rows.map(lambda row: row["event_time"]).max()  # action
    window = timedelta(minutes=window_minutes)
    recent_start = latest - window
    previous_start = latest - 2 * window

    def bucket(row):
        moment = row["event_time"]
        recent = 1 if moment > recent_start else 0
        previous = 1 if previous_start < moment <= recent_start else 0
        return row["company"], (recent, previous)

    totals = (
        rows.map(bucket)                                                      # transformation
        .filter(lambda item: item[1] != (0, 0))                               # transformation
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))                 # wide transformation
        .map(lambda item: (item[0], item[1][0], item[1][1], item[1][0] - item[1][1]))
    )
    top = totals.takeOrdered(k, key=lambda item: (-item[3], -item[1], item[0]))  # action
    return [
        {
            "rank": index + 1,
            "company": company,
            "recent_new": recent,
            "previous_new": previous,
            "momentum": momentum,
            "growth_pct": round(100.0 * momentum / max(previous, 1), 1),
            "window_minutes": window_minutes,
        }
        for index, (company, recent, previous, momentum) in enumerate(top)
    ]


def to_records(dataframe) -> List[Dict[str, Any]]:
    return [row.asDict(recursive=True) for row in dataframe.collect()]
