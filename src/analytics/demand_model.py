"""Simple job-demand forecasting with Spark MLlib.

Target: the number of NEW postings a job category receives in the next time
bucket. Features per (category, bucket): NEW counts in the three previous
buckets, a rolling mean, CLOSED count, average salary, the time of day
(sine/cosine) and the one-hot category. A linear regression is trained on the
earlier 80% of buckets and evaluated on the most recent 20% against a naive
"same as last bucket" baseline, then used to forecast the next bucket.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

NUMERIC_FEATURES = ["lag_1", "lag_2", "lag_3", "rolling_mean_3", "closed_lag_1", "avg_salary_lakh", "hour_sin", "hour_cos"]
MIN_TRAINING_ROWS = 12


def build_demand_features(events_df: DataFrame, bucket_minutes: int = 60) -> DataFrame:
    """One row per (category, bucket) with lag features and the next-bucket label.

    Buckets with no activity are filled with zeros so the lags are true
    time-series lags rather than "previous non-empty bucket".
    """
    seconds = int(bucket_minutes) * 60
    bucketed = events_df.withColumn(
        "bucket_epoch",
        (F.floor(F.unix_timestamp("event_time") / seconds) * seconds).cast("long"),
    )
    activity = bucketed.groupBy("category", "bucket_epoch").agg(
        F.sum(F.when(F.col("event") == "NEW", 1).otherwise(0)).alias("new_count"),
        F.sum(F.when(F.col("event") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.avg(F.when(F.col("event").isin("NEW", "MODIFIED"), F.col("salary"))).alias("avg_salary"),
    )

    bounds = bucketed.agg(F.min("bucket_epoch").alias("lo"), F.max("bucket_epoch").alias("hi"))
    grid = (
        bounds.select(F.explode(F.sequence("lo", "hi", F.lit(seconds).cast("long"))).alias("bucket_epoch"))
        .crossJoin(bucketed.select("category").distinct())
    )
    series = (
        grid.join(activity, ["category", "bucket_epoch"], "left")
        .fillna({"new_count": 0, "closed_count": 0})
    )

    by_time = Window.partitionBy("category").orderBy("bucket_epoch")
    salary_fill = by_time.rowsBetween(Window.unboundedPreceding, 0)
    hour = (F.col("bucket_epoch") % 86400) / 3600.0
    return (
        series
        .withColumn("avg_salary", F.last("avg_salary", ignorenulls=True).over(salary_fill))
        .withColumn("lag_1", F.lag("new_count", 1).over(by_time))
        .withColumn("lag_2", F.lag("new_count", 2).over(by_time))
        .withColumn("lag_3", F.lag("new_count", 3).over(by_time))
        .withColumn("closed_lag_1", F.lag("closed_count", 1).over(by_time))
        .withColumn("rolling_mean_3", (F.col("lag_1") + F.col("lag_2") + F.col("lag_3")) / 3.0)
        .withColumn("label", F.lead("new_count", 1).over(by_time).cast("double"))
        .withColumn("avg_salary_lakh", F.coalesce(F.col("avg_salary") / 100000.0, F.lit(0.0)))
        .withColumn("hour_sin", F.sin(2 * math.pi * hour / 24))
        .withColumn("hour_cos", F.cos(2 * math.pi * hour / 24))
        .withColumn("bucket_start", F.from_unixtime("bucket_epoch", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .where(F.col("lag_3").isNotNull())
    )


def _forecast_rows(features: DataFrame) -> DataFrame:
    """Rows describing each category's *next* bucket, built from its latest one."""
    latest = Window.partitionBy("category").orderBy(F.col("bucket_epoch").desc())
    last = features.withColumn("rn", F.row_number().over(latest)).where("rn = 1").drop("rn")
    return (
        last
        .withColumn("lag_3", F.col("lag_2"))
        .withColumn("lag_2", F.col("lag_1"))
        .withColumn("lag_1", F.col("new_count").cast("double"))
        .withColumn("closed_lag_1", F.col("closed_count"))
        .withColumn("rolling_mean_3", (F.col("lag_1") + F.col("lag_2") + F.col("lag_3")) / 3.0)
    )


def train_demand_model(features: DataFrame, bucket_minutes: int = 60) -> Dict[str, Any]:
    labelled = features.where(F.col("label").isNotNull()).cache()
    rows = labelled.count()
    if rows < MIN_TRAINING_ROWS:
        return {
            "status": "insufficient_data",
            "message": f"Need at least {MIN_TRAINING_ROWS} labelled (category, bucket) rows; found {rows}. "
                       "Stream for longer, use a smaller --bucket-minutes, or backfill history.",
            "training_rows": rows,
            "predictions": [],
        }

    cutoff = labelled.approxQuantile("bucket_epoch", [0.8], 0.0)[0]
    train = labelled.where(F.col("bucket_epoch") <= cutoff)
    test = labelled.where(F.col("bucket_epoch") > cutoff)
    if test.count() == 0:
        train, test = labelled, labelled

    pipeline = Pipeline(stages=[
        StringIndexer(inputCol="category", outputCol="category_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["category_index"], outputCols=["category_vec"]),
        VectorAssembler(inputCols=NUMERIC_FEATURES + ["category_vec"], outputCol="features"),
        LinearRegression(featuresCol="features", labelCol="label", regParam=0.1, elasticNetParam=0.0),
    ])
    model = pipeline.fit(train)

    scored = model.transform(test)
    rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(scored)
    mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae").evaluate(scored)
    r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2").evaluate(scored)
    baseline = scored.withColumn("baseline", F.col("lag_1").cast("double"))
    baseline_rmse = RegressionEvaluator(labelCol="label", predictionCol="baseline", metricName="rmse").evaluate(baseline)

    next_bucket = model.transform(_forecast_rows(features))
    predictions = [
        {
            "category": row["category"],
            "last_bucket_start": row["bucket_start"],
            "last_bucket_new": int(row["new_count"]),
            "predicted_next_new": round(max(0.0, float(row["prediction"])), 2),
            "bucket_minutes": bucket_minutes,
        }
        for row in next_bucket.select("category", "bucket_start", "new_count", "prediction")
        .orderBy(F.col("prediction").desc()).collect()
    ]
    labelled.unpersist()
    return {
        "status": "trained",
        "model": "Spark MLlib LinearRegression",
        "target": f"NEW postings per category in the next {bucket_minutes}-minute bucket",
        "features": NUMERIC_FEATURES + ["category (one-hot)"],
        "training_rows": train.count(),
        "test_rows": test.count(),
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "r2": round(r2, 4),
        "baseline_rmse": round(baseline_rmse, 4),
        "predictions": predictions,
    }


def forecast_demand(events_df: DataFrame, bucket_minutes: int = 60) -> Dict[str, Any]:
    return train_demand_model(build_demand_features(events_df, bucket_minutes), bucket_minutes)


def prediction_records(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(result.get("predictions", []))
