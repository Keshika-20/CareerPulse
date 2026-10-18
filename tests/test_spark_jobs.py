"""Spark Structured Streaming ETL, Spark SQL, RDD and MLlib tests.

Everything runs on in-memory DataFrames, so no Kafka broker or Hadoop
filesystem (winutils on Windows) is needed.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from src.analytics import spark_analytics  # noqa: E402
from src.analytics.demand_model import build_demand_features, forecast_demand  # noqa: E402
from src.common.spark_session import build_spark_session  # noqa: E402
from src.generator.job_generator import JobEventSimulator  # noqa: E402
from src.storage import parquet_store  # noqa: E402
from src.storage.hbase_store import HBaseJobStore  # noqa: E402
from src.streaming.etl import clean_and_validate  # noqa: E402
from src.streaming.pipeline import StreamPipeline  # noqa: E402
from src.streaming.spark_kafka_consumer import (  # noqa: E402
    apply_etl,
    make_batch_processor,
    parse_kafka_values,
    window_counts,
)

BASE = {
    "job_id": "J1",
    "company": " Google ",
    "title": "Machine  Learning Engineer",
    "location": "Pune",
    "experience": "2-4 years",
    "salary": 1800000,
    "timestamp": "2026-10-06T10:00:00Z",
    "event": "new",
}
RAW_SAMPLES = [
    json.dumps(BASE),
    json.dumps({**BASE, "salary": None, "experience": None}),
    json.dumps({**BASE, "event": None, "event_type": "closed"}),
    json.dumps({**BASE, "job_id": 1024}),
    "{not json",
    '"a json string"',
    json.dumps({**BASE, "company": ""}),
    json.dumps({key: value for key, value in BASE.items() if key != "title"}),
    json.dumps({**BASE, "event": "PROMOTED"}),
    json.dumps({**BASE, "salary": "twelve lakh"}),
    json.dumps({**BASE, "salary": -5}),
    json.dumps({**BASE, "salary": True}),
    json.dumps({**BASE, "timestamp": "yesterday"}),
]


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session("CareerPulse tests", "local[2]")
    yield session
    session.stop()


def _kafka_frame(spark, values):
    rows = [(value.encode("utf-8"), "job-events", offset % 3, offset) for offset, value in enumerate(values)]
    return spark.createDataFrame(rows, "value binary, topic string, partition int, offset long")


def _events_frame(spark, records):
    import pandas as pd

    frame = pd.DataFrame(records)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    return spark.createDataFrame(frame)


def test_spark_etl_matches_python_rules(spark):
    rows = apply_etl(parse_kafka_values(_kafka_frame(spark, RAW_SAMPLES))).orderBy("kafka_offset").collect()

    for raw, row in zip(RAW_SAMPLES, rows):
        expected, error = clean_and_validate(raw)
        assert row["validation_error"] == error, raw
        if expected is not None:
            assert row["company"] == expected["company"]
            assert row["title"] == expected["title"]
            assert row["event"] == expected["event"]
            assert row["salary"] == expected["salary"]
            assert row["category"] == expected["category"]
            assert row["event_time_iso"].replace("Z", "+00:00").startswith(expected["event_time"][:19])


def test_foreach_batch_handler_runs_change_detection_and_storage(spark, tmp_path):
    hbase = {"mode": "local", "path": str(tmp_path / "hbase.sqlite3")}
    process = make_batch_processor(hbase=hbase, lake_root=tmp_path / "lake", num_partitions=2, source="test")
    values = [
        json.dumps(BASE),
        json.dumps({**BASE, "salary": 2000000, "event": "MODIFIED", "timestamp": "2026-10-06T10:05:00Z"}),
        json.dumps({**BASE, "salary": 2000000, "event": "NEW", "timestamp": "2026-10-06T10:06:00Z"}),
        "{not json",
    ]
    batch = apply_etl(parse_kafka_values(_kafka_frame(spark, values)))
    process(batch, 0)
    process(batch, 0)  # replay of the same micro-batch must not change anything

    events = parquet_store.read_dataset(tmp_path / "lake")
    assert list(events.sort_values("event_time")["event"]) == ["NEW", "MODIFIED", "DUPLICATE"]
    assert set(events["event_id"]) == {"job-events-0-0", "job-events-1-1", "job-events-2-2"}
    store = HBaseJobStore.connect("local", path=hbase["path"])
    assert store.get_job("J1")["salary"] == 2000000.0
    assert store.company_stats()[0]["NEW"] == 1
    store.close()
    assert len(parquet_store.read_dataset(tmp_path / "lake", parquet_store.QUARANTINE_DATASET)) == 1


def test_parallel_partitions_match_sequential_processing(spark, tmp_path):
    raw = JobEventSimulator(seed=13, invalid_rate=0.05).generate(
        600, start=datetime(2026, 10, 6, tzinfo=timezone.utc), span=timedelta(hours=2)
    )
    values = [value if isinstance(value, str) else json.dumps(value) for value in raw]
    batch = apply_etl(parse_kafka_values(_kafka_frame(spark, values)))

    results = {}
    for partitions in (1, 4):
        root = tmp_path / f"p{partitions}"
        hbase = {"mode": "local", "path": str(root / "hbase.sqlite3")}
        make_batch_processor(hbase=hbase, lake_root=root / "lake", num_partitions=partitions, source="t")(batch, 0)
        store = HBaseJobStore.connect("local", path=hbase["path"])
        events = parquet_store.read_dataset(root / "lake").sort_values("event_id")
        results[partitions] = (
            list(zip(events["event_id"], events["event"])),
            {job: state["status"] for job, state in store.list_jobs().items()},
            sorted(map(str, store.company_stats())),
        )
        store.close()

    # Partitioning by job_id makes parallel processing produce exactly the sequential result.
    assert results[1] == results[4]
    assert len(results[4][0]) > 500


def test_window_counts_slide_over_event_time(spark):
    values = [
        json.dumps({**BASE, "job_id": f"J{index}", "timestamp": f"2026-10-06T10:0{index}:30Z"})
        for index in range(3)
    ]
    valid = apply_etl(parse_kafka_values(_kafka_frame(spark, values))).where("validation_error IS NULL")
    windows = {
        (row["window_start"], row["count"])
        for row in window_counts(valid, watermark="10 minutes", window="2 minutes", slide="1 minute").collect()
    }
    assert ("2026-10-06T10:00:00.000000Z", 2) in windows
    assert ("2026-10-06T10:02:00.000000Z", 1) in windows


def test_window_counts_estimate_distinct_jobs_and_companies(spark):
    values = [
        json.dumps({**BASE, "job_id": "J0", "timestamp": "2026-10-06T10:00:10Z"}),
        json.dumps({**BASE, "job_id": "J0", "timestamp": "2026-10-06T10:00:20Z"}),  # same job again
        json.dumps({**BASE, "job_id": "J1", "company": "Amazon", "timestamp": "2026-10-06T10:00:30Z"}),
    ] + [
        json.dumps({**BASE, "job_id": f"K{index}", "company": f"Co{index % 40}", "timestamp": "2026-10-06T10:10:00Z"})
        for index in range(500)
    ]
    valid = apply_etl(parse_kafka_values(_kafka_frame(spark, values))).where("validation_error IS NULL")
    rows = {
        row["window_start"]: row
        for row in window_counts(valid, watermark="10 minutes", window="1 minute", slide="1 minute").collect()
    }
    small = rows["2026-10-06T10:00:00.000000Z"]
    assert (small["count"], small["unique_jobs"], small["unique_companies"]) == (3, 2, 2)
    large = rows["2026-10-06T10:10:00.000000Z"]
    assert large["count"] == 500
    assert abs(large["unique_jobs"] - 500) <= 500 * 0.06  # 3 x the configured 2% rsd
    assert abs(large["unique_companies"] - 40) <= 2


def _lake_records():
    start = datetime(2026, 10, 6, 8, 0, tzinfo=timezone.utc)
    records = []
    plan = [("Google", "NEW", 0), ("Google", "NEW", 10), ("Amazon", "NEW", 20), ("Google", "MODIFIED", 30),
            ("Amazon", "NEW", 70), ("Amazon", "NEW", 80), ("Amazon", "NEW", 90), ("Uber", "CLOSED", 95),
            ("Uber", "NEW", 100)]
    for index, (company, event, minutes) in enumerate(plan):
        records.append({
            "event_id": str(index), "job_id": f"J{index}", "company": company,
            "title": "Data Engineer" if company != "Uber" else "Product Manager",
            "location": "Pune", "experience": "2-4 years", "salary": 1000000.0 + index * 100000,
            "category": "Data" if company != "Uber" else "Product", "event": event,
            "event_time": (start + timedelta(minutes=minutes)).isoformat(),
        })
    return records


def test_spark_sql_hiring_trends(spark):
    results = spark_analytics.hiring_trends(spark, _events_frame(spark, _lake_records()), recent_minutes=15)
    top = spark_analytics.to_records(results["top_companies_new"])
    assert top[0] == {"company": "Amazon", "new_postings": 4}
    assert spark_analytics.to_records(results["recent_new_jobs"])[0]["new_jobs"] == 2
    salary = {row["category"]: row["avg_salary"] for row in spark_analytics.to_records(results["avg_salary_by_category"])}
    assert salary["Product"] == 1800000
    changes = {row["company"]: row for row in spark_analytics.to_records(results["modified_closed_by_company"])}
    assert changes["Google"]["modified"] == 1 and changes["Uber"]["closed"] == 1


def test_rdd_top_k_and_trending_companies(spark):
    events = _events_frame(spark, _lake_records())
    assert spark_analytics.top_k_companies_rdd(events, k=2) == [
        {"rank": 1, "company": "Amazon", "new_postings": 4},
        {"rank": 2, "company": "Google", "new_postings": 2},
    ]
    trending = spark_analytics.trending_companies_rdd(events, k=3, window_minutes=60)
    assert [row["company"] for row in trending] == ["Amazon", "Uber", "Google"]
    assert (trending[0]["recent_new"], trending[0]["previous_new"], trending[0]["momentum"]) == (3, 1, 2)
    assert trending[0]["growth_pct"] == 200.0
    assert trending[2]["momentum"] == -2


def test_demand_model_trains_and_forecasts(spark, tmp_path):
    simulator = JobEventSimulator(seed=5)
    raw = simulator.generate(2500, start=datetime(2026, 10, 5, tzinfo=timezone.utc), span=timedelta(hours=24))
    StreamPipeline(HBaseJobStore(), tmp_path).process_raw_records(raw, batch_id=0)
    events = spark_analytics.load_events(spark, tmp_path)

    features = build_demand_features(events, bucket_minutes=60)
    assert {"lag_1", "lag_2", "lag_3", "label", "hour_sin"} <= set(features.columns)

    result = forecast_demand(events, bucket_minutes=60)
    assert result["status"] == "trained"
    assert result["rmse"] >= 0
    assert {row["category"] for row in result["predictions"]} == set(
        row["category"] for row in events.select("category").distinct().collect()
    )
    assert all(row["predicted_next_new"] >= 0 for row in result["predictions"])


def test_demand_model_reports_insufficient_data(spark):
    result = forecast_demand(_events_frame(spark, _lake_records()), bucket_minutes=60)
    assert result["status"] == "insufficient_data"
    assert result["predictions"] == []
