from datetime import datetime, timedelta, timezone

from src.analytics.duckdb_queries import LakeQueries
from src.generator.job_generator import JobEventSimulator
from src.storage import parquet_store
from src.storage.hbase_store import HBaseJobStore
from src.streaming.kafka_producer import JsonSerializer, KafkaJobProducer
from src.streaming.pipeline import StreamPipeline


def _event(job_id, event, minutes, **overrides):
    moment = datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=minutes)
    record = {
        "job_id": job_id,
        "company": "Google",
        "title": "Data Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1500000,
        "timestamp": moment.isoformat(),
        "event": event,
    }
    record.update(overrides)
    return record


def test_pipeline_writes_lake_quarantine_hbase_and_status(tmp_path):
    store = HBaseJobStore()
    pipeline = StreamPipeline(store, tmp_path)
    summary = pipeline.process_raw_records(
        [
            _event("J1", "NEW", 0),
            _event("J2", "NEW", 1, company="Amazon", title="QA Engineer", salary=None),
            _event("J1", "MODIFIED", 2, salary=1700000),
            _event("J1", "NEW", 3, salary=1700000),
            _event("J2", "CLOSED", 4, company="Amazon", title="QA Engineer", salary=None),
            "{broken",
            _event("J3", "PROMOTED", 5),
        ],
        batch_id=0,
    )

    assert summary["processed"] == 5
    assert summary["rejected"] == 2
    assert summary["detected"] == {"NEW": 2, "MODIFIED": 1, "DUPLICATE": 1, "CLOSED": 1}
    assert store.get_job("J2")["status"] == "CLOSED"

    events = parquet_store.read_dataset(tmp_path, parquet_store.EVENTS_DATASET)
    assert len(events) == 5
    assert set(events["category"]) == {"Data", "Quality"}
    quarantine = parquet_store.read_dataset(tmp_path, parquet_store.QUARANTINE_DATASET)
    assert sorted(quarantine["validation_error"]) == ["invalid_event", "malformed_json"]
    assert not list(tmp_path.rglob("*.tmp"))

    # Totals survive a restart through the status file.
    restarted = StreamPipeline(HBaseJobStore(), tmp_path)
    assert restarted.totals["RECEIVED"] == 7
    assert restarted.totals["INVALID"] == 2


def test_duckdb_queries_over_lake(tmp_path):
    pipeline = StreamPipeline(HBaseJobStore(), tmp_path)
    pipeline.process_raw_records(
        [
            _event("J1", "NEW", 0),
            _event("J2", "NEW", 1, company="Amazon", title="Machine Learning Engineer", salary=2500000),
            _event("J3", "NEW", 70, location="Pune"),
            _event("J1", "CLOSED", 80),
            {"job_id": "J9"},
        ],
        batch_id=0,
    )
    parquet_store.merge_window_counts(
        [{"window_start": "2026-10-06T10:00:00Z", "window_end": "2026-10-06T10:05:00Z", "event": "NEW", "count": 2}],
        tmp_path,
    )
    parquet_store.merge_window_counts(
        [{"window_start": "2026-10-06T10:00:00Z", "window_end": "2026-10-06T10:05:00Z", "event": "NEW", "count": 3}],
        tmp_path,
    )

    queries = LakeQueries(tmp_path)
    kpis = queries.kpis(recent_minutes=5)
    assert kpis["total_events"] == 4
    assert kpis["jobs_tracked"] == 3
    assert kpis["new_recent"] == 3  # processed just now
    assert kpis["new_recent_by_event_time"] == 0  # newest NEW is 10 min before the newest event
    assert kpis["closed"] == 1
    assert queries.top_companies(1) == [{"company": "Google", "new_postings": 2}]
    assert {row["label"] for row in queries.trend("category")} == {"Data", "AI / ML"}
    assert len({row["bucket"] for row in queries.trend("location", "hour")}) == 2
    salary = {row["category"]: row["avg_salary"] for row in queries.avg_salary_by_category()}
    assert salary["AI / ML"] == 2500000
    assert [row["count"] for row in queries.window_counts()] == [3]
    assert queries.quarantine_summary()[0]["error"] == "missing_fields"
    assert queries.pipeline_status()["totals"]["RECEIVED"] == 5
    queries.close()


def test_window_counts_from_before_distinct_columns_still_read_and_merge(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    legacy = pa.schema([
        ("window_start", pa.timestamp("us", tz="UTC")),
        ("window_end", pa.timestamp("us", tz="UTC")),
        ("event", pa.string()),
        ("count", pa.int64()),
    ])
    target = tmp_path / parquet_store.WINDOW_DATASET / "window_counts.parquet"
    target.parent.mkdir(parents=True)
    start = parquet_store.to_utc_datetime("2026-10-06T10:00:00Z")
    end = parquet_store.to_utc_datetime("2026-10-06T10:05:00Z")
    pq.write_table(
        pa.Table.from_pylist([{"window_start": start, "window_end": end, "event": "NEW", "count": 2}], schema=legacy),
        target,
    )

    queries = LakeQueries(tmp_path)
    assert [(row["count"], row["unique_jobs"]) for row in queries.window_counts()] == [(2, None)]

    parquet_store.merge_window_counts(
        [{"window_start": "2026-10-06T10:05:00Z", "window_end": "2026-10-06T10:10:00Z", "event": "NEW",
          "count": 4, "unique_jobs": 3, "unique_companies": 2}],
        tmp_path,
    )
    rows = queries.window_counts()
    assert [(row["count"], row["unique_jobs"], row["unique_companies"]) for row in rows] == [(2, None, None), (4, 3, 2)]
    queries.close()


def test_empty_lake_queries_are_safe(tmp_path):
    queries = LakeQueries(tmp_path)
    assert queries.has_events() is False
    assert queries.kpis()["total_events"] == 0
    assert queries.recent_events() == []
    assert queries.window_counts() == []
    assert queries.analytics_table("rdd_top_companies") == []
    queries.close()


def test_simulator_events_flow_through_pipeline(tmp_path):
    simulator = JobEventSimulator(seed=11, invalid_rate=0.1)
    events = simulator.generate(400, start=datetime(2026, 10, 6, tzinfo=timezone.utc), span=timedelta(hours=4))
    summary = StreamPipeline(HBaseJobStore(), tmp_path).process_raw_records(events, batch_id=0)
    assert summary["processed"] + summary["rejected"] == 400
    assert summary["rejected"] > 0
    assert set(summary["detected"]) <= {"NEW", "MODIFIED", "DUPLICATE", "CLOSED"}


def test_producer_keys_by_job_id_and_passes_raw_payloads_through():
    assert KafkaJobProducer.message_key({"job_id": "J7"}) == "J7"
    assert KafkaJobProducer.message_key("{broken") is None
    serializer = JsonSerializer()
    assert serializer.serialize("topic", "{broken") == b"{broken"
    assert serializer.serialize("topic", {"a": 1}) == b'{"a": 1}'


def test_replaying_a_batch_is_idempotent(tmp_path):
    store = HBaseJobStore.connect("local", path=tmp_path / "hbase.sqlite3")
    pipeline = StreamPipeline(store, tmp_path / "lake", source="test")
    batch = [_event("J1", "NEW", 0), _event("J1", "MODIFIED", 1, salary=1700000), _event("J2", "NEW", 2), "{bad"]

    first = pipeline.process_raw_records(batch, batch_id=0)
    stats_before = store.company_stats()
    replay = pipeline.process_raw_records(batch, batch_id=0)  # e.g. Spark re-runs the batch after a crash

    # Same classification (not re-detected as DUPLICATE), no double counting.
    assert replay["detected"] == first["detected"] == {"NEW": 2, "MODIFIED": 1}
    assert replay["replay"] is True
    assert store.company_stats() == stats_before
    assert pipeline.totals["RECEIVED"] == 4
    events = parquet_store.read_dataset(tmp_path / "lake")
    assert len(events) == 3  # the replay overwrote its own files instead of appending
    assert len(parquet_store.read_dataset(tmp_path / "lake", parquet_store.QUARANTINE_DATASET)) == 1
    assert len(store.get_history("J1")) == 2
    store.close()


def test_event_ids_are_deterministic():
    from src.streaming.change_detection import event_id_for

    kafka_record = {"job_id": "J1", "kafka_topic": "job-events", "kafka_partition": 2, "kafka_offset": 41}
    assert event_id_for(kafka_record) == "job-events-2-41"
    content = {"job_id": "J1", "event_time": "t", "event": "NEW", "fingerprint": "f"}
    assert event_id_for(content) == event_id_for(dict(content))
    assert event_id_for(content) != event_id_for({**content, "event_time": "t2"})
