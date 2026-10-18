import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from src.generator.job_generator import JobEventSimulator
from src.storage import parquet_store
from src.storage.hbase_store import HBaseJobStore
from src.streaming.alerts import AlertSink, category_windows, data_quality_alert, detect_surges
from src.streaming.etl import clean_and_validate
from src.streaming.pipeline import StreamPipeline

START = datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc)


def _window(minutes, category, new):
    start = START + timedelta(minutes=minutes)
    return {"window_start": start, "window_end": start + timedelta(minutes=5), "category": category,
            "new_postings": new}


# ---- enrichment of high-dimensional, semi-structured and unstructured fields

def test_enrichment_standardises_and_extracts_skills():
    record, error = clean_and_validate({
        "job_id": "J1", "company": "Acme", "title": "Data Engineer", "location": "Pune",
        "experience": "2-4 years", "salary": 1500000, "timestamp": "2026-10-06T10:00:00Z", "event": "NEW",
        "employment_type": " full time ", "work_mode": "WFH",
        "requirements": {"skills": ["PySpark", "py", " SQL "], "education": "B.Tech"},
        "description": "Build pipelines on Kafka and Apache Spark; Airflow experience is a plus.",
    })
    assert error is None
    assert record["employment_type"] == "Full-time"
    assert record["work_mode"] == "Remote"
    assert record["education"] == "B.Tech"
    assert record["skills"] == ["airflow", "kafka", "python", "spark", "sql"]
    assert record["skill_count"] == 5


def test_optional_fields_never_reject_and_remote_location_implies_work_mode():
    record, error = clean_and_validate({
        "job_id": "J2", "company": "Acme", "title": "QA Engineer", "location": "Remote",
        "experience": None, "salary": None, "timestamp": "2026-10-06T10:00:00Z", "event": "NEW",
        "requirements": "not an object", "employment_type": "gig",
    })
    assert error is None
    assert record["work_mode"] == "Remote"
    assert record["employment_type"] == "Other"
    assert record["skills"] == [] and record["education"] is None


def test_simulator_emits_enriched_events_and_surges():
    events = JobEventSimulator(seed=4).generate(
        2000, start=START, span=timedelta(hours=10), surge=("Data Engineer", 0.5, 0.6)
    )
    new = [event for event in events if event["event"] == "NEW"]
    assert all({"employment_type", "work_mode", "requirements", "description"} <= set(event) for event in new)
    surge = [event for event in new if START + timedelta(hours=5) <= datetime.fromisoformat(event["timestamp"]) < START + timedelta(hours=6)]
    before = [event for event in new if datetime.fromisoformat(event["timestamp"]) < START + timedelta(hours=5)]
    share = lambda items: sum(item["title"] == "Data Engineer" for item in items) / len(items)  # noqa: E731
    assert share(surge) > 3 * share(before)


# ---- monitoring and notification

def test_category_windows_slide_per_minute():
    events = [
        {"event_time": (START + timedelta(minutes=2, seconds=10)).isoformat(), "category": "Data",
         "event": "NEW", "claimed_event": "NEW", "salary": 100.0, "company": "A"},
        {"event_time": (START + timedelta(minutes=4)).isoformat(), "category": "Data",
         "event": "CLOSED", "claimed_event": "CLOSED", "salary": None, "company": "B"},
    ]
    windows = {row["window_start"]: row for row in category_windows(events)}
    first = windows[START]  # [10:00, 10:05) holds both events
    assert (first["new_postings"], first["closed_postings"], first["events"]) == (1, 1, 2)
    assert first["avg_salary"] == 100.0 and first["active_companies"] == 2
    assert windows[START + timedelta(minutes=3)]["events"] == 1  # [10:03, 10:08) only the closure
    assert len(windows) == 7  # starts 09:58 ... 10:04


def test_detect_surges_flags_a_spike_but_not_noise():
    history = [_window(minute, "Data", 4) for minute in range(0, 60, 5)]
    history += [_window(minute, "Data", 5) for minute in range(1, 60, 5)]  # unaligned windows are ignored
    spike = [_window(60, "Data", 20)]
    blip = [_window(60, "Data", 8)]

    alerts = detect_surges(spike, history + spike)
    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "HIRING_SURGE" and alerts[0]["baseline"] == 4.0
    assert alerts[0]["alert_id"] == "surge:Data:2026-10-06T11:00:00+00:00"
    assert detect_surges(blip, history + blip) == []  # 8 < 4 + 3*sqrt(4)=10
    assert detect_surges([_window(61, "Data", 50)], history) == []  # not an aligned window
    assert detect_surges(spike, spike) == []  # no baseline yet


def test_data_quality_alert_threshold():
    assert data_quality_alert({"processed": 70, "rejected": 30, "source": "s", "batch_id": 4})["alert_id"] == "quality:s:4"
    assert data_quality_alert({"processed": 95, "rejected": 5, "source": "s", "batch_id": 4}) is None
    assert data_quality_alert({"processed": 5, "rejected": 5, "source": "s", "batch_id": 4}) is None  # too small


def test_alert_sink_deduplicates_and_calls_webhook(tmp_path):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        alert = data_quality_alert({"processed": 50, "rejected": 50, "source": "s", "batch_id": 1})
        sink = AlertSink(tmp_path, webhook_url=f"http://127.0.0.1:{server.server_port}/hook")
        assert len(sink.publish([alert])) == 1
        assert sink.publish([alert]) == []  # same alert_id never fires twice
        assert AlertSink(tmp_path).publish([alert]) == []  # ... even from a new process
    finally:
        server.shutdown()
    assert len(received) == 1 and received[0]["alert"]["alert_type"] == "DATA_QUALITY"
    assert len(parquet_store.read_dataset(tmp_path, parquet_store.ALERTS_DATASET)) == 1


# ---- cold storage and verification

def test_raw_archive_and_verification(tmp_path):
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from verify_pipeline import verify

    store = HBaseJobStore.connect("local", path=tmp_path / "hbase.sqlite3")
    pipeline = StreamPipeline(store, tmp_path / "lake", source="t")
    events = JobEventSimulator(seed=8, invalid_rate=0.1).generate(300, start=START, span=timedelta(hours=1))
    pipeline.process_raw_records(events[:150], batch_id=0)
    pipeline.process_raw_records(events[150:], batch_id=1)

    raw = parquet_store.read_dataset(tmp_path / "lake", parquet_store.RAW_DATASET)
    assert len(raw) == 300 and raw["event_id"].is_unique
    quarantine = parquet_store.read_dataset(tmp_path / "lake", parquet_store.QUARANTINE_DATASET)
    assert quarantine["event_id"].notna().all()

    results = verify(tmp_path / "lake", store=store)
    assert all(result["status"] == "PASS" for result in results), results

    # A record processed twice (e.g. a non-idempotent writer) must be caught.
    events_frame = parquet_store.read_dataset(tmp_path / "lake")
    parquet_store.append_events(events_frame.head(1).to_dict("records"), tmp_path / "lake", label="dup")
    failed = {result["check"] for result in verify(tmp_path / "lake", store=store) if result["status"] == "FAIL"}
    assert failed == {"processed unique"}
    store.close()


def test_verify_detects_kafka_offsets_missing_from_archive(tmp_path):
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from verify_pipeline import verify

    parquet_store.append_raw(
        [{"event_id": f"job-events-0-{offset}", "kafka_topic": "job-events", "kafka_partition": 0,
          "kafka_offset": offset, "kafka_timestamp": START.isoformat(), "raw_value": "{}",
          "received_at": START.isoformat()} for offset in (0, 1, 3)],
        tmp_path, label="x",
    )
    coverage = next(r for r in verify(tmp_path, offsets={0: (0, 4)}) if r["check"] == "kafka coverage")
    assert coverage["status"] == "FAIL" and "1 missing" in coverage["detail"]

