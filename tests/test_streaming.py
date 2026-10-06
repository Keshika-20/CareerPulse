from src.common.models import validate_record
from src.streaming.spark_stream_processor import detect_change, summarize_event_stream


def test_validate_record_accepts_valid_event():
    record = {
        "job_id": "J100",
        "company": "Google",
        "title": "Data Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1800000,
        "timestamp": "2026-10-06T12:00:00Z",
        "event": "NEW",
    }
    ok, error = validate_record(record)
    assert ok is True
    assert error is None


def test_detect_change_new_vs_existing():
    previous = {"company": "Google", "title": "Engineer"}
    current = {"company": "Google", "title": "Senior Engineer", "event": "MODIFIED"}
    assert detect_change(previous, current) == "MODIFIED"


def test_summarize_event_stream():
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "NEW"},
    ]
    summary = summarize_event_stream(records)
    assert summary["NEW"] == 2
