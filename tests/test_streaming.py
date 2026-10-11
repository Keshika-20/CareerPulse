import pytest

from src.common.models import validate_record
from src.streaming import spark_stream_processor
from src.streaming.spark_stream_processor import (
    SparkJobStreamProcessor,
    aggregate_valid_event_counts,
    detect_change,
    summarize_event_stream,
)


@pytest.fixture(scope="module")
def spark_processor():
    processor = SparkJobStreamProcessor(master="local[1]")
    try:
        yield processor
    finally:
        processor.close()


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


def test_summarize_event_stream_does_not_initialize_spark(monkeypatch):
    def fail_if_spark_started(*args, **kwargs):
        pytest.fail("summarize_event_stream must not initialize Spark")

    monkeypatch.setattr(
        spark_stream_processor,
        "SparkJobStreamProcessor",
        fail_if_spark_started,
    )
    monkeypatch.setattr(spark_stream_processor, "SparkSession", None)

    records = [
        {"job_id": "J10", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J11", "company": "Amazon", "title": "Data Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "MODIFIED"},
    ]

    assert summarize_event_stream(records) == {"NEW": 1, "MODIFIED": 1}


def test_aggregate_valid_event_counts_accepts_valid_records():
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "MODIFIED"},
        {"job_id": "J3", "company": "Microsoft", "title": "Data Engineer", "location": "Remote", "experience": "4-6 years", "salary": 1500000, "timestamp": "2026-10-06T00:02:00Z", "event": "NEW"},
    ]
    summary = aggregate_valid_event_counts(records)
    assert summary == {"NEW": 2, "MODIFIED": 1}


def test_aggregate_valid_event_counts_ignores_invalid_records():
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "0-2 years", "salary": "not-a-number", "timestamp": "2026-10-06T00:01:00Z", "event": "NEW"},
        {"job_id": "J3", "company": "Microsoft", "title": "Data Engineer", "location": "Remote", "experience": "4-6 years", "salary": 1500000, "timestamp": "2026-10-06T00:02:00Z", "event": "UNKNOWN"},
    ]
    summary = aggregate_valid_event_counts(records)
    assert summary == {"NEW": 1}


def test_aggregate_valid_event_counts_empty_input():
    assert aggregate_valid_event_counts([]) == {}


def test_aggregate_valid_event_counts_mixed_types_are_normalized():
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "new"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "CLOSED"},
        {"job_id": "J3", "company": "Microsoft", "title": "Data Engineer", "location": "Remote", "experience": "4-6 years", "salary": 1500000, "timestamp": "2026-10-06T00:02:00Z", "event": "modified"},
    ]
    summary = aggregate_valid_event_counts(records)
    assert summary == {"NEW": 1, "CLOSED": 1, "MODIFIED": 1}


def test_process_batch_uses_spark_dataframe_aggregation(spark_processor):
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "MODIFIED"},
        {"job_id": "J3", "company": "Microsoft", "title": "Data Engineer", "location": "Remote", "experience": "4-6 years", "salary": 1500000, "timestamp": "2026-10-06T00:02:00Z", "event": "new"},
        {"job_id": "J4", "company": "Acme", "title": "Analyst", "location": "Remote", "experience": None, "salary": None, "timestamp": "2026-10-06T00:03:00Z", "event": "CLOSED"},
    ]

    assert spark_processor.process_batch(records) == {"NEW": 2, "MODIFIED": 1, "CLOSED": 1}


def test_process_batch_excludes_invalid_records(spark_processor):
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "0-2 years", "salary": "not-a-number", "timestamp": "2026-10-06T00:01:00Z", "event": "NEW"},
        {"job_id": "J3", "company": "Microsoft", "title": "Data Engineer", "location": "Remote", "experience": "4-6 years", "salary": 1500000, "timestamp": "2026-10-06T00:02:00Z", "event": "UNKNOWN"},
    ]

    assert spark_processor.process_batch(records) == {"NEW": 1}


def test_process_batch_normalizes_lowercase_event_names(spark_processor):
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1000000, "timestamp": "2026-10-06T00:00:00Z", "event": "new"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "modified"},
    ]

    assert spark_processor.process_batch(records) == {"NEW": 1, "MODIFIED": 1}


def test_process_batch_returns_empty_for_empty_input(spark_processor):
    assert spark_processor.process_batch([]) == {}


def test_detect_change_closed_event_takes_priority_over_field_changes():
    previous = {
        "job_id": "J100",
        "company": "Google",
        "title": "Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1000000,
    }
    current = {
        **previous,
        "company": "Microsoft",
        "event": "CLOSED",
    }

    assert detect_change(previous, current) == "CLOSED"


def test_detect_change_detects_salary_change():
    previous = {
        "company": "Google",
        "title": "Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1000000,
    }
    current = {
        **previous,
        "salary": 1500000,
        "event": "MODIFIED",
    }

    assert detect_change(previous, current) == "MODIFIED"


def test_detect_change_detects_location_change():
    previous = {
        "company": "Google",
        "title": "Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1000000,
    }
    current = {
        **previous,
        "location": "Chennai",
        "event": "MODIFIED",
    }

    assert detect_change(previous, current) == "MODIFIED"


def test_detect_change_ignores_timestamp_when_job_details_are_unchanged():
    previous = {
        "company": "Google",
        "title": "Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1000000,
        "timestamp": "2026-10-10T10:00:00Z",
    }
    current = {
        **previous,
        "timestamp": "2026-10-11T10:00:00Z",
        "event": "MODIFIED",
    }

    assert detect_change(previous, current) == "DUPLICATE"


def test_process_batch_returns_empty_when_all_records_are_invalid(spark_processor):
    records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": "invalid", "timestamp": "2026-10-06T00:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Engineer", "location": "Hyderabad", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T00:01:00Z", "event": "INVALID"},
    ]

    assert spark_processor.process_batch(records) == {}
