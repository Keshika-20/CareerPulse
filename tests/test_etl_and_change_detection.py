import json

import pytest

from src.common.models import derive_category, job_fingerprint
from src.storage.hbase_store import HBaseJobStore
from src.streaming.change_detection import ChangeDetector, changed_fields, detect_change
from src.streaming.etl import clean_and_validate

VALID = {
    "job_id": "J1024",
    "company": "  Google ",
    "title": "Software   Engineer",
    "location": "Bangalore",
    "experience": "0-2 years",
    "salary": 1200000,
    "timestamp": "2026-10-06T16:30:00Z",
    "event": "new",
}


def test_clean_and_validate_cleans_and_enriches():
    record, error = clean_and_validate(json.dumps(VALID))
    assert error is None
    assert record["company"] == "Google"
    assert record["title"] == "Software Engineer"
    assert record["event"] == "NEW"
    assert record["salary"] == 1200000.0
    assert record["event_time"] == "2026-10-06T16:30:00+00:00"
    assert record["category"] == "Engineering"
    assert record["fingerprint"] == job_fingerprint(record)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("{not json", "malformed_json"),
        ('"just a string"', "malformed_json"),
        ({**VALID, "company": "   "}, "missing_fields:company"),
        ({key: value for key, value in VALID.items() if key not in {"title", "event"}}, "missing_fields:title,event"),
        ({**VALID, "event": "PROMOTED"}, "invalid_event"),
        ({**VALID, "salary": "twelve lakh"}, "invalid_salary"),
        ({**VALID, "salary": -1}, "invalid_salary"),
        ({**VALID, "salary": True}, "invalid_salary"),
        ({**VALID, "timestamp": "yesterday"}, "invalid_timestamp"),
    ],
)
def test_clean_and_validate_error_codes(raw, expected):
    record, error = clean_and_validate(raw)
    assert record is None
    assert error == expected


def test_nullable_salary_and_experience_and_event_type_alias():
    record, error = clean_and_validate({**VALID, "salary": None, "experience": None, "event": None, "event_type": "closed"})
    assert error is None
    assert record["salary"] is None
    assert record["experience"] is None
    assert record["event"] == "CLOSED"


@pytest.mark.parametrize(
    "title, category",
    [
        ("Machine Learning Engineer", "AI / ML"),
        ("Senior Data Engineer", "Data"),
        ("DevOps Engineer", "DevOps / Cloud"),
        ("QA Engineer", "Quality"),
        ("Product Manager", "Product"),
        ("Backend Engineer", "Engineering"),
        ("Account Executive", "Sales & Marketing"),
        ("Office Coordinator", "Other"),
        (None, "Other"),
    ],
)
def test_derive_category(title, category):
    assert derive_category(title) == category


def test_detect_change_uses_stored_status():
    open_job = {"company": "Google", "title": "Engineer", "location": "Pune", "experience": None,
                "salary": 1.0, "status": "OPEN"}
    closed_job = {**open_job, "status": "CLOSED"}
    assert detect_change(None, {"event": "MODIFIED"}) == "NEW"
    assert detect_change(open_job, {**open_job, "event": "NEW"}) == "DUPLICATE"
    assert detect_change(open_job, {**open_job, "title": "Lead", "event": "NEW"}) == "MODIFIED"
    assert detect_change(closed_job, {**open_job, "event": "MODIFIED"}) == "NEW"
    assert detect_change(closed_job, {**open_job, "event": "CLOSED"}) == "DUPLICATE"
    assert changed_fields(open_job, {**open_job, "salary": 2.0, "location": "Delhi"}) == ["location", "salary"]


def test_change_detector_sees_earlier_events_in_the_same_batch():
    store = HBaseJobStore()
    base, _ = clean_and_validate(VALID)
    raised = {**base, "salary": 1500000.0, "event": "MODIFIED", "event_time": "2026-10-06T17:00:00+00:00"}
    resent = {**raised, "event": "NEW", "event_time": "2026-10-06T17:05:00+00:00"}
    closed = {**raised, "event": "CLOSED", "event_time": "2026-10-06T18:00:00+00:00"}

    processed = ChangeDetector(store).process([base, raised, resent, closed], batch_id=3)

    assert [event["event"] for event in processed] == ["NEW", "MODIFIED", "DUPLICATE", "CLOSED"]
    assert processed[1]["changed_fields"] == "salary"
    assert processed[2]["claimed_event"] == "NEW"
    assert {event["batch_id"] for event in processed} == {3}
    assert store.get_job("J1024")["status"] == "CLOSED"
