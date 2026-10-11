import pytest

from src.common.models import validate_record
from src.generator.job_generator import (
    generate_batch,
    generate_job_event,
    generate_job_lifecycle,
)


def test_generate_job_event_has_required_fields():
    event = generate_job_event(event_type="NEW")
    required = {"job_id", "company", "title", "location", "experience", "salary", "timestamp", "event"}
    assert set(event.keys()) == required
    assert event["event"] == "NEW"


def test_generate_batch_count():
    batch = generate_batch(count=5)
    assert len(batch) == 5
    assert all("job_id" in event for event in batch)


def test_generate_job_lifecycle_preserves_order_and_job_identity():
    lifecycle = generate_job_lifecycle(
        ["NEW", "DUPLICATE", "MODIFIED", "DUPLICATE", "CLOSED"],
        job_id="J1234",
        seed=7,
    )

    assert [event["event"] for event in lifecycle] == [
        "NEW",
        "DUPLICATE",
        "MODIFIED",
        "DUPLICATE",
        "CLOSED",
    ]
    assert {event["job_id"] for event in lifecycle} == {"J1234"}
    assert all(validate_record(event) == (True, None) for event in lifecycle)


def test_generate_job_lifecycle_modification_changes_job_details():
    lifecycle = generate_job_lifecycle(["NEW", "MODIFIED"], seed=11)
    tracked_fields = {"company", "title", "location", "experience", "salary"}

    changed_fields = {
        field
        for field in tracked_fields
        if lifecycle[0][field] != lifecycle[1][field]
    }
    assert changed_fields


def test_generate_job_lifecycle_duplicate_preserves_job_details():
    lifecycle = generate_job_lifecycle(["NEW", "MODIFIED", "DUPLICATE"], seed=19)
    tracked_fields = ("company", "title", "location", "experience", "salary")

    assert all(
        lifecycle[1][field] == lifecycle[2][field]
        for field in tracked_fields
    )


def test_generate_job_lifecycle_closed_event_refers_to_same_job():
    lifecycle = generate_job_lifecycle(["NEW", "MODIFIED", "CLOSED"], seed=23)
    tracked_fields = ("company", "title", "location", "experience", "salary")

    assert lifecycle[-1]["event"] == "CLOSED"
    assert lifecycle[-1]["job_id"] == lifecycle[0]["job_id"]
    assert all(
        lifecycle[-1][field] == lifecycle[-2][field]
        for field in tracked_fields
    )


@pytest.mark.parametrize(
    ("event_types", "job_id"),
    [
        ([], None),
        (["MODIFIED", "NEW"], None),
        (["NEW", "UNKNOWN"], None),
        (["NEW", "CLOSED", "DUPLICATE"], None),
        (["NEW", None], None),
        (["NEW"], ""),
    ],
)
def test_generate_job_lifecycle_rejects_invalid_arguments(event_types, job_id):
    with pytest.raises(ValueError):
        generate_job_lifecycle(event_types, job_id=job_id, seed=1)
