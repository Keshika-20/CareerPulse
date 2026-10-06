from src.generator.job_generator import generate_batch, generate_job_event


def test_generate_job_event_has_required_fields():
    event = generate_job_event(event_type="NEW")
    required = {"job_id", "company", "title", "location", "experience", "salary", "timestamp", "event"}
    assert set(event.keys()) == required
    assert event["event"] == "NEW"


def test_generate_batch_count():
    batch = generate_batch(count=5)
    assert len(batch) == 5
    assert all("job_id" in event for event in batch)
