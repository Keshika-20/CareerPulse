from __future__ import annotations

import json

import pytest

from src.dashboard.app import load_ingested_dashboard_data
from src.ingestion.base_source import SourceFetchError
from src.ingestion.service import IngestionService, load_config
from src.ingestion.sqlite_store import SQLiteSnapshotStore


class QueueClient:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.urls = []

    def get_json(self, url):
        self.urls.append(url)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def greenhouse_payload(title="Engineer"):
    return {
        "jobs": [
            {
                "id": 123,
                "title": title,
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/123",
            }
        ],
        "meta": {"total": 1},
    }


def service_config(database_path):
    return {
        "sqlite_path": str(database_path),
        "raw_retention_days": 7,
        "request_timeout_seconds": 2,
        "max_retries": 0,
        "sources": {
            "greenhouse": [
                {
                    "enabled": True,
                    "board_token": "acme-board",
                    "company": "Acme",
                }
            ],
            "lever": [],
        },
    }


def test_service_runs_mocked_source_through_normalization_and_sqlite(tmp_path):
    database = tmp_path / "ingestion.sqlite3"
    service = IngestionService(
        service_config(database),
        http_client=QueueClient([greenhouse_payload()]),
    )

    result = service.run()

    assert result["failures"] == []
    assert result["runs"][0]["new"] == 1
    store = SQLiteSnapshotStore(database)
    events = store.list_events()
    assert [event["event"] for event in events] == ["NEW"]
    assert events[0]["payload"]["job_id"] == "greenhouse:acme-board:123"
    assert store.raw_snapshot_count() == 1
    store.close()


def test_failed_source_fetch_does_not_advance_snapshot_or_absence_state(tmp_path):
    database = tmp_path / "ingestion.sqlite3"
    initial = IngestionService(
        service_config(database),
        http_client=QueueClient([greenhouse_payload()]),
    )
    initial.run()

    failed = IngestionService(
        service_config(database),
        http_client=QueueClient([SourceFetchError("simulated source failure")]),
    )
    result = failed.run()

    assert result["runs"] == []
    assert result["failures"][0]["error"] == "simulated source failure"
    store = SQLiteSnapshotStore(database)
    snapshot = store.get_snapshot("greenhouse:acme-board:123")
    assert snapshot["is_active"] is True
    assert snapshot["absence_count"] == 0
    assert len(store.list_runs()) == 1
    store.close()


def test_service_rejects_all_disabled_config_without_network_or_database(tmp_path):
    config = service_config(tmp_path / "not-created.sqlite3")
    config["sources"]["greenhouse"][0]["enabled"] = False

    with pytest.raises(ValueError, match="No Greenhouse or Lever accounts"):
        IngestionService(config, http_client=QueueClient([])).run()
    assert not (tmp_path / "not-created.sqlite3").exists()


def test_load_config_validates_enabled_accounts_and_source_types(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(service_config(tmp_path / "db.sqlite3")))
    loaded = load_config(config_path)
    assert loaded["sources"]["greenhouse"][0]["board_token"] == "acme-board"

    invalid = service_config(tmp_path / "db.sqlite3")
    invalid["sources"]["linkedin"] = []
    config_path.write_text(json.dumps(invalid))
    with pytest.raises(ValueError, match="Unsupported source"):
        load_config(config_path)


def test_dashboard_reads_persisted_ingestion_data_without_demo_fallback(tmp_path):
    database = tmp_path / "ingestion.sqlite3"
    service = IngestionService(
        service_config(database),
        http_client=QueueClient([greenhouse_payload()]),
    )
    service.run()

    dashboard_data = load_ingested_dashboard_data(database)

    assert dashboard_data is not None
    assert len(dashboard_data["active_jobs"]) == 1
    assert dashboard_data["event_counts"] == {"NEW": 1}
    assert dashboard_data["recent_events"][0]["job_id"] == "greenhouse:acme-board:123"
    assert load_ingested_dashboard_data(tmp_path / "missing.sqlite3") is None


class FakeKafkaProducer:
    """Stands in for KafkaJobProducer; fails once `fail_after` sends succeed."""

    sent: list = []
    fail_after: int | None = None
    unreachable = False

    def __init__(self, **_kwargs):
        if FakeKafkaProducer.unreachable:
            raise RuntimeError("Unable to bootstrap from ['localhost:9092']")

    def send_event(self, event):
        if FakeKafkaProducer.fail_after is not None and len(FakeKafkaProducer.sent) >= FakeKafkaProducer.fail_after:
            raise RuntimeError("broker unavailable")
        FakeKafkaProducer.sent.append(event)

    def close(self):
        pass


@pytest.fixture
def fake_kafka(monkeypatch):
    import src.streaming.kafka_producer as kafka_module

    FakeKafkaProducer.sent = []
    FakeKafkaProducer.fail_after = None
    FakeKafkaProducer.unreachable = False
    monkeypatch.setattr(kafka_module, "KafkaJobProducer", FakeKafkaProducer)
    return FakeKafkaProducer


def two_job_payload(title="Engineer"):
    return {
        "jobs": [
            {"id": 1, "title": title, "absolute_url": "https://boards.greenhouse.io/acme/jobs/1"},
            {"id": 2, "title": title, "absolute_url": "https://boards.greenhouse.io/acme/jobs/2"},
        ],
        "meta": {"total": 2},
    }


def test_kafka_events_left_by_failed_publish_are_retried_on_next_run(tmp_path, fake_kafka):
    database = tmp_path / "ingestion.sqlite3"
    fake_kafka.fail_after = 1

    first = IngestionService(
        service_config(database),
        with_kafka=True,
        http_client=QueueClient([two_job_payload()]),
    ).run()

    assert first["runs"][0]["new"] == 2
    assert first["kafka"] == {"published": 1, "pending": 1}
    assert first["failures"][0]["stage"] == "kafka_publish"
    assert [e["job_id"] for e in fake_kafka.sent] == ["greenhouse:acme-board:1"]

    # The next scan sees no changes, but the unsent NEW event is still delivered.
    fake_kafka.fail_after = None
    second = IngestionService(
        service_config(database),
        with_kafka=True,
        http_client=QueueClient([two_job_payload()]),
    ).run()

    assert second["runs"][0]["unchanged"] == 2
    assert second["kafka"] == {"published": 1, "pending": 0}
    assert second["failures"] == []
    assert [e["job_id"] for e in fake_kafka.sent] == [
        "greenhouse:acme-board:1",
        "greenhouse:acme-board:2",
    ]
    assert all(e["event"] == "NEW" for e in fake_kafka.sent)


def test_run_without_kafka_flag_reports_no_kafka_stats(tmp_path):
    result = IngestionService(
        service_config(tmp_path / "ingestion.sqlite3"),
        http_client=QueueClient([greenhouse_payload()]),
    ).run()
    assert "kafka" not in result


def test_unreachable_kafka_still_ingests_and_queues_events_for_later(tmp_path, fake_kafka):
    database = tmp_path / "ingestion.sqlite3"
    fake_kafka.unreachable = True

    offline = IngestionService(
        service_config(database),
        with_kafka=True,
        http_client=QueueClient([two_job_payload()]),
    ).run()

    assert offline["runs"][0]["new"] == 2
    assert offline["kafka"] == {"published": 0, "pending": 2}
    assert offline["failures"][0]["stage"] == "kafka_connect"
    assert fake_kafka.sent == []

    fake_kafka.unreachable = False
    online = IngestionService(
        service_config(database),
        with_kafka=True,
        http_client=QueueClient([two_job_payload()]),
    ).run()

    assert online["failures"] == []
    assert online["kafka"] == {"published": 2, "pending": 0}
    assert [e["job_id"] for e in fake_kafka.sent] == [
        "greenhouse:acme-board:1",
        "greenhouse:acme-board:2",
    ]
