from __future__ import annotations

import sqlite3

import pytest

from src.ingestion.base_source import SourceScan
from src.ingestion.sqlite_store import IncompleteScanError, SQLiteSnapshotStore


SOURCE = "lever"
ACCOUNT = "global:acme"


def make_scan(
    provider_ids: list[str],
    complete: bool = True,
    raw_pages: list[object] | None = None,
) -> SourceScan:
    records = [{"id": provider_id} for provider_id in provider_ids]
    return SourceScan(
        source=SOURCE,
        account_id=ACCOUNT,
        company="Acme",
        records=records,
        raw_pages=raw_pages if raw_pages is not None else [records],
        complete=complete,
        fetched_count=len(records),
    )


def make_listing(
    provider_id: str,
    *,
    title: str = "Platform Engineer",
    timestamp: str = "2026-01-01T00:00:00+00:00",
) -> dict[str, object]:
    return {
        "job_id": f"lever:{ACCOUNT}:{provider_id}",
        "source": SOURCE,
        "source_account_id": ACCOUNT,
        "source_job_id": provider_id,
        "source_url": f"https://jobs.lever.co/acme/{provider_id}",
        "source_updated_at": None,
        "company": "Acme",
        "title": title,
        "location": "Remote",
        "experience": None,
        "salary": None,
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_interval": None,
        "timestamp": timestamp,
        "event": "NEW",
    }


def apply(
    store: SQLiteSnapshotStore,
    listings: list[dict[str, object]],
    observed_at: str,
    *,
    complete: bool = True,
    raw_pages: list[object] | None = None,
) -> dict[str, object]:
    ids = [str(listing["source_job_id"]) for listing in listings]
    return store.apply_scan(
        make_scan(ids, complete=complete, raw_pages=raw_pages),
        listings,
        observed_at=observed_at,
    )


def test_sqlite_store_persists_new_modified_and_unchanged_snapshots(tmp_path):
    database = tmp_path / "ingestion.sqlite3"
    store = SQLiteSnapshotStore(database)
    original = make_listing("posting-1")

    first = apply(store, [original], "2026-01-01T00:00:00Z")
    assert (first["new"], first["modified"], first["unchanged"]) == (1, 0, 0)

    unchanged = apply(
        store,
        [make_listing("posting-1", timestamp="2026-01-02T00:00:00+00:00")],
        "2026-01-02T00:00:00Z",
    )
    assert (unchanged["new"], unchanged["modified"], unchanged["unchanged"]) == (0, 0, 1)
    assert store.get_snapshot(original["job_id"])["last_seen_at"] == "2026-01-02T00:00:00+00:00"

    modified = apply(
        store,
        [make_listing("posting-1", title="Senior Platform Engineer")],
        "2026-01-03T00:00:00Z",
    )
    assert modified["modified"] == 1
    assert [event["event"] for event in store.list_events(original["job_id"])] == [
        "NEW",
        "MODIFIED",
    ]
    store.close()

    reopened = SQLiteSnapshotStore(database)
    assert reopened.get_snapshot(original["job_id"])["title"] == "Senior Platform Engineer"
    assert len(reopened.list_runs()) == 3
    reopened.close()


def test_absence_requires_two_complete_successful_scans_and_reappearance_resets_it(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3")
    listing = make_listing("posting-1")
    apply(store, [listing], "2026-01-01T00:00:00Z")

    first_absence = apply(store, [], "2026-01-02T00:00:00Z")
    snapshot = store.get_snapshot(listing["job_id"])
    assert first_absence["closed"] == 0
    assert snapshot["is_active"] is True
    assert snapshot["absence_count"] == 1

    reappeared = apply(
        store,
        [make_listing("posting-1", timestamp="2026-01-03T00:00:00+00:00")],
        "2026-01-03T00:00:00Z",
    )
    assert reappeared["unchanged"] == 1
    assert store.get_snapshot(listing["job_id"])["absence_count"] == 0

    apply(store, [], "2026-01-04T00:00:00Z")
    second_absence = apply(store, [], "2026-01-05T00:00:00Z")
    assert second_absence["closed"] == 1
    assert store.get_snapshot(listing["job_id"])["is_active"] is False
    assert [event["event"] for event in store.list_events(listing["job_id"])] == [
        "NEW",
        "CLOSED",
    ]
    store.close()


def test_failed_or_incomplete_scan_does_not_change_state_or_absence_evidence(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3")
    listing = make_listing("posting-1")
    apply(store, [listing], "2026-01-01T00:00:00Z")
    apply(store, [], "2026-01-02T00:00:00Z")
    before_runs = store.list_runs()
    before_snapshot = store.get_snapshot(listing["job_id"])

    with pytest.raises(IncompleteScanError):
        apply(store, [], "2026-01-03T00:00:00Z", complete=False)

    assert store.list_runs() == before_runs
    assert store.get_snapshot(listing["job_id"]) == before_snapshot
    assert [event["event"] for event in store.list_events(listing["job_id"])] == ["NEW"]
    store.close()


def test_duplicate_job_ids_make_scan_incomplete_without_committing(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3")
    first = make_listing("posting-1")
    duplicate = make_listing("posting-1", title="Different duplicate")
    scan = make_scan(["posting-1", "posting-1"])

    with pytest.raises(IncompleteScanError, match="Duplicate normalized job ID"):
        store.apply_scan(scan, [first, duplicate], observed_at="2026-01-01T00:00:00Z")

    assert store.list_runs() == []
    assert store.list_events() == []
    assert store.get_snapshot(first["job_id"]) is None
    store.close()


def test_database_transaction_rolls_back_absence_counter_event_and_run_on_failure(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3")
    listing = make_listing("posting-1")
    apply(store, [listing], "2026-01-01T00:00:00Z")
    apply(store, [], "2026-01-02T00:00:00Z")
    before_runs = store.list_runs()

    store._connection.execute(
        """
        CREATE TRIGGER reject_close_event
        BEFORE INSERT ON lifecycle_events
        WHEN NEW.event_type = 'CLOSED'
        BEGIN
            SELECT RAISE(ABORT, 'injected close-event failure');
        END;
        """
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected close-event failure"):
        apply(store, [], "2026-01-03T00:00:00Z")

    snapshot = store.get_snapshot(listing["job_id"])
    assert snapshot["absence_count"] == 1
    assert snapshot["is_active"] is True
    assert store.list_runs() == before_runs
    assert [event["event"] for event in store.list_events(listing["job_id"])] == ["NEW"]
    store.close()


def test_reopened_closed_listing_is_a_new_lifecycle(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3")
    listing = make_listing("posting-1")
    apply(store, [listing], "2026-01-01T00:00:00Z")
    apply(store, [], "2026-01-02T00:00:00Z")
    apply(store, [], "2026-01-03T00:00:00Z")

    result = apply(
        store,
        [make_listing("posting-1", timestamp="2026-01-04T00:00:00+00:00")],
        "2026-01-04T00:00:00Z",
    )

    assert result["new"] == 1
    assert [event["event"] for event in store.list_events(listing["job_id"])] == [
        "NEW",
        "CLOSED",
        "NEW",
    ]
    store.close()


def test_raw_response_retention_is_configurable_and_prunes_old_payloads(tmp_path):
    store = SQLiteSnapshotStore(tmp_path / "ingestion.sqlite3", raw_retention_days=2)
    listing = make_listing("posting-1")
    apply(
        store,
        [listing],
        "2026-01-01T00:00:00Z",
        raw_pages=[{"description": "raw posting content"}],
    )
    assert store.raw_snapshot_count() == 1

    apply(store, [], "2026-01-04T00:00:00Z", raw_pages=[{"page": 2}])
    assert store.raw_snapshot_count() == 1
    store.close()

    disabled_store = SQLiteSnapshotStore(
        tmp_path / "no-raw.sqlite3",
        raw_retention_days=0,
    )
    apply(disabled_store, [], "2026-01-01T00:00:00Z")
    assert disabled_store.raw_snapshot_count() == 0
    disabled_store.close()


def test_raw_retention_rejects_invalid_values(tmp_path):
    with pytest.raises(ValueError):
        SQLiteSnapshotStore(tmp_path / "invalid.sqlite3", raw_retention_days=-1)
