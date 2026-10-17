from __future__ import annotations

from src.dashboard.app import format_salary, load_ingested_dashboard_data
from src.ingestion.base_source import SourceScan
from src.ingestion.sqlite_store import SQLiteSnapshotStore


def test_dashboard_reads_active_jobs_events_and_runs_read_only(tmp_path):
    database = tmp_path / "ingestion.sqlite3"
    store = SQLiteSnapshotStore(database)
    listing = {
        "job_id": "greenhouse:board:role-1",
        "source": "greenhouse",
        "source_account_id": "board",
        "source_job_id": "role-1",
        "source_url": "https://example.com/jobs/role-1",
        "source_updated_at": None,
        "company": "Example Co",
        "title": "Software Engineer",
        "location": "Remote",
        "experience": None,
        "salary": None,
        "salary_min": 90000,
        "salary_max": 120000,
        "salary_currency": "USD",
        "salary_interval": "year",
        "salary_ranges": [{"salary_min": 90000, "salary_max": 120000, "salary_currency": "USD", "salary_interval": "year"}],
        "timestamp": "2026-10-12T10:00:00+00:00",
        "event": "NEW",
    }
    scan = SourceScan(
        source="greenhouse",
        account_id="board",
        company="Example Co",
        records=[{"id": "role-1"}],
        raw_pages=[],
        complete=True,
        fetched_count=1,
    )
    store.apply_scan(scan, [listing], observed_at="2026-10-12T10:00:00+00:00")
    store.close()

    data = load_ingested_dashboard_data(database)
    assert data is not None
    assert len(data["active_jobs"]) == 1
    assert data["active_jobs"][0]["title"] == "Software Engineer"
    assert data["event_counts"] == {"NEW": 1}
    assert data["runs"][0]["fetched_count"] == 1
    assert data["latest_by_account"][0]["account_id"] == "board"


def test_salary_format_preserves_unknown_and_ranges():
    assert format_salary({"salary": None}) == "Not disclosed"
    assert format_salary({
        "salary_min": 90000,
        "salary_max": 120000,
        "salary_currency": "USD",
        "salary_interval": "year",
    }) == "USD 90,000–120,000 / year"


def test_role_resolution_is_case_insensitive_and_least_privilege():
    from src.dashboard.app import resolve_role

    config = {
        "admin_emails": ["Admin@Example.com"],
        "analyst_emails": ["analyst@example.com"],
        "viewer_emails": ["viewer@example.com"],
    }
    assert resolve_role("admin@example.com", config) == "admin"
    assert resolve_role("ANALYST@example.com", config) == "analyst"
    assert resolve_role("viewer@example.com", config) == "viewer"
    assert resolve_role("unknown@example.com", config) == "viewer"
    assert resolve_role("", config) == "viewer"


def test_admin_role_has_precedence_if_email_is_misconfigured_in_multiple_lists():
    from src.dashboard.app import resolve_role

    assert resolve_role("same@example.com", {
        "admin_emails": ["same@example.com"],
        "analyst_emails": ["same@example.com"],
    }) == "admin"
