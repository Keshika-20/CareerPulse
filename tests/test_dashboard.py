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


def test_role_resolution_is_case_insensitive_and_denies_unlisted_emails():
    from src.dashboard.app import resolve_role

    config = {
        "admin_emails": ["Admin@Example.com"],
        "analyst_emails": ["analyst@example.com"],
        "viewer_emails": ["viewer@example.com"],
    }
    assert resolve_role("admin@example.com", config) == "admin"
    assert resolve_role("ANALYST@example.com", config) == "analyst"
    assert resolve_role("viewer@example.com", config) == "viewer"
    assert resolve_role("unknown@example.com", config) is None
    assert resolve_role("", config) is None
    assert resolve_role("anyone@example.com", {}) is None


def test_admin_role_has_precedence_if_email_is_misconfigured_in_multiple_lists():
    from src.dashboard.app import resolve_role

    assert resolve_role("same@example.com", {
        "admin_emails": ["same@example.com"],
        "analyst_emails": ["same@example.com"],
    }) == "admin"


def test_run_ingestion_builds_command_with_optional_spark(monkeypatch, tmp_path):
    import subprocess

    from src.dashboard import app

    config = tmp_path / "ingestion.json"
    config.write_text("{}")
    commands = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout='{"runs": []}', stderr="")

    monkeypatch.setattr(app.subprocess, "run", fake_run)

    assert app._run_ingestion(str(config)) == (0, '{"runs": []}')
    assert app._run_ingestion(str(config), spark_batch=True)[0] == 0
    assert "--spark-batch" not in commands[0]
    assert commands[1][-1] == "--spark-batch"
    assert commands[0][1:4] == ["-m", "src.ingestion.service", "--config"]


def test_run_ingestion_reports_missing_config(tmp_path):
    from src.dashboard.app import _run_ingestion

    code, message = _run_ingestion(str(tmp_path / "missing.json"))
    assert code == 2
    assert "Config file not found" in message


def test_missing_auth_settings_includes_cookie_secret():
    from src.dashboard.app import REQUIRED_AUTH_SETTINGS, missing_auth_settings

    complete = {name: "value" for name in REQUIRED_AUTH_SETTINGS}
    assert missing_auth_settings(complete) == []
    assert missing_auth_settings({**complete, "cookie_secret": ""}) == ["cookie_secret"]
    assert missing_auth_settings({}) == list(REQUIRED_AUTH_SETTINGS)


def test_streaming_view_renders_lake_and_hbase(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from streamlit.testing.v1 import AppTest

    from src.generator.job_generator import JobEventSimulator
    from src.storage.hbase_store import HBaseJobStore
    from src.streaming.pipeline import StreamPipeline

    hbase_path = tmp_path / "hbase.sqlite3"
    store = HBaseJobStore.connect("local", path=hbase_path)
    events = JobEventSimulator(seed=3, invalid_rate=0.05).generate(
        200, start=datetime(2026, 10, 6, tzinfo=timezone.utc), span=timedelta(hours=3)
    )
    StreamPipeline(store, tmp_path / "lake").process_raw_records(events, batch_id=0)
    job_id = next(iter(store.list_jobs()))
    store.close()
    monkeypatch.setenv("CAREERPULSE_HBASE_PATH", str(hbase_path))
    monkeypatch.setenv("CP_TEST_LAKE", str(tmp_path / "lake"))

    def page():
        import os
        from pathlib import Path

        import streamlit as st

        from src.dashboard.streaming_view import render_streaming_analytics

        render_streaming_analytics(st, Path.cwd(), os.environ["CP_TEST_LAKE"])

    app = AppTest.from_function(page, default_timeout=60).run()
    assert not app.exception
    labels = [metric.label for metric in app.metric]
    assert "New jobs · last 5 min" in labels and "Rejected by validation" in labels
    assert any("Run `python -m src.analytics.run_analytics`" in info.value for info in app.info)

    app.text_input(key="cp_hbase_job").input(job_id).run()
    assert not app.exception
    assert app.json  # current HBase state is rendered
