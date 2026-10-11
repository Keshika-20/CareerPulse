from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.trend_analytics import build_summary
from src.generator.job_generator import generate_batch

try:
    import streamlit as st
except ImportError:  # pragma: no cover
    st = None


def load_demo_records() -> List[Dict[str, Any]]:
    data_path = Path("./data/processed/demo_jobs.jsonl")
    if data_path.exists():
        records = []
        with data_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))
        if records:
            return records

    generated = generate_batch(count=25)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    with data_path.open("w", encoding="utf-8") as handle:
        for event in generated:
            handle.write(json.dumps(event) + "\n")
    return generated


def load_ingested_dashboard_data(database_path: str | Path) -> Dict[str, Any] | None:
    path = Path(database_path)
    if not path.exists():
        return None

    database_uri = f"{path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(database_uri, uri=True) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        required_tables = {"job_snapshots", "lifecycle_events"}
        if not required_tables.issubset(tables):
            raise RuntimeError(
                f"{path} exists but is not a CareerPulse ingestion database."
            )
        active_jobs = [
            json.loads(row[0])
            for row in connection.execute(
                """
                SELECT payload_json FROM job_snapshots
                WHERE is_active = 1 ORDER BY last_seen_at DESC
                """
            )
        ]
        recent_events = [
            json.loads(row[0])
            for row in connection.execute(
                """
                SELECT payload_json FROM lifecycle_events
                ORDER BY event_id DESC LIMIT 20
                """
            )
        ]
        event_counts = {
            row[0]: int(row[1])
            for row in connection.execute(
                """
                SELECT event_type, COUNT(*) FROM lifecycle_events
                GROUP BY event_type
                """
            )
        }
    return {
        "active_jobs": active_jobs,
        "recent_events": recent_events,
        "event_counts": event_counts,
    }


def render_dashboard():
    if st is None:
        print("Streamlit is not installed. Install requirements.txt to run the dashboard.")
        return

    st.title("CareerPulse Dashboard")
    database_path = os.environ.get(
        "CAREERPULSE_SQLITE_PATH",
        "./data/processed/ingestion.sqlite3",
    )
    ingestion_data = load_ingested_dashboard_data(database_path)
    if ingestion_data is None:
        st.info("Demo mode: displaying synthetic job events. No ingestion database was found.")
        records = load_demo_records()
        recent_events = records[:10]
        event_counts = {
            "NEW": sum(str(row.get("event", "")).upper() == "NEW" for row in records),
            "MODIFIED": sum(
                str(row.get("event", "")).upper() == "MODIFIED" for row in records
            ),
            "CLOSED": sum(
                str(row.get("event", "")).upper() == "CLOSED" for row in records
            ),
        }
    else:
        st.success(f"Persisted ingestion data: {database_path}")
        records = ingestion_data["active_jobs"]
        recent_events = ingestion_data["recent_events"]
        event_counts = ingestion_data["event_counts"]
    summary = build_summary(records)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Jobs", summary["total_jobs"])
    col2.metric("New Events (all time)", event_counts.get("NEW", 0))
    col3.metric("Modified Events (all time)", event_counts.get("MODIFIED", 0))
    col4.metric("Closed Events (all time)", event_counts.get("CLOSED", 0))

    st.subheader("Top companies")
    st.bar_chart({item["company"]: item["count"] for item in summary["top_companies"]})

    st.subheader("Average salary by company")
    salary_data = {
        item["company"]: item["average_salary"]
        for item in summary["average_salary_by_company"]
        if item["average_salary"] is not None
    }
    if salary_data:
        st.bar_chart(salary_data)
    else:
        st.caption("No scalar salary values are available; salary ranges are not midpoint-imputed.")

    st.subheader("Recent job events")
    st.dataframe(recent_events)


def main() -> None:
    if st is None:
        print("Optional dependency missing: streamlit")
        return
    render_dashboard()


if __name__ == "__main__":
    main()
