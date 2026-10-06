from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

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


def render_dashboard():
    if st is None:
        print("Streamlit is not installed. Install requirements.txt to run the dashboard.")
        return

    st.title("CareerPulse Dashboard")
    records = load_demo_records()
    summary = build_summary(records)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Jobs", summary["total_jobs"])
    col2.metric("New Jobs", summary["new_jobs"])
    col3.metric("Modified Jobs", summary["modified_jobs"])
    col4.metric("Closed Jobs", summary["closed_jobs"])

    st.subheader("Top companies")
    st.bar_chart({item["company"]: item["count"] for item in summary["top_companies"]})

    st.subheader("Average salary by company")
    salary_data = {item["company"]: item["average_salary"] for item in summary["average_salary_by_company"]}
    st.bar_chart(salary_data)

    st.subheader("Recent job events")
    st.dataframe(records[:10])


def main() -> None:
    if st is None:
        print("Optional dependency missing: streamlit")
        return
    render_dashboard()


if __name__ == "__main__":
    main()
