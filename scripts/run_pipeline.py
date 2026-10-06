from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.trend_analytics import build_summary
from src.generator.job_generator import generate_batch, write_events
from src.storage.parquet_store import write_parquet_records
from src.streaming.spark_stream_processor import summarize_event_stream


def main() -> None:
    raw_dir = Path("./data/raw")
    processed_dir = Path("./data/processed")
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    events = generate_batch(count=30)
    raw_path = raw_dir / "job_events.jsonl"
    write_events(events, str(raw_path))

    summary = summarize_event_stream(events)
    write_parquet_records(events, str(processed_dir / "job_history.parquet"))

    dashboard_summary = build_summary(events)
    print("Stream summary:", summary)
    print("Dashboard summary:", dashboard_summary)
    print(f"Raw event file: {raw_path}")
    print(f"Processed data file: {processed_dir / 'job_history.parquet'}")


if __name__ == "__main__":
    main()
