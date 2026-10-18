from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover
    SparkSession = None

from src.common.models import validate_record
from src.common.spark_session import build_spark_session
from src.streaming.change_detection import detect_change  # noqa: F401  (re-exported)


def aggregate_valid_event_counts(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for record in records:
        ok, _ = validate_record(record)
        if not ok:
            continue
        event_name = str(record.get("event", "")).upper()
        if event_name:
            counts[event_name] += 1
    return dict(counts)


class SparkJobStreamProcessor:
    def __init__(self, app_name: str = "CareerPulse", master: str = "local[*]") -> None:
        self.app_name = app_name
        self.master = master
        self.spark = None

        if SparkSession is None:
            raise RuntimeError("Spark integration requires a compatible PySpark + Java environment. "
                               "Ordinary Python validation and aggregation do not require a JVM.")

        self.spark = build_spark_session(app_name, master)

    def process_batch(self, records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        valid_events = []
        for record in records:
            ok, _ = validate_record(record)
            if not ok:
                continue
            event_name = str(record.get("event", "")).upper()
            if event_name:
                valid_events.append(event_name)

        if not valid_events:
            return {}

        event_counts = (
            self.spark.createDataFrame([(event,) for event in valid_events], ["event"])
            .groupBy("event")
            .count()
            .collect()
        )
        return {row["event"]: int(row["count"]) for row in event_counts}

    def close(self) -> None:
        if self.spark is not None:
            self.spark.stop()


def summarize_event_stream(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    return aggregate_valid_event_counts(records)


if __name__ == "__main__":
    sample_records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "2-4 years", "salary": 1500000, "timestamp": "2026-10-06T12:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Data Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1800000, "timestamp": "2026-10-06T12:05:00Z", "event": "NEW"},
    ]
    print(summarize_event_stream(sample_records))
