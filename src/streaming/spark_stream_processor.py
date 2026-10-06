from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover
    SparkSession = None
    F = None

from src.common.models import validate_record


class SparkJobStreamProcessor:
    def __init__(self, app_name: str = "CareerPulse", master: str = "local[*]") -> None:
        self.app_name = app_name
        self.master = master
        self.spark = None

        if SparkSession is not None:
            self.spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()

    def process_batch(self, records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        valid_records = []
        for record in records:
            ok, error = validate_record(record)
            if not ok:
                continue
            valid_records.append(record)

        summary = Counter(record["event"].upper() for record in valid_records)
        return dict(summary)

    def close(self) -> None:
        if self.spark is not None:
            self.spark.stop()


def detect_change(previous_state: Optional[Dict[str, Any]], current_record: Dict[str, Any]) -> str:
    if previous_state is None:
        return "NEW"
    if previous_state.get("company") != current_record.get("company"):
        return "MODIFIED"
    if current_record.get("event", "").upper() == "CLOSED":
        return "CLOSED"
    if previous_state.get("title") != current_record.get("title"):
        return "MODIFIED"
    return "DUPLICATE"


def summarize_event_stream(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    processor = SparkJobStreamProcessor()
    try:
        return processor.process_batch(records)
    finally:
        processor.close()


if __name__ == "__main__":
    sample_records = [
        {"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "2-4 years", "salary": 1500000, "timestamp": "2026-10-06T12:00:00Z", "event": "NEW"},
        {"job_id": "J2", "company": "Amazon", "title": "Data Engineer", "location": "Hyderabad", "experience": "2-4 years", "salary": 1800000, "timestamp": "2026-10-06T12:05:00Z", "event": "NEW"},
    ]
    print(summarize_event_stream(sample_records))
