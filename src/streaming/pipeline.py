"""Micro-batch processing shared by Spark Structured Streaming and the local runner.

One micro-batch: validated records -> change detection against HBase ->
HBase state/history writes -> Parquet event lake; rejected records go to the
Parquet quarantine dataset. A small status file records pipeline progress for
the dashboard.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.storage import parquet_store
from src.streaming.change_detection import ChangeDetector
from src.streaming.etl import clean_and_validate

STATUS_FILE = "_status/pipeline_status.json"


class StreamPipeline:
    def __init__(self, store: Any, lake_root: str | Path, *, source: str = "local") -> None:
        self.store = store
        self.lake_root = Path(lake_root)
        self.source = source
        self.detector = ChangeDetector(store)
        self.totals: Counter[str] = Counter(self.load_status().get("totals", {}))

    def load_status(self) -> Dict[str, Any]:
        path = self.lake_root / STATUS_FILE
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def process_valid_records(
        self,
        records: Iterable[Dict[str, Any]],
        batch_id: int,
        *,
        rejected: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        ordered = sorted(records, key=lambda record: str(record.get("event_time") or ""))
        processed = self.detector.process(ordered, batch_id=batch_id)
        label = f"b{batch_id:07d}"
        parquet_store.append_events(processed, self.lake_root, label=label)

        rejected = [{**row, "batch_id": batch_id} for row in rejected or []]
        if rejected:
            parquet_store.append_quarantine(rejected, self.lake_root, label=label)

        counts = Counter(event["event"] for event in processed)
        self.totals.update(counts)
        self.totals["INVALID"] += len(rejected)
        self.totals["RECEIVED"] += len(processed) + len(rejected)
        summary = {
            "source": self.source,
            "batch_id": batch_id,
            "processed": len(processed),
            "rejected": len(rejected),
            "detected": dict(counts),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "totals": dict(self.totals),
        }
        parquet_store.write_json(summary, self.lake_root / STATUS_FILE)
        return summary

    def process_raw_records(self, raw_records: Iterable[Any], batch_id: int) -> Dict[str, Any]:
        valid, rejected = split_valid(raw_records)
        return self.process_valid_records(valid, batch_id, rejected=rejected)


def split_valid(raw_records: Iterable[Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    received_at = datetime.now(timezone.utc).isoformat()
    valid: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for raw in raw_records:
        record, error = clean_and_validate(raw)
        if record is not None:
            valid.append(record)
            continue
        raw_value = raw if isinstance(raw, str) else json.dumps(raw, default=str)
        rejected.append({"raw_value": raw_value, "validation_error": error, "received_at": received_at})
    return valid, rejected
