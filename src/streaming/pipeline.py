"""Micro-batch processing shared by Spark Structured Streaming and the local runner.

One micro-batch partition: validated records -> change detection against
HBase -> HBase state/history writes -> Parquet event lake; rejected records go
to the Parquet quarantine dataset. Spark runs ``process_partition`` on its
executors in parallel (one writer per job_id); the driver then aggregates the
partition summaries with ``record_batch`` into a status file for the dashboard.

Every output is replay-safe: event IDs are deterministic, HBase recognises
already-applied events, Parquet file names are derived from
(source, batch, partition) so a re-run overwrites rather than appends, and
batch totals are only counted the first time a batch ID is recorded.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.storage import parquet_store
from src.streaming.change_detection import ChangeDetector
from src.streaming.etl import clean_and_validate

STATUS_FILE = "_status/pipeline_status.json"


def _safe_label(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value) or "stream"


class StreamPipeline:
    def __init__(self, store: Any, lake_root: str | Path, *, source: str = "local") -> None:
        self.store = store
        self.lake_root = Path(lake_root)
        self.source = _safe_label(source)
        self.detector = ChangeDetector(store) if store is not None else None

    def load_status(self) -> Dict[str, Any]:
        path = self.lake_root / STATUS_FILE
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def process_partition(
        self,
        records: Iterable[Dict[str, Any]],
        batch_id: int,
        *,
        rejected: Optional[List[Dict[str, Any]]] = None,
        partition: int = 0,
    ) -> Dict[str, Any]:
        """Process one partition of a micro-batch; safe to run concurrently per job_id partition."""
        ordered = sorted(records, key=lambda record: str(record.get("event_time") or ""))
        processed = self.detector.process(ordered, batch_id=batch_id) if ordered else []
        label = f"{self.source}-b{batch_id:07d}-p{partition:04d}"
        parquet_store.append_events(processed, self.lake_root, label=label)

        rejected_rows = [{**row, "batch_id": batch_id} for row in rejected or []]
        if rejected_rows:
            parquet_store.append_quarantine(rejected_rows, self.lake_root, label=label)
        return {
            "partition": partition,
            "processed": len(processed),
            "rejected": len(rejected_rows),
            "detected": dict(Counter(event["event"] for event in processed)),
        }

    def record_batch(self, summaries: Iterable[Dict[str, Any]], batch_id: int, **extra: Any) -> Dict[str, Any]:
        """Aggregate partition summaries and update the status file (driver side)."""
        summaries = list(summaries)
        detected: Counter = Counter()
        for item in summaries:
            detected.update(item.get("detected", {}))
        processed = sum(item.get("processed", 0) for item in summaries)
        rejected = sum(item.get("rejected", 0) for item in summaries)

        status = self.load_status()
        totals: Counter = Counter(status.get("totals", {}))
        seen = status.get("last_batch_by_source", {})
        # A replayed batch (same source, batch ID not newer) must not be counted twice.
        replay = batch_id <= int(seen.get(self.source, -1))
        if not replay:
            totals.update(detected)
            totals["INVALID"] += rejected
            totals["RECEIVED"] += processed + rejected
            seen[self.source] = batch_id

        summary = {
            "source": self.source,
            "batch_id": batch_id,
            "partitions": len(summaries),
            "processed": processed,
            "rejected": rejected,
            "detected": dict(detected),
            "replay": replay,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "totals": dict(totals),
            "last_batch_by_source": seen,
            **extra,
        }
        parquet_store.write_json(summary, self.lake_root / STATUS_FILE)
        return summary

    @property
    def totals(self) -> Counter:
        return Counter(self.load_status().get("totals", {}))

    def process_valid_records(
        self,
        records: Iterable[Dict[str, Any]],
        batch_id: int,
        *,
        rejected: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return self.record_batch([self.process_partition(records, batch_id, rejected=rejected)], batch_id)

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
