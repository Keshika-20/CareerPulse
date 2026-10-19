"""Parquet storage: a small date-partitioned event lake plus a one-file helper.

Layout under the lake root::

    raw/event_date=YYYY-MM-DD/part-*.parquet          cold storage: every incoming message, as received
    events/event_date=YYYY-MM-DD/part-*.parquet       processed, change-detected events
    quarantine/event_date=YYYY-MM-DD/part-*.parquet   records rejected by validation
    window_counts/window_counts.parquet               streaming window aggregates per event type
    window_category/window_category.parquet           streaming window metrics per job category
    alerts/event_date=YYYY-MM-DD/part-*.parquet       monitoring alerts (surges, data quality)
    analytics/*.parquet                               Spark SQL / RDD / ML outputs

Files are written to a hidden temporary name and atomically renamed, so readers
such as the DuckDB dashboard never see a half-written file.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None

RAW_DATASET = "raw"
EVENTS_DATASET = "events"
QUARANTINE_DATASET = "quarantine"
WINDOW_DATASET = "window_counts"
CATEGORY_WINDOW_DATASET = "window_category"
ALERTS_DATASET = "alerts"
ANALYTICS_DATASET = "analytics"


def _require_pyarrow() -> None:
    if pa is None:
        raise RuntimeError("Parquet lake storage requires pyarrow: pip install pyarrow")


def _ts_type():
    return pa.timestamp("us", tz="UTC")


def events_schema():
    _require_pyarrow()
    return pa.schema([
        ("event_id", pa.string()),
        ("batch_id", pa.int64()),
        ("job_id", pa.string()),
        ("company", pa.string()),
        ("title", pa.string()),
        ("location", pa.string()),
        ("experience", pa.string()),
        ("salary", pa.float64()),
        ("category", pa.string()),
        ("source", pa.string()),
        ("claimed_event", pa.string()),
        ("event", pa.string()),
        ("changed_fields", pa.string()),
        ("fingerprint", pa.string()),
        ("event_time", _ts_type()),
        ("processed_at", _ts_type()),
        # Optional, higher-dimensional attributes (null in older files).
        ("employment_type", pa.string()),
        ("work_mode", pa.string()),
        ("education", pa.string()),
        ("skills", pa.list_(pa.string())),
        ("skill_count", pa.int64()),
    ])


def raw_schema():
    _require_pyarrow()
    return pa.schema([
        ("event_id", pa.string()),
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int64()),
        ("kafka_offset", pa.int64()),
        ("kafka_timestamp", _ts_type()),
        ("raw_value", pa.string()),
        ("received_at", _ts_type()),
    ])


def category_window_schema():
    _require_pyarrow()
    return pa.schema([
        ("window_start", _ts_type()),
        ("window_end", _ts_type()),
        ("category", pa.string()),
        ("new_postings", pa.int64()),
        ("closed_postings", pa.int64()),
        ("events", pa.int64()),
        ("avg_salary", pa.float64()),
        ("active_companies", pa.int64()),
    ])


def alerts_schema():
    _require_pyarrow()
    return pa.schema([
        ("alert_id", pa.string()),
        ("alert_type", pa.string()),
        ("severity", pa.string()),
        ("subject", pa.string()),
        ("message", pa.string()),
        ("observed", pa.float64()),
        ("threshold", pa.float64()),
        ("baseline", pa.float64()),
        ("window_start", _ts_type()),
        ("window_end", _ts_type()),
        ("created_at", _ts_type()),
    ])


def quarantine_schema():
    _require_pyarrow()
    return pa.schema([
        ("batch_id", pa.int64()),
        ("event_id", pa.string()),
        ("raw_value", pa.string()),
        ("validation_error", pa.string()),
        ("received_at", _ts_type()),
    ])


def window_schema():
    _require_pyarrow()
    return pa.schema([
        ("window_start", _ts_type()),
        ("window_end", _ts_type()),
        ("event", pa.string()),
        ("count", pa.int64()),
        # HyperLogLog++ estimates; null in files written before they existed.
        ("unique_jobs", pa.int64()),
        ("unique_companies", pa.int64()),
    ])


def to_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _atomic_write_table(table, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    pq.write_table(table, temp)
    _replace_with_retry(temp, target)
    return target


def _replace_with_retry(source: Path, target: Path, attempts: int = 20) -> None:
    # On Windows, replacing a file that a reader (e.g. the dashboard) has open
    # raises PermissionError until the reader closes it; POSIX never blocks.
    for attempt in range(attempts):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt + 1 == attempts:
                source.unlink(missing_ok=True)
                raise
            time.sleep(0.05 * (attempt + 1))


def _rows_for_schema(records: Iterable[Dict[str, Any]], schema) -> List[Dict[str, Any]]:
    timestamp_fields = {field.name for field in schema if pa.types.is_timestamp(field.type)}
    rows = []
    for record in records:
        row = {}
        for field in schema:
            value = record.get(field.name)
            if field.name in timestamp_fields:
                value = to_utc_datetime(value)
            elif value is not None and pa.types.is_floating(field.type):
                value = float(value)
            elif value is not None and pa.types.is_integer(field.type):
                value = int(value)
            elif value is not None and pa.types.is_string(field.type):
                value = str(value)
            row[field.name] = value
        rows.append(row)
    return rows


def append_partitioned(
    records: Iterable[Dict[str, Any]],
    lake_root: str | Path,
    dataset: str,
    schema,
    *,
    time_field: str,
    label: str = "",
) -> int:
    """Write records as one Parquet file per ``event_date`` partition.

    With a ``label`` the file name is deterministic (``part-<label>.parquet``),
    so re-running the same micro-batch partition replaces its earlier output
    instead of duplicating it. Without one, a unique name is generated.
    """
    _require_pyarrow()
    rows = _rows_for_schema(records, schema)
    if not rows:
        return 0
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        moment = row.get(time_field) or datetime.now(timezone.utc)
        by_date.setdefault(moment.strftime("%Y-%m-%d"), []).append(row)

    name = f"part-{label}.parquet" if label else f"part-{uuid.uuid4().hex[:12]}.parquet"
    for event_date, items in by_date.items():
        table = pa.Table.from_pylist(items, schema=schema)
        _atomic_write_table(table, Path(lake_root) / dataset / f"event_date={event_date}" / name)
    return len(rows)


def append_events(records: Iterable[Dict[str, Any]], lake_root: str | Path, *, label: str = "") -> int:
    return append_partitioned(
        records, lake_root, EVENTS_DATASET, events_schema(), time_field="event_time", label=label
    )


def append_quarantine(records: Iterable[Dict[str, Any]], lake_root: str | Path, *, label: str = "") -> int:
    return append_partitioned(
        records, lake_root, QUARANTINE_DATASET, quarantine_schema(), time_field="received_at", label=label
    )


def append_raw(records: Iterable[Dict[str, Any]], lake_root: str | Path, *, label: str = "") -> int:
    """Archive incoming messages unchanged (cold storage), partitioned by Kafka time.

    Partitioning by the broker timestamp (not the processing time) keeps a
    replayed batch's archive file in the same directory, so it is overwritten.
    """
    return append_partitioned(
        records, lake_root, RAW_DATASET, raw_schema(), time_field="kafka_timestamp", label=label
    )


def append_alerts(records: Iterable[Dict[str, Any]], lake_root: str | Path, *, label: str = "") -> int:
    return append_partitioned(
        records, lake_root, ALERTS_DATASET, alerts_schema(), time_field="created_at", label=label
    )


def _merge_keyed(records: Iterable[Dict[str, Any]], target: Path, schema, keys: tuple) -> int:
    """Upsert rows into a single small Parquet file (single writer per file)."""
    merged: Dict[tuple, Dict[str, Any]] = {}
    if target.exists():
        for row in pq.read_table(target).to_pylist():
            merged[tuple(row.get(key) for key in keys)] = row
    for row in _rows_for_schema(records, schema):
        merged[tuple(row[key] for key in keys)] = row
    if not merged:
        return 0
    ordered = sorted(merged.values(), key=lambda row: tuple(str(row.get(key)) for key in keys))
    _atomic_write_table(pa.Table.from_pylist(ordered, schema=schema), target)
    return len(ordered)


def merge_window_counts(records: Iterable[Dict[str, Any]], lake_root: str | Path) -> int:
    """Upsert streaming window aggregates keyed by (window_start, window_end, event)."""
    _require_pyarrow()
    return _merge_keyed(
        records, Path(lake_root) / WINDOW_DATASET / "window_counts.parquet", window_schema(),
        ("window_start", "window_end", "event"),
    )


def merge_category_windows(records: Iterable[Dict[str, Any]], lake_root: str | Path) -> int:
    """Upsert per-category window metrics keyed by (window_start, window_end, category)."""
    _require_pyarrow()
    return _merge_keyed(
        records, Path(lake_root) / CATEGORY_WINDOW_DATASET / "window_category.parquet",
        category_window_schema(), ("window_start", "window_end", "category"),
    )


def write_analytics_table(records: List[Dict[str, Any]], lake_root: str | Path, name: str) -> Path:
    """Replace one analytics output table (``<lake>/analytics/<name>.parquet``)."""
    _require_pyarrow()
    return _atomic_write_table(
        pa.Table.from_pylist(records), Path(lake_root) / ANALYTICS_DATASET / f"{name}.parquet"
    )


def dataset_files(lake_root: str | Path, dataset: str) -> List[Path]:
    root = Path(lake_root) / dataset
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.parquet") if not path.name.startswith("."))


def read_arrow(lake_root: str | Path, dataset: str = EVENTS_DATASET):
    """Read a lake dataset as one pyarrow Table (schemas promoted across files)."""
    _require_pyarrow()
    files = dataset_files(lake_root, dataset)
    if not files:
        return None
    return pa.concat_tables([pq.read_table(path) for path in files], promote_options="default")


def read_dataset(lake_root: str | Path, dataset: str = EVENTS_DATASET):
    """Read a lake dataset into pandas (empty frame with the schema if missing)."""
    _require_pyarrow()
    schemas = {
        RAW_DATASET: raw_schema(),
        EVENTS_DATASET: events_schema(),
        QUARANTINE_DATASET: quarantine_schema(),
        WINDOW_DATASET: window_schema(),
        CATEGORY_WINDOW_DATASET: category_window_schema(),
        ALERTS_DATASET: alerts_schema(),
    }
    files = dataset_files(lake_root, dataset)
    if not files:
        schema = schemas.get(dataset)
        return schema.empty_table().to_pandas() if schema is not None else pd.DataFrame()
    # Files are read one by one (not as a hive dataset) because every file
    # already carries event_time; the event_date directory is for pruning only.
    tables = [pq.read_table(path) for path in files]
    return pa.concat_tables(tables, promote_options="default").to_pandas()


def write_json(payload: Dict[str, Any], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    temp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    _replace_with_retry(temp, target)
    return target


def write_parquet_records(records: Iterable[Dict[str, Any]], output_path: str) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = list(records)
    if pd is not None:
        frame = pd.DataFrame(rows)
        frame.to_parquet(path, index=False)
        return path

    json_path = path.with_suffix(".jsonl")
    with json_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    return json_path


if __name__ == "__main__":
    sample = [{"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T09:00:00Z", "event": "NEW"}]
    result = write_parquet_records(sample, "./data/processed/sample_jobs.parquet")
    print(result)
