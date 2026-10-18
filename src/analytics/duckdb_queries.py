"""DuckDB query layer over the Parquet lake, used by the Streamlit dashboard.

DuckDB queries the Parquet files in place (no JVM, no import step), so the
dashboard can refresh live while the streaming job keeps appending files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None

from src.storage import parquet_store


class LakeQueries:
    def __init__(self, lake_root: str | Path) -> None:
        if duckdb is None:
            raise RuntimeError("The analytics dashboard requires duckdb: pip install duckdb")
        self.lake_root = Path(lake_root)
        self.connection = duckdb.connect(database=":memory:")
        self.connection.execute("SET TimeZone = 'UTC'")

    def _glob(self, dataset: str) -> Optional[str]:
        if not parquet_store.dataset_files(self.lake_root, dataset):
            return None
        return (self.lake_root / dataset / "**" / "*.parquet").as_posix()

    def _rows(self, sql: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        cursor = self.connection.execute(sql, params or [])
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _events(self) -> Optional[str]:
        path = self._glob(parquet_store.EVENTS_DATASET)
        # hive_partitioning exposes event_date for partition pruning.
        return f"read_parquet('{path}', hive_partitioning = true, union_by_name = true)" if path else None

    def has_events(self) -> bool:
        return self._events() is not None

    def kpis(self, recent_minutes: int = 5) -> Dict[str, Any]:
        source = self._events()
        if source is None:
            return {"total_events": 0, "jobs_tracked": 0, "new_recent": 0, "new_recent_by_event_time": 0,
                    "modified": 0, "closed": 0, "duplicates": 0, "latest_event": None, "latest_processed": None}
        row = self._rows(
            f"""
            SELECT COUNT(*) AS total_events,
                   COUNT(DISTINCT job_id) AS jobs_tracked,
                   COUNT(*) FILTER (WHERE event = 'NEW'
                       AND processed_at > now() - to_minutes(CAST(? AS BIGINT))) AS new_recent,
                   COUNT(*) FILTER (WHERE event = 'NEW'
                       AND event_time > (SELECT MAX(event_time) FROM {source}) - to_minutes(CAST(? AS BIGINT))) AS new_recent_by_event_time,
                   COUNT(*) FILTER (WHERE event = 'MODIFIED') AS modified,
                   COUNT(*) FILTER (WHERE event = 'CLOSED') AS closed,
                   COUNT(*) FILTER (WHERE event = 'DUPLICATE') AS duplicates,
                   MAX(event_time) AS latest_event,
                   MAX(processed_at) AS latest_processed
            FROM {source}
            """,
            [recent_minutes, recent_minutes],
        )[0]
        return row

    def event_counts(self) -> List[Dict[str, Any]]:
        source = self._events()
        if source is None:
            return []
        return self._rows(f"SELECT event, COUNT(*) AS events FROM {source} GROUP BY event ORDER BY events DESC")

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        source = self._events()
        if source is None:
            return []
        return self._rows(
            f"""
            SELECT event_time, processed_at, event, claimed_event, job_id, company, title, location,
                   category, experience, salary, changed_fields
            FROM {source} ORDER BY processed_at DESC, event_time DESC LIMIT ?
            """,
            [limit],
        )

    def top_companies(self, limit: int = 10) -> List[Dict[str, Any]]:
        source = self._events()
        if source is None:
            return []
        return self._rows(
            f"""
            SELECT company, COUNT(*) AS new_postings FROM {source}
            WHERE event = 'NEW' GROUP BY company ORDER BY new_postings DESC, company LIMIT ?
            """,
            [limit],
        )

    def trend(self, dimension: str, bucket: str = "hour") -> List[Dict[str, Any]]:
        if dimension not in {"company", "location", "category"}:
            raise ValueError("dimension must be company, location or category")
        if bucket not in {"minute", "hour", "day"}:
            raise ValueError("bucket must be minute, hour or day")
        source = self._events()
        if source is None:
            return []
        return self._rows(
            f"""
            SELECT date_trunc('{bucket}', event_time) AS bucket, {dimension} AS label, COUNT(*) AS new_postings
            FROM {source} WHERE event = 'NEW'
            GROUP BY 1, 2 ORDER BY 1, 2
            """
        )

    def breakdown(self, dimension: str) -> List[Dict[str, Any]]:
        if dimension not in {"company", "location", "category"}:
            raise ValueError("dimension must be company, location or category")
        source = self._events()
        if source is None:
            return []
        return self._rows(
            f"""
            SELECT {dimension} AS label,
                   COUNT(*) FILTER (WHERE event = 'NEW') AS new,
                   COUNT(*) FILTER (WHERE event = 'MODIFIED') AS modified,
                   COUNT(*) FILTER (WHERE event = 'CLOSED') AS closed,
                   COUNT(*) FILTER (WHERE event = 'DUPLICATE') AS duplicate
            FROM {source} GROUP BY 1 ORDER BY new DESC, label
            """
        )

    def avg_salary_by_category(self) -> List[Dict[str, Any]]:
        source = self._events()
        if source is None:
            return []
        return self._rows(
            f"""
            SELECT category, ROUND(AVG(salary)) AS avg_salary, COUNT(salary) AS samples
            FROM {source} WHERE event IN ('NEW', 'MODIFIED') AND salary IS NOT NULL
            GROUP BY category ORDER BY avg_salary DESC
            """
        )

    def window_counts(self, limit: int = 240) -> List[Dict[str, Any]]:
        path = self._glob(parquet_store.WINDOW_DATASET)
        if path is None:
            return []
        # SELECT * so a window file written before the distinct-count columns
        # existed still reads; those rows get None.
        rows = self._rows(
            f"""
            SELECT * FROM (
                SELECT * FROM read_parquet('{path}')
                ORDER BY window_start DESC LIMIT ?
            ) ORDER BY window_start, event
            """,
            [limit],
        )
        for row in rows:
            row.setdefault("unique_jobs", None)
            row.setdefault("unique_companies", None)
        return rows

    def quarantine_summary(self) -> List[Dict[str, Any]]:
        path = self._glob(parquet_store.QUARANTINE_DATASET)
        if path is None:
            return []
        return self._rows(
            f"""
            SELECT split_part(validation_error, ':', 1) AS error, COUNT(*) AS records,
                   MAX(received_at) AS latest
            FROM read_parquet('{path}', hive_partitioning = true) GROUP BY 1 ORDER BY records DESC
            """
        )

    def quarantine_samples(self, limit: int = 20) -> List[Dict[str, Any]]:
        path = self._glob(parquet_store.QUARANTINE_DATASET)
        if path is None:
            return []
        return self._rows(
            f"""
            SELECT received_at, validation_error, raw_value
            FROM read_parquet('{path}', hive_partitioning = true) ORDER BY received_at DESC LIMIT ?
            """,
            [limit],
        )

    def analytics_table(self, name: str) -> List[Dict[str, Any]]:
        path = self.lake_root / parquet_store.ANALYTICS_DATASET / f"{name}.parquet"
        if not path.exists():
            return []
        return self._rows(f"SELECT * FROM read_parquet('{path.as_posix()}')")

    def analytics_summary(self) -> Optional[Dict[str, Any]]:
        return _read_json(self.lake_root / parquet_store.ANALYTICS_DATASET / "summary.json")

    def pipeline_status(self) -> Optional[Dict[str, Any]]:
        return _read_json(self.lake_root / "_status" / "pipeline_status.json")

    def close(self) -> None:
        self.connection.close()


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
