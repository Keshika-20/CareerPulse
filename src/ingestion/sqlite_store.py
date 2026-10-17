from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Optional

from src.common.models import validate_record
from src.ingestion.base_source import SourceScan


class IncompleteScanError(ValueError):
    """Raised when a scan cannot safely be used to infer lifecycle changes."""


class SQLiteSnapshotStore:
    _TRACKED_FIELDS = (
        "company",
        "title",
        "location",
        "experience",
        "salary",
        "salary_min",
        "salary_max",
        "salary_currency",
        "salary_interval",
        "salary_ranges",
        "source_url",
    )

    def __init__(self, database_path: str | Path, raw_retention_days: int = 7) -> None:
        if (
            isinstance(raw_retention_days, bool)
            or not isinstance(raw_retention_days, int)
            or raw_retention_days < 0
        ):
            raise ValueError("raw_retention_days must be a non-negative integer.")
        self.database_path = str(database_path)
        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.raw_retention_days = raw_retention_days
        self._connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        if self.database_path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS job_snapshots (
                job_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                account_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                is_active INTEGER NOT NULL CHECK (is_active IN (0, 1)),
                absence_count INTEGER NOT NULL DEFAULT 0 CHECK (absence_count >= 0),
                last_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_job_snapshots_account
                ON job_snapshots (source, account_id, is_active);

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                account_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                fetched_count INTEGER NOT NULL,
                new_count INTEGER NOT NULL,
                modified_count INTEGER NOT NULL,
                unchanged_count INTEGER NOT NULL,
                closed_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lifecycle_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id)
                    ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
                job_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (event_type IN ('NEW', 'MODIFIED', 'CLOSED')),
                event_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                kafka_status TEXT CHECK (kafka_status IN ('pending', 'published'))
            );
            CREATE INDEX IF NOT EXISTS idx_lifecycle_events_job
                ON lifecycle_events (job_id, event_id);

            CREATE TABLE IF NOT EXISTS raw_snapshots (
                run_id TEXT PRIMARY KEY REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
                captured_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )
        # Databases created before Kafka delivery tracking lack this column.
        columns = {
            row["name"]
            for row in self._connection.execute("PRAGMA table_info(lifecycle_events)")
        }
        if "kafka_status" not in columns:
            self._connection.execute(
                "ALTER TABLE lifecycle_events ADD COLUMN kafka_status TEXT "
                "CHECK (kafka_status IN ('pending', 'published'))"
            )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lifecycle_events_kafka_pending
                ON lifecycle_events (event_id) WHERE kafka_status = 'pending'
            """
        )

    @contextmanager
    def _transaction(self) -> Generator[sqlite3.Connection, None, None]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield self._connection
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    @staticmethod
    def _observation_timestamp(value: Optional[str]) -> str:
        raw = datetime.now(timezone.utc).isoformat() if value is None else value
        if not isinstance(raw, str):
            raise ValueError("observed_at must be an ISO-8601 string.")
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("observed_at must be a valid ISO-8601 timestamp.") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()

    def _validate_scan(
        self,
        scan: SourceScan,
        listings: Iterable[Dict[str, Any]],
    ) -> list[Dict[str, Any]]:
        if not isinstance(scan, SourceScan) or not scan.complete:
            raise IncompleteScanError("Only complete source scans can update snapshots.")
        if scan.fetched_count != len(scan.records):
            raise IncompleteScanError("Fetched count does not match the source scan records.")
        normalized = list(listings)
        if len(normalized) != scan.fetched_count:
            raise IncompleteScanError(
                "Normalized listing count does not match the complete source scan."
            )

        seen_job_ids = set()
        for listing in normalized:
            valid, error = validate_record(listing)
            if not valid:
                raise IncompleteScanError(f"Invalid normalized listing: {error}")
            if listing.get("source") != scan.source:
                raise IncompleteScanError("Normalized listing source does not match its scan.")
            if listing.get("source_account_id") != scan.account_id:
                raise IncompleteScanError("Normalized listing account does not match its scan.")
            job_id = listing["job_id"]
            if job_id in seen_job_ids:
                raise IncompleteScanError(f"Duplicate normalized job ID in scan: {job_id}")
            seen_job_ids.add(job_id)
        return normalized

    @classmethod
    def _changed(cls, old: Dict[str, Any], new: Dict[str, Any]) -> bool:
        return any(old.get(field) != new.get(field) for field in cls._TRACKED_FIELDS)

    @staticmethod
    def _event_payload(listing: Dict[str, Any], event_type: str, observed_at: str) -> Dict[str, Any]:
        return {**listing, "event": event_type, "timestamp": observed_at}

    def apply_scan(
        self,
        scan: SourceScan,
        listings: Iterable[Dict[str, Any]],
        observed_at: Optional[str] = None,
        *,
        queue_for_kafka: bool = False,
    ) -> Dict[str, Any]:
        normalized = self._validate_scan(scan, listings)
        # Queued in the same transaction as the events, so a crash cannot
        # commit lifecycle changes that Kafka will never be offered.
        kafka_status = "pending" if queue_for_kafka else None
        scan_at = self._observation_timestamp(observed_at)
        run_id = str(uuid.uuid4())
        counts = {"new": 0, "modified": 0, "unchanged": 0, "closed": 0}
        seen_ids = {listing["job_id"] for listing in normalized}

        with self._transaction() as connection:
            for listing in normalized:
                conflicting_row = connection.execute(
                    """
                    SELECT source, account_id FROM job_snapshots
                    WHERE job_id = ?
                    """,
                    (listing["job_id"],),
                ).fetchone()
                if conflicting_row is not None and (
                    conflicting_row["source"] != scan.source
                    or conflicting_row["account_id"] != scan.account_id
                ):
                    raise IncompleteScanError(
                        f"Job ID {listing['job_id']} is already assigned to another source account."
                    )

            previous_rows = connection.execute(
                """
                SELECT job_id, payload_json, is_active, absence_count
                FROM job_snapshots
                WHERE source = ? AND account_id = ?
                """,
                (scan.source, scan.account_id),
            ).fetchall()
            previous_by_id = {row["job_id"]: row for row in previous_rows}

            for listing in normalized:
                previous = previous_by_id.get(listing["job_id"])
                old_payload = (
                    json.loads(previous["payload_json"])
                    if previous is not None
                    else None
                )
                if previous is None or not previous["is_active"]:
                    event_type = "NEW"
                    counts["new"] += 1
                    event_payload = self._event_payload(listing, event_type, scan_at)
                    self._insert_event(connection, run_id, listing["job_id"], event_payload, kafka_status)
                elif self._changed(old_payload, listing):
                    event_type = "MODIFIED"
                    counts["modified"] += 1
                    event_payload = self._event_payload(listing, event_type, scan_at)
                    self._insert_event(connection, run_id, listing["job_id"], event_payload, kafka_status)
                else:
                    counts["unchanged"] += 1

                connection.execute(
                    """
                    INSERT INTO job_snapshots
                        (job_id, source, account_id, payload_json, is_active,
                         absence_count, last_seen_at, updated_at)
                    VALUES (?, ?, ?, ?, 1, 0, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        source = excluded.source,
                        account_id = excluded.account_id,
                        payload_json = excluded.payload_json,
                        is_active = 1,
                        absence_count = 0,
                        last_seen_at = excluded.last_seen_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        listing["job_id"],
                        scan.source,
                        scan.account_id,
                        json.dumps(listing, sort_keys=True, allow_nan=False),
                        listing["timestamp"],
                        scan_at,
                    ),
                )

            for previous in previous_rows:
                job_id = previous["job_id"]
                if job_id in seen_ids or not previous["is_active"]:
                    continue
                next_absences = int(previous["absence_count"]) + 1
                if next_absences >= 2:
                    old_payload = json.loads(previous["payload_json"])
                    close_payload = self._event_payload(old_payload, "CLOSED", scan_at)
                    self._insert_event(connection, run_id, job_id, close_payload, kafka_status)
                    connection.execute(
                        """
                        UPDATE job_snapshots
                        SET payload_json = ?, is_active = 0, absence_count = ?,
                            updated_at = ?
                        WHERE job_id = ?
                        """,
                        (
                            json.dumps(close_payload, sort_keys=True, allow_nan=False),
                            next_absences,
                            scan_at,
                            job_id,
                        ),
                    )
                    counts["closed"] += 1
                else:
                    connection.execute(
                        """
                        UPDATE job_snapshots
                        SET absence_count = ?, updated_at = ?
                        WHERE job_id = ?
                        """,
                        (next_absences, scan_at, job_id),
                    )

            connection.execute(
                """
                INSERT INTO ingestion_runs
                    (run_id, source, account_id, observed_at, fetched_count,
                     new_count, modified_count, unchanged_count, closed_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    scan.source,
                    scan.account_id,
                    scan_at,
                    scan.fetched_count,
                    counts["new"],
                    counts["modified"],
                    counts["unchanged"],
                    counts["closed"],
                ),
            )

            if self.raw_retention_days > 0:
                connection.execute(
                    """
                    INSERT INTO raw_snapshots (run_id, captured_at, payload_json)
                    VALUES (?, ?, ?)
                    """,
                    (
                        run_id,
                        scan_at,
                        json.dumps(scan.raw_pages, sort_keys=True, allow_nan=False),
                    ),
                )
            # Prune even when retention is 0 so payloads kept under an earlier
            # setting do not linger. Compare parsed datetimes, not ISO strings.
            connection.execute(
                """
                DELETE FROM raw_snapshots
                WHERE datetime(captured_at) < datetime(?, ?)
                """,
                (scan_at, f"-{self.raw_retention_days} days"),
            )

        return {
            "run_id": run_id,
            "source": scan.source,
            "account_id": scan.account_id,
            "fetched": scan.fetched_count,
            **counts,
        }

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        run_id: str,
        job_id: str,
        payload: Dict[str, Any],
        kafka_status: Optional[str] = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lifecycle_events
                (run_id, job_id, event_type, event_at, payload_json, kafka_status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                job_id,
                payload["event"],
                payload["timestamp"],
                json.dumps(payload, sort_keys=True, allow_nan=False),
                kafka_status,
            ),
        )

    def get_snapshot(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self._connection.execute(
            """
            SELECT payload_json, is_active, absence_count, last_seen_at
            FROM job_snapshots WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            **json.loads(row["payload_json"]),
            "is_active": bool(row["is_active"]),
            "absence_count": row["absence_count"],
            "last_seen_at": row["last_seen_at"],
        }

    def list_events(self, job_id: Optional[str] = None) -> list[Dict[str, Any]]:
        if job_id is None:
            rows = self._connection.execute(
                """
                SELECT job_id, event_type, event_at, payload_json
                FROM lifecycle_events ORDER BY event_id
                """
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT job_id, event_type, event_at, payload_json
                FROM lifecycle_events WHERE job_id = ? ORDER BY event_id
                """,
                (job_id,),
            ).fetchall()
        return [
            {
                "job_id": row["job_id"],
                "event": row["event_type"],
                "timestamp": row["event_at"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def list_events_for_run(self, run_id: str) -> list[Dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT job_id, event_type, event_at, payload_json
            FROM lifecycle_events WHERE run_id = ? ORDER BY event_id
            """,
            (run_id,),
        ).fetchall()
        return [self._flat_event(row) for row in rows]

    @staticmethod
    def _flat_event(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "job_id": row["job_id"],
            "event": row["event_type"],
            "timestamp": row["event_at"],
            **json.loads(row["payload_json"]),
        }

    def list_pending_kafka_events(self) -> list[tuple[int, Dict[str, Any]]]:
        """Return (event_id, event) pairs not yet acknowledged by Kafka, oldest first."""
        rows = self._connection.execute(
            """
            SELECT event_id, job_id, event_type, event_at, payload_json
            FROM lifecycle_events WHERE kafka_status = 'pending' ORDER BY event_id
            """
        ).fetchall()
        return [(row["event_id"], self._flat_event(row)) for row in rows]

    def mark_kafka_published(self, event_id: int) -> None:
        self._connection.execute(
            "UPDATE lifecycle_events SET kafka_status = 'published' WHERE event_id = ?",
            (event_id,),
        )

    def list_active_snapshots(self) -> list[Dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT payload_json FROM job_snapshots
            WHERE is_active = 1 ORDER BY job_id
            """
        ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def list_runs(self) -> list[Dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT * FROM ingestion_runs ORDER BY observed_at, run_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def raw_snapshot_count(self) -> int:
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM raw_snapshots"
        ).fetchone()
        return int(row["count"])

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "SQLiteSnapshotStore":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
