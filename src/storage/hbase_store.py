"""HBase wide-column storage for operational job state and history.

Data model (table ``job_history``, row key = ``job_id``):

* ``info:*``   current job state, one JSON-encoded cell per field. The family
  keeps several cell versions, timestamped with the event time, so earlier
  values (e.g. salary revisions) stay readable with ``cells()``.
* ``events:<epoch_ms>#<seq>`` one column per lifecycle change. A job's row is
  therefore a wide, time-ordered event log - the classic HBase time-series
  layout.

Table ``company_stats`` (row key = company) keeps HBase atomic counters
``stats:<EVENT>`` for every processed event type.

``HBaseJobStore`` talks to anything implementing the small happybase API
subset used here: a real ``happybase.Connection`` (HBase Thrift server) or
``LocalHBaseConnection``, an embedded emulation of the same cell model backed by
SQLite so the pipeline can run without an HBase cluster.
"""

from __future__ import annotations

import json
import sqlite3
import struct
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

JOB_TABLE = "job_history"
STATS_TABLE = "company_stats"
INFO_VERSIONS = 10
JOB_FAMILIES = {
    "info": {"max_versions": INFO_VERSIONS},
    "events": {"max_versions": 1},
}
STATS_FAMILIES = {"stats": {"max_versions": 1}}
STATE_FIELDS = (
    "job_id",
    "company",
    "title",
    "location",
    "experience",
    "salary",
    "category",
    "source",
    "status",
    "first_seen",
    "last_seen",
    "last_event",
    "last_event_time",
    "fingerprint",
)


def _b(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def _now_ms() -> int:
    return int(time.time() * 1000)


class LocalTable:
    """happybase.Table-compatible view over the emulator's cell store."""

    def __init__(self, connection: "LocalHBaseConnection", name: bytes) -> None:
        self.connection = connection
        self.name = name

    def _families(self) -> Dict[bytes, int]:
        families = self.connection._families(self.name)
        if not families:
            raise ValueError(f"Table {self.name.decode()} does not exist.")
        return families

    def put(self, row: Any, data: Dict[Any, Any], timestamp: Optional[int] = None) -> None:
        families = self._families()
        ts = int(timestamp) if timestamp is not None else _now_ms()
        with self.connection._lock, self.connection._db:
            for column, value in data.items():
                column_b = _b(column)
                family = column_b.split(b":", 1)[0]
                if family not in families:
                    raise ValueError(f"Unknown column family {family.decode()!r}.")
                self.connection._db.execute(
                    "INSERT OR REPLACE INTO cells (tbl, row, col, ts, value) VALUES (?, ?, ?, ?, ?)",
                    (self.name, _b(row), column_b, ts, _b(value)),
                )
                self._trim_versions(_b(row), column_b, families[family])

    def _trim_versions(self, row: bytes, column: bytes, max_versions: int) -> None:
        self.connection._db.execute(
            """
            DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ? AND ts NOT IN (
                SELECT ts FROM cells WHERE tbl = ? AND row = ? AND col = ?
                ORDER BY ts DESC LIMIT ?
            )
            """,
            (self.name, row, column, self.name, row, column, max_versions),
        )

    @staticmethod
    def _matches(column: bytes, columns: Optional[Iterable[Any]]) -> bool:
        if not columns:
            return True
        for wanted in columns:
            wanted_b = _b(wanted)
            if b":" not in wanted_b or wanted_b.endswith(b":"):
                if column.split(b":", 1)[0] == wanted_b.rstrip(b":"):
                    return True
            elif column == wanted_b:
                return True
        return False

    def row(
        self,
        row: Any,
        columns: Optional[Iterable[Any]] = None,
        timestamp: Optional[int] = None,
        include_timestamp: bool = False,
    ) -> Dict[bytes, Any]:
        self._families()
        query = "SELECT col, ts, value FROM cells WHERE tbl = ? AND row = ?"
        params: List[Any] = [self.name, _b(row)]
        if timestamp is not None:
            query += " AND ts < ?"
            params.append(int(timestamp))
        query += " ORDER BY col, ts DESC"
        result: Dict[bytes, Any] = {}
        with self.connection._lock:
            cursor = self.connection._db.execute(query, params)
            for column, ts, value in cursor:
                column = bytes(column)
                if column in result or not self._matches(column, columns):
                    continue
                result[column] = (bytes(value), ts) if include_timestamp else bytes(value)
        return result

    def rows(self, rows: Iterable[Any], columns: Optional[Iterable[Any]] = None) -> List[Tuple[bytes, Dict[bytes, Any]]]:
        output = []
        for key in rows:
            data = self.row(key, columns=columns)
            if data:
                output.append((_b(key), data))
        return output

    def cells(
        self,
        row: Any,
        column: Any,
        versions: Optional[int] = None,
        timestamp: Optional[int] = None,
        include_timestamp: bool = False,
    ) -> List[Any]:
        self._families()
        query = "SELECT ts, value FROM cells WHERE tbl = ? AND row = ? AND col = ?"
        params: List[Any] = [self.name, _b(row), _b(column)]
        if timestamp is not None:
            query += " AND ts < ?"
            params.append(int(timestamp))
        query += " ORDER BY ts DESC"
        if versions is not None:
            query += " LIMIT ?"
            params.append(int(versions))
        with self.connection._lock:
            fetched = self.connection._db.execute(query, params).fetchall()
        if include_timestamp:
            return [(bytes(value), ts) for ts, value in fetched]
        return [bytes(value) for _ts, value in fetched]

    def scan(
        self,
        row_start: Any = None,
        row_stop: Any = None,
        row_prefix: Any = None,
        columns: Optional[Iterable[Any]] = None,
        limit: Optional[int] = None,
        **_ignored: Any,
    ) -> Iterator[Tuple[bytes, Dict[bytes, bytes]]]:
        self._families()
        query = "SELECT DISTINCT row FROM cells WHERE tbl = ?"
        params: List[Any] = [self.name]
        if row_prefix is not None:
            prefix = _b(row_prefix)
            query += " AND substr(row, 1, ?) = ?"
            params.extend([len(prefix), prefix])
        if row_start is not None:
            query += " AND row >= ?"
            params.append(_b(row_start))
        if row_stop is not None:
            query += " AND row < ?"
            params.append(_b(row_stop))
        query += " ORDER BY row"
        with self.connection._lock:
            keys = [bytes(item[0]) for item in self.connection._db.execute(query, params)]
        returned = 0
        for key in keys:
            data = self.row(key, columns=columns)
            if not data:
                continue
            yield key, data
            returned += 1
            if limit is not None and returned >= limit:
                return

    def counter_get(self, row: Any, column: Any) -> int:
        values = self.cells(row, column, versions=1)
        return struct.unpack(">q", values[0])[0] if values else 0

    def counter_inc(self, row: Any, column: Any, value: int = 1) -> int:
        with self.connection._lock:
            updated = self.counter_get(row, column) + int(value)
            self.put(row, {column: struct.pack(">q", updated)})
        return updated

    def delete(self, row: Any, columns: Optional[Iterable[Any]] = None) -> None:
        with self.connection._lock, self.connection._db:
            if not columns:
                self.connection._db.execute(
                    "DELETE FROM cells WHERE tbl = ? AND row = ?", (self.name, _b(row))
                )
                return
            existing = self.row(row, columns=columns)
            for column in existing:
                self.connection._db.execute(
                    "DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ?",
                    (self.name, _b(row), column),
                )


class LocalHBaseConnection:
    """Embedded stand-in for ``happybase.Connection``.

    It keeps HBase's data model - tables, column families, timestamped cell
    versions with a per-family version limit, row scans and atomic counters -
    in SQLite, so state survives restarts when a file path is given.
    """

    def __init__(self, path: str | Path | None = None, *, read_only: bool = False) -> None:
        self.path = str(path) if path else ":memory:"
        if path and not read_only:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        if path and read_only:
            uri = Path(path).resolve().as_uri() + "?mode=ro"
            self._db = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10)
        else:
            self._db = sqlite3.connect(self.path, check_same_thread=False, timeout=10)
        self._lock = threading.RLock()
        if not read_only:
            if path:
                self._db.execute("PRAGMA journal_mode=WAL")
            with self._db:
                self._db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS families (
                        tbl BLOB NOT NULL, family BLOB NOT NULL, max_versions INTEGER NOT NULL,
                        PRIMARY KEY (tbl, family)
                    )
                    """
                )
                self._db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cells (
                        tbl BLOB NOT NULL, row BLOB NOT NULL, col BLOB NOT NULL,
                        ts INTEGER NOT NULL, value BLOB NOT NULL,
                        PRIMARY KEY (tbl, row, col, ts)
                    )
                    """
                )

    def _families(self, table: bytes) -> Dict[bytes, int]:
        with self._lock:
            return {
                bytes(family): max_versions
                for family, max_versions in self._db.execute(
                    "SELECT family, max_versions FROM families WHERE tbl = ?", (table,)
                )
            }

    def tables(self) -> List[bytes]:
        with self._lock:
            return sorted({bytes(row[0]) for row in self._db.execute("SELECT tbl FROM families")})

    def create_table(self, name: Any, families: Dict[Any, Dict[str, Any]]) -> None:
        name_b = _b(name)
        if name_b in self.tables():
            raise ValueError(f"Table {name_b.decode()} already exists.")
        with self._lock, self._db:
            for family, options in families.items():
                self._db.execute(
                    "INSERT INTO families (tbl, family, max_versions) VALUES (?, ?, ?)",
                    (name_b, _b(family).rstrip(b":"), int((options or {}).get("max_versions", 3))),
                )

    def table(self, name: Any) -> LocalTable:
        return LocalTable(self, _b(name))

    def close(self) -> None:
        with self._lock:
            self._db.close()


def _encode(value: Any) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


def _decode(value: bytes) -> Any:
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return value.decode("utf-8", errors="replace")


def _epoch_ms(iso_timestamp: Optional[str]) -> int:
    from datetime import datetime, timezone

    if not iso_timestamp:
        return _now_ms()
    try:
        parsed = datetime.fromisoformat(str(iso_timestamp).replace("Z", "+00:00"))
    except ValueError:
        return _now_ms()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class HBaseJobStore:
    def __init__(self, connection: Any = None, *, table_prefix: str = "") -> None:
        self.connection = connection if connection is not None else LocalHBaseConnection()
        self.table_prefix = table_prefix
        self.job_table_name = f"{table_prefix}{JOB_TABLE}"
        self.stats_table_name = f"{table_prefix}{STATS_TABLE}"
        self._event_seq = 0
        self.ensure_tables()

    @classmethod
    def connect(
        cls,
        mode: str = "local",
        *,
        host: str = "localhost",
        port: int = 9090,
        path: str | Path | None = "./data/hbase/careerpulse_hbase.sqlite3",
        table_prefix: str = "",
        read_only: bool = False,
        retries: int = 1,
        retry_delay_seconds: float = 5.0,
    ) -> "HBaseJobStore":
        """Open a store on a real HBase Thrift server (``thrift``) or the emulator (``local``)."""
        if mode == "local":
            return cls(LocalHBaseConnection(path, read_only=read_only), table_prefix=table_prefix)
        if mode != "thrift":
            raise ValueError("HBase mode must be 'local' or 'thrift'.")
        try:
            import happybase
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("HBase Thrift mode requires happybase: pip install happybase") from exc

        last_error: Optional[Exception] = None
        for attempt in range(max(1, retries)):
            try:
                connection = happybase.Connection(host=host, port=int(port), timeout=30000)
                connection.open()
                return cls(connection, table_prefix=table_prefix)
            except Exception as exc:  # HBase can take a while to accept Thrift clients.
                last_error = exc
                if attempt + 1 < retries:
                    time.sleep(retry_delay_seconds)
        raise RuntimeError(f"Unable to connect to HBase Thrift at {host}:{port}: {last_error}")

    def ensure_tables(self) -> None:
        try:
            existing = {_b(name) for name in self.connection.tables()}
        except Exception:
            existing = set()
        for name, families in (
            (self.job_table_name, JOB_FAMILIES),
            (self.stats_table_name, STATS_FAMILIES),
        ):
            if _b(name) in existing:
                continue
            try:
                self.connection.create_table(name, families)
            except Exception:
                # A read-only emulator or a table created concurrently is fine.
                if _b(name) not in {_b(table) for table in self.connection.tables()}:
                    raise

    @property
    def jobs(self):
        return self.connection.table(self.job_table_name)

    @property
    def stats(self):
        return self.connection.table(self.stats_table_name)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        data = self.jobs.row(str(job_id), columns=[b"info"])
        if not data:
            return None
        return {key.split(b":", 1)[1].decode(): _decode(value) for key, value in data.items()}

    def put_job(
        self,
        job_event: Dict[str, Any],
        *,
        timestamp: Optional[int] = None,
        previous: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Write a job's current state into the ``info`` family.

        With ``previous``, only cells whose value changed are written, so each
        cell's version history records real changes (e.g. salary revisions).
        """
        state = {field: job_event.get(field) for field in STATE_FIELDS if field in job_event}
        state["job_id"] = str(job_event["job_id"])
        if "status" not in state:
            state["status"] = "CLOSED" if str(job_event.get("event", "")).upper() == "CLOSED" else "OPEN"
        ts = timestamp if timestamp is not None else _epoch_ms(job_event.get("timestamp"))
        cells = {
            f"info:{field}": _encode(value)
            for field, value in state.items()
            if previous is None or field not in previous or previous[field] != value
        }
        if cells:
            self.jobs.put(state["job_id"], cells, timestamp=ts)
        return self.get_job(state["job_id"]) or state

    def apply_event(self, event: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> None:
        """Persist one processed (change-detected) event.

        NEW / MODIFIED / CLOSED update the job's state and append a history
        column; DUPLICATE only refreshes ``last_seen``. Every event increments
        the company's counter for its type.
        """
        job_id = str(event["job_id"])
        detected = str(event["event"]).upper()
        ts = _epoch_ms(event.get("event_time"))

        if detected == "DUPLICATE":
            if previous is not None:
                self.jobs.put(job_id, {"info:last_seen": _encode(event.get("event_time"))}, timestamp=ts)
        else:
            base = previous or {}
            reopened = base.get("status") == "CLOSED" and detected == "NEW"
            state = {
                **{field: base.get(field) for field in STATE_FIELDS if field in base},
                **{
                    field: event.get(field)
                    for field in ("company", "title", "location", "experience", "salary", "category", "source", "fingerprint")
                    if field in event
                },
                "job_id": job_id,
                "status": "CLOSED" if detected == "CLOSED" else "OPEN",
                "first_seen": (None if reopened else base.get("first_seen")) or event.get("event_time"),
                "last_seen": event.get("event_time"),
                "last_event": detected,
                "last_event_time": event.get("event_time"),
            }
            self.put_job(state, timestamp=ts, previous=previous)

            self._event_seq = (self._event_seq + 1) % 1_000_000
            qualifier = f"events:{ts:013d}#{self._event_seq:06d}"
            history = {
                "event": detected,
                "claimed_event": event.get("claimed_event"),
                "event_time": event.get("event_time"),
                "changed_fields": event.get("changed_fields") or "",
                "title": event.get("title"),
                "location": event.get("location"),
                "salary": event.get("salary"),
                "experience": event.get("experience"),
                "event_id": event.get("event_id"),
            }
            self.jobs.put(job_id, {qualifier: _encode(history)}, timestamp=ts)

        company = str(event.get("company") or "Unknown")
        self.stats.counter_inc(company, f"stats:{detected}")

    def get_history(self, job_id: str) -> List[Dict[str, Any]]:
        data = self.jobs.row(str(job_id), columns=[b"events"])
        return [_decode(data[key]) for key in sorted(data)]

    def field_versions(self, job_id: str, field: str, versions: int = INFO_VERSIONS) -> List[Dict[str, Any]]:
        cells = self.jobs.cells(str(job_id), f"info:{field}", versions=versions, include_timestamp=True)
        return [{"value": _decode(value), "timestamp_ms": ts} for value, ts in cells]

    def list_jobs(self, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        jobs: Dict[str, Dict[str, Any]] = {}
        for key, data in self.jobs.scan(columns=[b"info"], limit=limit):
            jobs[key.decode()] = {
                column.split(b":", 1)[1].decode(): _decode(value) for column, value in data.items()
            }
        return jobs

    def company_stats(self) -> List[Dict[str, Any]]:
        rows = []
        for key, data in self.stats.scan(columns=[b"stats"]):
            counts = {
                column.split(b":", 1)[1].decode(): struct.unpack(">q", value)[0]
                for column, value in data.items()
                if len(value) == 8
            }
            rows.append({"company": key.decode(), **counts})
        return rows

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    store = HBaseJobStore()
    store.put_job({"job_id": "J101", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T10:00:00Z", "event": "NEW"})
    print(store.get_job("J101"))
