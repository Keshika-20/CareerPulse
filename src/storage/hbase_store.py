"""HBase wide-column storage for operational job state and history.

Data model (table ``job_history``, row key = ``job_id``):

* ``info:*``   current job state, one JSON-encoded cell per field. The family
  keeps several cell versions, timestamped with the event time, and only
  changed cells are rewritten, so a cell's versions are its change history.
* ``events:<epoch_ms:013d>#<event_id>`` one column per processed event. A job's
  row is a wide, time-ordered event log (the classic HBase time-series
  layout). Because the qualifier embeds the deterministic event ID, the same
  column doubles as an idempotency ledger: a replayed event is recognised and
  its original classification reused instead of being re-applied.

Table ``company_stats`` (row key = company) keeps HBase atomic counters
``stats:<EVENT>``.

Concurrency model: every event of one job is processed by exactly one writer
(Kafka is keyed by job_id and Spark repartitions micro-batches by job_id), so a
row is never mutated concurrently and read-then-write per row is safe without
check-and-put. Counters are the only cells shared across writers and use
HBase's server-side atomic increment.

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
from collections import Counter, defaultdict
from contextlib import contextmanager, nullcontext
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
    "employment_type",
    "work_mode",
    "education",
    "skills",
)
MULTI_GET_CHUNK = 500


def _b(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def _now_ms() -> int:
    return int(time.time() * 1000)


class LocalBatch:
    """happybase.Batch-compatible mutation buffer, applied in one transaction."""

    def __init__(self, table: "LocalTable", timestamp: Optional[int] = None) -> None:
        self.table = table
        self.timestamp = timestamp
        self._puts: List[Tuple[Any, Dict[Any, Any]]] = []

    def put(self, row: Any, data: Dict[Any, Any], wal: Any = None) -> None:
        self._puts.append((row, data))

    def send(self) -> None:
        if not self._puts:
            return
        with self.table.connection._write():
            for row, data in self._puts:
                self.table._put_unlocked(row, data, self.timestamp)
        self._puts = []

    def __enter__(self) -> "LocalBatch":
        return self

    def __exit__(self, exc_type, *_exc) -> None:
        if exc_type is None:
            self.send()


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

    def _put_unlocked(self, row: Any, data: Dict[Any, Any], timestamp: Optional[int]) -> None:
        families = self._families()
        ts = int(timestamp) if timestamp is not None else _now_ms()
        db = self.connection._db
        for column, value in data.items():
            column_b = _b(column)
            family = column_b.split(b":", 1)[0]
            if family not in families:
                raise ValueError(f"Unknown column family {family.decode()!r}.")
            db.execute(
                "INSERT OR REPLACE INTO cells (tbl, row, col, ts, value) VALUES (?, ?, ?, ?, ?)",
                (self.name, _b(row), column_b, ts, _b(value)),
            )
            if families[family] > 1:
                # Drop versions older than the newest N; the OFFSET lookup walks
                # the primary-key index and is a no-op until N is exceeded.
                db.execute(
                    """
                    DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ? AND ts <= (
                        SELECT ts FROM cells WHERE tbl = ? AND row = ? AND col = ?
                        ORDER BY ts DESC LIMIT 1 OFFSET ?
                    )
                    """,
                    (self.name, _b(row), column_b, self.name, _b(row), column_b, families[family]),
                )
            else:
                db.execute(
                    "DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ? AND ts < ?",
                    (self.name, _b(row), column_b, ts),
                )

    def put(self, row: Any, data: Dict[Any, Any], timestamp: Optional[int] = None) -> None:
        with self.connection._write():
            self._put_unlocked(row, data, timestamp)

    def batch(self, timestamp: Optional[int] = None, **_options: Any) -> LocalBatch:
        return LocalBatch(self, timestamp)

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
        # Read and write inside one IMMEDIATE transaction: the database write
        # lock makes the increment atomic across threads *and* processes.
        with self.connection._write():
            current = self.connection._db.execute(
                "SELECT value FROM cells WHERE tbl = ? AND row = ? AND col = ? ORDER BY ts DESC LIMIT 1",
                (self.name, _b(row), _b(column)),
            ).fetchone()
            updated = (struct.unpack(">q", bytes(current[0]))[0] if current else 0) + int(value)
            self.connection._db.execute(
                "DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ?",
                (self.name, _b(row), _b(column)),
            )
            self.connection._db.execute(
                "INSERT INTO cells (tbl, row, col, ts, value) VALUES (?, ?, ?, ?, ?)",
                (self.name, _b(row), _b(column), _now_ms(), struct.pack(">q", updated)),
            )
        return updated

    def delete(self, row: Any, columns: Optional[Iterable[Any]] = None) -> None:
        existing = self.row(row, columns=columns) if columns else None
        with self.connection._write():
            if not columns:
                self.connection._db.execute(
                    "DELETE FROM cells WHERE tbl = ? AND row = ?", (self.name, _b(row))
                )
                return
            for column in existing or {}:
                self.connection._db.execute(
                    "DELETE FROM cells WHERE tbl = ? AND row = ? AND col = ?",
                    (self.name, _b(row), column),
                )


class LocalHBaseConnection:
    """Embedded stand-in for ``happybase.Connection``.

    It keeps HBase's data model - tables, column families, timestamped cell
    versions with a per-family version limit, row scans and atomic counters -
    in SQLite, so state survives restarts when a file path is given. Several
    processes may share one file: writes take SQLite's write lock up front
    (``BEGIN IMMEDIATE``) and WAL mode lets readers proceed concurrently.
    """

    def __init__(self, path: str | Path | None = None, *, read_only: bool = False) -> None:
        self.path = str(path) if path else ":memory:"
        if path and not read_only:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        if path and read_only:
            target = Path(path).resolve().as_uri() + "?mode=ro"
            self._db = sqlite3.connect(target, uri=True, check_same_thread=False, timeout=30, isolation_level=None)
        else:
            self._db = sqlite3.connect(self.path, check_same_thread=False, timeout=30, isolation_level=None)
        self._lock = threading.RLock()
        self._depth = 0
        self._family_cache: Dict[bytes, Dict[bytes, int]] = {}
        if not read_only:
            if path:
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=NORMAL")
            with self._write():
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
                    ) WITHOUT ROWID
                    """
                )

    @contextmanager
    def _write(self):
        """One write transaction; nested calls join the outermost one."""
        with self._lock:
            if self._depth:
                self._depth += 1
                try:
                    yield
                finally:
                    self._depth -= 1
                return
            self._db.execute("BEGIN IMMEDIATE")
            self._depth = 1
            try:
                yield
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")
            finally:
                self._depth = 0

    def atomic(self):
        """Group many puts/batches into a single transaction (one disk sync)."""
        return self._write()

    def _families(self, table: bytes) -> Dict[bytes, int]:
        # Schemas are immutable once created, so they are cached per connection.
        cached = self._family_cache.get(table)
        if cached:
            return cached
        with self._lock:
            families = {
                bytes(family): max_versions
                for family, max_versions in self._db.execute(
                    "SELECT family, max_versions FROM families WHERE tbl = ?", (table,)
                )
            }
        if families:
            self._family_cache[table] = families
        return families

    def tables(self) -> List[bytes]:
        with self._lock:
            return sorted({bytes(row[0]) for row in self._db.execute("SELECT tbl FROM families")})

    def create_table(self, name: Any, families: Dict[Any, Dict[str, Any]]) -> None:
        name_b = _b(name)
        with self._write():
            if name_b in self.tables():
                raise ValueError(f"Table {name_b.decode()} already exists.")
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


def _history_qualifier(ts: int, event_id: str) -> str:
    return f"events:{ts:013d}#{event_id}"


def _event_id_from_qualifier(column: bytes) -> str:
    return column.split(b"#", 1)[1].decode() if b"#" in column else ""


class HBaseJobStore:
    def __init__(self, connection: Any = None, *, table_prefix: str = "") -> None:
        self.connection = connection if connection is not None else LocalHBaseConnection()
        self.table_prefix = table_prefix
        self.job_table_name = f"{table_prefix}{JOB_TABLE}"
        self.stats_table_name = f"{table_prefix}{STATS_TABLE}"
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
            except Exception as exc:
                # Several workers can race to create the same table; losing that
                # race ("already exists"/"already in use") is success. HBase may
                # not list a table that is still being created, so the error
                # text is trusted rather than an immediate tables() re-check.
                message = str(getattr(exc, "message", "") or exc).lower()
                if "already" in message or type(exc).__name__ == "AlreadyExists":
                    continue
                if _b(name) not in {_b(table) for table in self.connection.tables()}:
                    raise

    @property
    def jobs(self):
        return self.connection.table(self.job_table_name)

    @property
    def stats(self):
        return self.connection.table(self.stats_table_name)

    @staticmethod
    def _state_from_cells(data: Dict[bytes, bytes]) -> Optional[Dict[str, Any]]:
        state = {
            key.split(b":", 1)[1].decode(): _decode(value)
            for key, value in data.items()
            if key.startswith(b"info:")
        }
        return state or None

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._state_from_cells(self.jobs.row(str(job_id), columns=[b"info"]))

    def load_states(
        self, job_ids: Iterable[str]
    ) -> Tuple[Dict[str, Optional[Dict[str, Any]]], Dict[str, Dict[str, Dict[str, Any]]]]:
        """Multi-get current state and the applied-event ledger for many jobs.

        Returns ``(states, applied)`` where ``applied[job_id][event_id]`` is the
        stored history entry of every event already applied to that job.
        """
        keys = list(dict.fromkeys(str(job_id) for job_id in job_ids))
        states: Dict[str, Optional[Dict[str, Any]]] = {key: None for key in keys}
        applied: Dict[str, Dict[str, Dict[str, Any]]] = {key: {} for key in keys}
        table = self.jobs
        for start in range(0, len(keys), MULTI_GET_CHUNK):
            for key, data in table.rows(keys[start:start + MULTI_GET_CHUNK], columns=[b"info", b"events"]):
                job_id = key.decode()
                states[job_id] = self._state_from_cells(data)
                applied[job_id] = {
                    _event_id_from_qualifier(column): _decode(value)
                    for column, value in data.items()
                    if column.startswith(b"events:")
                }
        return states, applied

    def put_job(
        self,
        job_event: Dict[str, Any],
        *,
        timestamp: Optional[int] = None,
        previous: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Write a job's current state into the ``info`` family (single job)."""
        state, cells = self._state_cells(job_event, previous)
        ts = timestamp if timestamp is not None else _epoch_ms(job_event.get("timestamp"))
        if cells:
            self.jobs.put(state["job_id"], cells, timestamp=ts)
        return self.get_job(state["job_id"]) or state

    @staticmethod
    def _state_cells(
        job_event: Dict[str, Any], previous: Optional[Dict[str, Any]]
    ) -> Tuple[Dict[str, Any], Dict[str, bytes]]:
        state = {field: job_event.get(field) for field in STATE_FIELDS if field in job_event}
        state["job_id"] = str(job_event["job_id"])
        if "status" not in state:
            state["status"] = "CLOSED" if str(job_event.get("event", "")).upper() == "CLOSED" else "OPEN"
        # Only changed cells are written so each cell's versions are its history.
        cells = {
            f"info:{field}": _encode(value)
            for field, value in state.items()
            if previous is None or field not in previous or previous[field] != value
        }
        return state, cells

    @staticmethod
    def next_state(event: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """The job state after applying one detected event."""
        detected = str(event["event"]).upper()
        base = dict(previous or {})
        if detected == "DUPLICATE":
            if previous is None:
                return base
            return {**base, "last_seen": event.get("event_time")}
        reopened = base.get("status") == "CLOSED" and detected == "NEW"
        return {
            **{field: base.get(field) for field in STATE_FIELDS if field in base},
            **{
                field: event.get(field)
                for field in (
                    "company", "title", "location", "experience", "salary", "category", "source",
                    "fingerprint", "employment_type", "work_mode", "education", "skills",
                )
                if field in event
            },
            "job_id": str(event["job_id"]),
            "status": "CLOSED" if detected == "CLOSED" else "OPEN",
            "first_seen": (None if reopened else base.get("first_seen")) or event.get("event_time"),
            "last_seen": event.get("event_time"),
            "last_event": detected,
            "last_event_time": event.get("event_time"),
        }

    @staticmethod
    def _history_entry(event: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "event": str(event["event"]).upper(),
            "claimed_event": event.get("claimed_event"),
            "event_time": event.get("event_time"),
            "changed_fields": event.get("changed_fields") or "",
            "title": event.get("title"),
            "location": event.get("location"),
            "salary": event.get("salary"),
            "experience": event.get("experience"),
            "event_id": event.get("event_id"),
        }

    def apply_events(self, items: Iterable[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]]) -> int:
        """Persist processed events in bulk.

        ``items`` are ``(event, state_before_event)`` pairs in processing order.
        All cells of one event share its timestamp and go in one mutation
        batch (one Thrift round trip), and counter increments are aggregated
        per (company, event) so each distinct pair costs one atomic increment.
        """
        groups: Dict[int, Dict[str, Dict[str, bytes]]] = defaultdict(lambda: defaultdict(dict))
        counters: Counter = Counter()
        applied = 0
        for event, previous in items:
            job_id = str(event["job_id"])
            ts = _epoch_ms(event.get("event_time"))
            cells: Dict[str, bytes] = {}
            new_state = self.next_state(event, previous)
            if new_state:
                _state, cells = self._state_cells(new_state, previous)
            cells[_history_qualifier(ts, str(event.get("event_id") or f"{ts}"))] = _encode(self._history_entry(event))
            groups[ts][job_id].update(cells)
            counters[(str(event.get("company") or "Unknown"), str(event["event"]).upper())] += 1
            applied += 1

        table = self.jobs
        atomic = getattr(self.connection, "atomic", None)
        with atomic() if atomic else nullcontext():
            for ts in sorted(groups):
                with table.batch(timestamp=ts) as batch:
                    for job_id, cells in groups[ts].items():
                        batch.put(job_id, cells)
            stats = self.stats
            for (company, detected), count in sorted(counters.items()):
                stats.counter_inc(company, f"stats:{detected}", count)
        return applied

    def apply_event(self, event: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> None:
        """Persist one processed event (convenience wrapper over ``apply_events``)."""
        self.apply_events([(event, previous)])

    def get_history(self, job_id: str, *, include_duplicates: bool = True) -> List[Dict[str, Any]]:
        data = self.jobs.row(str(job_id), columns=[b"events"])
        history = [_decode(data[key]) for key in sorted(data)]
        if include_duplicates:
            return history
        return [entry for entry in history if entry.get("event") != "DUPLICATE"]

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
