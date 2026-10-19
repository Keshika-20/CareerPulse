import pytest

from src.storage.hbase_store import HBaseJobStore, LocalHBaseConnection


def _event(event, ts, **overrides):
    record = {
        "job_id": "J1",
        "company": "Google",
        "title": "Data Engineer",
        "location": "Bangalore",
        "experience": "2-4 years",
        "salary": 1500000.0,
        "category": "Data",
        "event": event,
        "claimed_event": event,
        "event_time": ts,
        "changed_fields": "",
    }
    record.update(overrides)
    return record


def test_emulator_keeps_versions_up_to_family_limit():
    connection = LocalHBaseConnection()
    connection.create_table("t", {"cf": {"max_versions": 2}})
    table = connection.table("t")
    for ts, value in ((1, "a"), (2, "b"), (3, "c")):
        table.put("row1", {"cf:col": value}, timestamp=ts)

    assert table.row("row1") == {b"cf:col": b"c"}
    assert table.cells("row1", "cf:col", include_timestamp=True) == [(b"c", 3), (b"b", 2)]


def test_emulator_scan_prefix_family_filter_and_counters():
    connection = LocalHBaseConnection()
    connection.create_table("t", {"a": {}, "b": {}})
    table = connection.table("t")
    table.put("job#1", {"a:x": "1", "b:y": "2"})
    table.put("job#2", {"a:x": "3"})
    table.put("other", {"a:x": "4"})

    assert [key for key, _ in table.scan(row_prefix="job#")] == [b"job#1", b"job#2"]
    assert dict(table.scan(row_prefix="job#1", columns=["b"])) == {b"job#1": {b"b:y": b"2"}}
    assert table.counter_inc("c", "a:n") == 1
    assert table.counter_inc("c", "a:n", 4) == 5
    assert table.counter_get("c", "a:n") == 5


def test_emulator_rejects_unknown_family():
    connection = LocalHBaseConnection()
    connection.create_table("t", {"a": {}})
    with pytest.raises(ValueError):
        connection.table("t").put("r", {"zz:col": "v"})


def test_job_lifecycle_state_history_and_counters():
    store = HBaseJobStore()
    store.apply_event(_event("NEW", "2026-10-06T10:00:00+00:00"), None)
    previous = store.get_job("J1")
    assert previous["status"] == "OPEN"
    assert previous["first_seen"] == "2026-10-06T10:00:00+00:00"

    store.apply_event(
        _event("MODIFIED", "2026-10-06T11:00:00+00:00", salary=1800000.0, changed_fields="salary"),
        previous,
    )
    store.apply_event(_event("DUPLICATE", "2026-10-06T11:30:00+00:00", salary=1800000.0), store.get_job("J1"))
    store.apply_event(_event("CLOSED", "2026-10-06T12:00:00+00:00", salary=1800000.0), store.get_job("J1"))

    state = store.get_job("J1")
    assert state["status"] == "CLOSED"
    assert state["salary"] == 1800000.0
    assert state["first_seen"] == "2026-10-06T10:00:00+00:00"

    history = store.get_history("J1")
    assert [item["event"] for item in history] == ["NEW", "MODIFIED", "DUPLICATE", "CLOSED"]
    assert history[1]["changed_fields"] == "salary"
    assert [item["event"] for item in store.get_history("J1", include_duplicates=False)] == ["NEW", "MODIFIED", "CLOSED"]

    salaries = [item["value"] for item in store.field_versions("J1", "salary")]
    assert salaries[:2] == [1800000.0, 1500000.0]

    stats = {row["company"]: row for row in store.company_stats()}
    assert stats["Google"] == {"company": "Google", "NEW": 1, "MODIFIED": 1, "DUPLICATE": 1, "CLOSED": 1}


def test_reopened_job_resets_first_seen():
    store = HBaseJobStore()
    store.apply_event(_event("NEW", "2026-10-06T10:00:00+00:00"), None)
    store.apply_event(_event("CLOSED", "2026-10-06T11:00:00+00:00"), store.get_job("J1"))
    store.apply_event(_event("NEW", "2026-10-07T09:00:00+00:00"), store.get_job("J1"))

    state = store.get_job("J1")
    assert state["status"] == "OPEN"
    assert state["first_seen"] == "2026-10-07T09:00:00+00:00"


def test_local_store_persists_and_opens_read_only(tmp_path):
    path = tmp_path / "hbase.sqlite3"
    store = HBaseJobStore.connect("local", path=path)
    store.put_job({"job_id": "J9", "company": "Uber", "title": "QA Engineer", "event": "NEW",
                   "timestamp": "2026-10-06T10:00:00Z"})
    store.close()

    reader = HBaseJobStore.connect("local", path=path, read_only=True)
    assert reader.get_job("J9")["company"] == "Uber"
    assert set(reader.list_jobs()) == {"J9"}
    reader.close()


def test_put_job_keeps_legacy_interface():
    store = HBaseJobStore()
    stored = store.put_job({"job_id": "J101", "company": "Google", "title": "Engineer", "location": "Bangalore",
                            "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T10:00:00Z",
                            "event": "NEW"})
    assert stored["status"] == "OPEN"
    assert store.get_job("J101")["salary"] == 1200000
    assert store.get_job("missing") is None


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        HBaseJobStore.connect("cassandra")


INCREMENT_SCRIPT = """
import sys
from src.storage.hbase_store import HBaseJobStore
store = HBaseJobStore.connect("local", path=sys.argv[1])
for _ in range(int(sys.argv[2])):
    store.stats.counter_inc("Google", "stats:NEW")
store.close()
"""


def test_counter_increments_are_atomic_across_processes(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "hbase.sqlite3"
    HBaseJobStore.connect("local", path=path).close()
    root = Path(__file__).resolve().parents[1]
    # Independent OS processes hammer the same counter concurrently.
    workers = [
        subprocess.Popen([sys.executable, "-c", INCREMENT_SCRIPT, str(path), "150"], cwd=root)
        for _ in range(4)
    ]
    assert [worker.wait(timeout=120) for worker in workers] == [0, 0, 0, 0]

    store = HBaseJobStore.connect("local", path=path)
    assert store.stats.counter_get("Google", "stats:NEW") == 600  # no lost updates
    store.close()


def test_batched_writes_match_single_writes():
    single, batched = HBaseJobStore(), HBaseJobStore()
    events = [
        (_event("NEW", "2026-10-06T10:00:00+00:00", event_id="a"), None),
    ]
    single.apply_event(*events[0])
    modified = _event("MODIFIED", "2026-10-06T11:00:00+00:00", salary=1.0, changed_fields="salary", event_id="b")
    single.apply_event(modified, single.get_job("J1"))

    first_state = HBaseJobStore.next_state(events[0][0], None)
    batched.apply_events([events[0], (modified, first_state)])

    assert single.get_job("J1") == batched.get_job("J1")
    assert single.get_history("J1") == batched.get_history("J1")
    assert single.company_stats() == batched.company_stats()
