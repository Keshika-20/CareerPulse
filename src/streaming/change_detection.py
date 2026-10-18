"""Stateful change detection against the operational job store (HBase)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from src.common.models import TRACKED_FIELDS


def changed_fields(previous: Optional[Dict[str, Any]], current: Dict[str, Any]) -> List[str]:
    if previous is None:
        return []
    return [field for field in TRACKED_FIELDS if previous.get(field) != current.get(field)]


def detect_change(
    previous_state: Optional[Dict[str, Any]],
    current_record: Dict[str, Any],
) -> str:
    """Classify an incoming record as NEW, MODIFIED, CLOSED or DUPLICATE.

    The producer's claimed event type is only trusted for closures; everything
    else is derived by comparing the record with the stored state.
    """
    event_name = str(current_record.get("event", "")).upper()
    previously_closed = previous_state is not None and previous_state.get("status") == "CLOSED"

    # A closure event takes priority over changes to job fields; closing an
    # already closed posting again is a duplicate.
    if event_name == "CLOSED":
        return "DUPLICATE" if previously_closed else "CLOSED"

    # A job with no previous state, or one re-posted after closing, is new.
    if previous_state is None or previously_closed:
        return "NEW"

    if changed_fields(previous_state, current_record):
        return "MODIFIED"

    # The job details are unchanged; only metadata may differ.
    return "DUPLICATE"


class ChangeDetector:
    """Classifies ordered records and writes the resulting state to the store."""

    def __init__(self, store: Any) -> None:
        self.store = store

    def process(self, records: Iterable[Dict[str, Any]], batch_id: int = 0) -> List[Dict[str, Any]]:
        processed_at = datetime.now(timezone.utc).isoformat()
        # State read once per job per batch, then kept current as events apply,
        # so several events for one job inside a micro-batch see each other.
        state_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        output = []
        for index, record in enumerate(records):
            job_id = str(record["job_id"])
            if job_id not in state_cache:
                state_cache[job_id] = self.store.get_job(job_id)
            previous = state_cache[job_id]

            detected = detect_change(previous, record)
            changes = changed_fields(previous, record) if detected == "MODIFIED" else []
            event = {
                **record,
                "event_id": f"{job_id}:{record.get('event_time')}:{batch_id}:{index}",
                "batch_id": batch_id,
                "claimed_event": record.get("event"),
                "event": detected,
                "changed_fields": ",".join(changes),
                "processed_at": processed_at,
            }
            self.store.apply_event(event, previous)
            if detected != "DUPLICATE":
                state_cache[job_id] = {
                    **(previous or {}),
                    **{field: record.get(field) for field in TRACKED_FIELDS},
                    "status": "CLOSED" if detected == "CLOSED" else "OPEN",
                }
            output.append(event)
        return output
