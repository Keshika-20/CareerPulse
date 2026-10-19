"""Stateful, replay-safe change detection against the operational job store (HBase)."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def event_id_for(record: Dict[str, Any]) -> str:
    """Deterministic event identity.

    Kafka records are identified by their (topic, partition, offset), which is
    stable across Spark replays. Records without a Kafka position fall back to
    a content hash, so re-processing the same input yields the same ID.
    """
    if record.get("event_id"):
        return str(record["event_id"])
    if record.get("kafka_offset") is not None:
        return f"{record.get('kafka_topic') or 'kafka'}-{record.get('kafka_partition')}-{record['kafka_offset']}"
    material = "|".join(
        str(record.get(field)) for field in ("job_id", "event_time", "event", "fingerprint")
    )
    return hashlib.sha1(material.encode("utf-8")).hexdigest()[:20]


class ChangeDetector:
    """Classifies ordered records and writes the resulting state to the store.

    Exactly-once *effect* on HBase: the store's history row doubles as a ledger
    of applied event IDs. A replayed event (e.g. after a crash, Spark re-runs
    the micro-batch from its checkpoint) is recognised and its original
    classification is returned without mutating state or counters again, so
    a replay re-emits the same events (only ``processed_at`` differs).
    """

    def __init__(self, store: Any) -> None:
        self.store = store

    def process(self, records: Iterable[Dict[str, Any]], batch_id: int = 0) -> List[Dict[str, Any]]:
        ordered = list(records)
        processed_at = datetime.now(timezone.utc).isoformat()
        states, applied = self.store.load_states(record["job_id"] for record in ordered)

        output: List[Dict[str, Any]] = []
        to_apply: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
        for record in ordered:
            job_id = str(record["job_id"])
            event_id = event_id_for(record)
            previous = states.get(job_id)
            prior = applied.get(job_id, {}).get(event_id)
            if prior is not None:
                # Replay of an event that was already applied: reuse its result.
                detected, changes = prior["event"], prior.get("changed_fields") or ""
            else:
                detected = detect_change(previous, record)
                changes = ",".join(changed_fields(previous, record)) if detected == "MODIFIED" else ""

            event = {
                **{key: value for key, value in record.items() if not key.startswith("kafka_")},
                "event_id": event_id,
                "batch_id": batch_id,
                "claimed_event": record.get("event"),
                "event": detected,
                "changed_fields": changes,
                "processed_at": processed_at,
            }
            if prior is None:
                to_apply.append((event, previous))
                states[job_id] = self.store.next_state(event, previous) or previous
                applied.setdefault(job_id, {})[event_id] = {"event": detected, "changed_fields": changes}
            output.append(event)

        if to_apply:
            self.store.apply_events(to_apply)
        return output
