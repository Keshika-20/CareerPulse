"""Record-level ETL rules: clean, validate and enrich one raw job event.

The Spark Structured Streaming job applies the same rules with DataFrame
expressions (see ``spark_kafka_consumer.apply_etl``); this module is the plain
Python form used by the local pipeline and by tests. Both produce the same
validation error codes:

``malformed_json``, ``missing_fields:<a,b>``, ``invalid_event``,
``invalid_salary``, ``invalid_timestamp``.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from src.common.models import (
    VALID_EVENTS,
    derive_category,
    job_fingerprint,
    merge_skills,
    standardize_employment_type,
    standardize_work_mode,
)

REQUIRED_NON_NULL = ("job_id", "company", "title", "location", "timestamp", "event")
TEXT_FIELDS = ("job_id", "company", "title", "location", "experience", "source")
_SPACES = re.compile(r"\s+")


def clean_text(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    if isinstance(value, bool):
        value = str(value).lower()
    text = _SPACES.sub(" ", str(value)).strip()
    return text or None


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_salary(value: Any) -> Tuple[Optional[float], bool]:
    """Return (salary, ok). Missing salary is allowed; non-numeric or negative is not."""
    if value is None or value == "":
        return None, True
    if isinstance(value, bool):
        return None, False
    try:
        salary = float(value)
    except (TypeError, ValueError, OverflowError):
        return None, False
    if not math.isfinite(salary) or salary < 0:
        return None, False
    return salary, True


def clean_and_validate(raw: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return (clean_record, None) for a valid event or (None, error_code)."""
    if isinstance(raw, (bytes, str)):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None, "malformed_json"
    if not isinstance(raw, dict):
        return None, "malformed_json"

    record: Dict[str, Any] = {field: clean_text(raw.get(field)) for field in TEXT_FIELDS}
    event = clean_text(raw.get("event") if raw.get("event") is not None else raw.get("event_type"))
    record["event"] = event.upper() if event else None
    record["timestamp"] = clean_text(raw.get("timestamp"))

    missing = [field for field in REQUIRED_NON_NULL if not record.get(field)]
    if missing:
        return None, f"missing_fields:{','.join(missing)}"
    if record["event"] not in VALID_EVENTS:
        return None, "invalid_event"

    salary, salary_ok = parse_salary(raw.get("salary"))
    if not salary_ok:
        return None, "invalid_salary"
    record["salary"] = salary

    event_time = parse_timestamp(record["timestamp"])
    if event_time is None:
        return None, "invalid_timestamp"

    record["event_time"] = event_time.isoformat()
    record["category"] = derive_category(record["title"])
    record["fingerprint"] = job_fingerprint(record)
    enrich_optional_fields(record, raw)
    return record, None


def enrich_optional_fields(record: Dict[str, Any], raw: Dict[str, Any]) -> None:
    """Standardise the optional high-dimensional fields; they never reject a record.

    ``requirements`` is a nested object and ``description`` is unstructured
    text: skills come from both (declared skills normalised through aliases,
    plus skills recognised in the text).
    """
    requirements = raw.get("requirements") if isinstance(raw.get("requirements"), dict) else {}
    record["employment_type"] = standardize_employment_type(clean_text(raw.get("employment_type")))
    record["work_mode"] = standardize_work_mode(clean_text(raw.get("work_mode")), record.get("location"))
    record["education"] = clean_text(requirements.get("education"))
    record["skills"] = merge_skills(requirements.get("skills"), clean_text(raw.get("description")))
    record["skill_count"] = len(record["skills"])
