from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

REQUIRED_FIELDS = {
    "job_id",
    "company",
    "title",
    "location",
    "experience",
    "salary",
    "timestamp",
    "event",
}

VALID_EVENTS = {"NEW", "MODIFIED", "CLOSED", "DUPLICATE"}

# Fields whose change makes a posting MODIFIED; timestamps and source metadata do not.
TRACKED_FIELDS = ("company", "title", "location", "experience", "salary")

# Ordered (category, pattern) rules applied to job titles. The first match wins, so
# specific families (ML, data) are listed before the generic engineering rule. The
# patterns are also compiled into Spark `rlike` expressions, so they must stay
# compatible with both Python `re` and Java regular expressions.
CATEGORY_RULES = (
    ("AI / ML", r"\b(machine learning|ml|ai|deep learning|data scientist|nlp|computer vision)\b"),
    ("Data", r"\b(data|analytics|analyst|bi|business intelligence|etl)\b"),
    ("DevOps / Cloud", r"\b(devops|cloud|sre|site reliability|infrastructure|platform|kubernetes)\b"),
    ("Security", r"\b(security|secops|appsec)\b"),
    ("Quality", r"\b(qa|quality|test|tester|sdet)\b"),
    ("Product", r"\b(product manager|product owner|program manager|project manager)\b"),
    ("Design", r"\b(designer|design|ux|ui)\b"),
    ("Engineering", r"\b(engineer|engineering|developer|software|backend|frontend|full[ -]?stack|mobile|android|ios)\b"),
    ("Sales & Marketing", r"\b(sales|account executive|marketing|growth|business development)\b"),
)
DEFAULT_CATEGORY = "Other"
_COMPILED_CATEGORY_RULES = tuple(
    (category, re.compile(pattern, re.IGNORECASE)) for category, pattern in CATEGORY_RULES
)


def derive_category(title: Any) -> str:
    """Map a free-text job title to a coarse hiring category."""
    text = str(title or "")
    for category, pattern in _COMPILED_CATEGORY_RULES:
        if pattern.search(text):
            return category
    return DEFAULT_CATEGORY


def job_fingerprint(record: Dict[str, Any]) -> str:
    """Stable hash of the tracked job details, used to spot duplicate postings."""
    values = {field: record.get(field) for field in TRACKED_FIELDS}
    if values["salary"] is not None:
        values["salary"] = float(values["salary"])
    encoded = json.dumps(values, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


@dataclass
class JobEvent:
    job_id: str
    company: str
    title: str
    location: str
    experience: Optional[str]
    salary: Optional[float]
    timestamp: str
    event: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "JobEvent":
        clean = defaultdict_to_dict(payload)
        return cls(
            job_id=str(clean["job_id"]).strip(),
            company=str(clean["company"]).strip(),
            title=str(clean["title"]).strip(),
            location=str(clean["location"]).strip(),
            experience=(
                str(clean["experience"]).strip()
                if clean["experience"] is not None
                else None
            ),
            salary=float(clean["salary"]) if clean["salary"] is not None else None,
            timestamp=str(clean["timestamp"]),
            event=str(clean["event"]).upper(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "company": self.company,
            "title": self.title,
            "location": self.location,
            "experience": self.experience,
            "salary": self.salary,
            "timestamp": self.timestamp,
            "event": self.event,
        }


def defaultdict_to_dict(value: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    raise TypeError("Job event payload must be a dictionary.")


def normalize_timestamp(timestamp: str) -> str:
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        return timestamp


def validate_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    if not isinstance(record, dict):
        return False, "Record must be a dictionary."

    nullable_fields = {"experience", "salary"}
    missing = sorted(
        field
        for field in REQUIRED_FIELDS
        if field not in record
        or record[field] == ""
        or (record[field] is None and field not in nullable_fields)
    )
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    for field in ("job_id", "company", "title", "location", "event"):
        if not isinstance(record[field], str):
            return False, f"Field '{field}' must be a string."

    if record["experience"] is not None and not isinstance(record["experience"], str):
        return False, "Field 'experience' must be a string or None."

    salary = record["salary"]
    if salary is not None:
        if isinstance(salary, bool) or not isinstance(salary, (int, float)):
            return False, "Field 'salary' must be a non-negative finite number or None."
        try:
            valid_salary = math.isfinite(salary) and salary >= 0
        except OverflowError:
            valid_salary = False
        if not valid_salary:
            return False, "Field 'salary' must be a non-negative finite number or None."

    if record["event"].upper() not in VALID_EVENTS:
        return False, "Event type must be one of NEW, MODIFIED, CLOSED, DUPLICATE."

    return True, None
