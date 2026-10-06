from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

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


@dataclass
class JobEvent:
    job_id: str
    company: str
    title: str
    location: str
    experience: str
    salary: float
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
            experience=str(clean["experience"]).strip(),
            salary=float(clean["salary"]),
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

    missing = sorted(field for field in REQUIRED_FIELDS if field not in record or record[field] in (None, ""))
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    for field in ("job_id", "company", "title", "location", "experience", "event"):
        if not isinstance(record[field], str):
            return False, f"Field '{field}' must be a string."

    if not isinstance(record["salary"], (int, float)):
        return False, "Field 'salary' must be numeric."

    if record["event"].upper() not in VALID_EVENTS:
        return False, "Event type must be one of NEW, MODIFIED, CLOSED, DUPLICATE."

    return True, None
