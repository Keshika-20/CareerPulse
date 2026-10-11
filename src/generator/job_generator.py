from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

COMPANIES = [
    "Google",
    "Microsoft",
    "Amazon",
    "Netflix",
    "Infosys",
    "TCS",
    "Accenture",
    "Uber",
    "Stripe",
    "Meta",
]
TITLES = [
    "Software Engineer",
    "Data Engineer",
    "Product Manager",
    "Machine Learning Engineer",
    "DevOps Engineer",
    "Data Analyst",
    "Backend Engineer",
    "Frontend Engineer",
    "Cloud Engineer",
    "QA Engineer",
]
LOCATIONS = [
    "Bangalore",
    "Hyderabad",
    "Pune",
    "Delhi",
    "Chennai",
    "Remote",
    "Mumbai",
    "Noida",
    "San Francisco",
    "Austin",
]
EXPERIENCE_LEVELS = ["0-2 years", "2-4 years", "4-6 years", "6-8 years", "8+ years"]
EVENT_TYPES = ["NEW", "MODIFIED", "CLOSED", "DUPLICATE"]


def generate_job_event(event_type: Optional[str] = None, job_id: Optional[str] = None) -> Dict[str, Any]:
    timestamp = datetime.now(timezone.utc).isoformat()
    event = (event_type or random.choice(EVENT_TYPES)).upper()
    payload = {
        "job_id": job_id or f"J{random.randint(1000, 9999)}",
        "company": random.choice(COMPANIES),
        "title": random.choice(TITLES),
        "location": random.choice(LOCATIONS),
        "experience": random.choice(EXPERIENCE_LEVELS),
        "salary": random.randint(600000, 3000000),
        "timestamp": timestamp,
        "event": event,
    }
    return payload


def generate_batch(count: int = 10, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
    return [generate_job_event(event_type=event_type) for _ in range(count)]


def generate_job_lifecycle(
    event_types: Sequence[str],
    job_id: Optional[str] = None,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if (
        isinstance(event_types, str)
        or not isinstance(event_types, Sequence)
        or not event_types
    ):
        raise ValueError("event_types must be a non-empty sequence of lifecycle events.")

    if any(not isinstance(event_type, str) for event_type in event_types):
        raise ValueError("event_types must contain only event-type strings.")
    normalized_events = [event_type.upper() for event_type in event_types]
    if any(event_type not in EVENT_TYPES for event_type in normalized_events):
        raise ValueError(f"event_types must contain only: {', '.join(EVENT_TYPES)}.")
    if normalized_events[0] != "NEW":
        raise ValueError("A job lifecycle must start with NEW.")
    if "CLOSED" in normalized_events[:-1]:
        raise ValueError("CLOSED must be the final lifecycle event.")
    if job_id is not None and (not isinstance(job_id, str) or not job_id.strip()):
        raise ValueError("job_id must be a non-empty string when provided.")

    rng = random.Random(seed)
    current_job_id = job_id or f"J{rng.randint(1000, 9999)}"
    details = {
        "company": rng.choice(COMPANIES),
        "title": rng.choice(TITLES),
        "location": rng.choice(LOCATIONS),
        "experience": rng.choice(EXPERIENCE_LEVELS),
        "salary": rng.randint(600000, 3000000),
    }
    events = []

    for event_type in normalized_events:
        if event_type == "MODIFIED":
            field = rng.choice(tuple(details))
            if field == "salary":
                choices = [salary for salary in range(600000, 3000001, 10000) if salary != details[field]]
                details[field] = rng.choice(choices)
            else:
                values = {
                    "company": COMPANIES,
                    "title": TITLES,
                    "location": LOCATIONS,
                    "experience": EXPERIENCE_LEVELS,
                }
                choices = [value for value in values[field] if value != details[field]]
                details[field] = rng.choice(choices)

        events.append({
            "job_id": current_job_id,
            **details,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
        })

    return events


def write_events(events: Iterable[Dict[str, Any]], output_path: str) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CareerPulse job-posting events.")
    parser.add_argument("--count", type=int, default=10, help="Number of job events to generate")
    parser.add_argument("--event-type", choices=EVENT_TYPES, default=None, help="Optional event type for every record")
    parser.add_argument("--output", default="./data/raw/job_events.jsonl", help="Path for generated JSONL output")
    args = parser.parse_args()

    events = generate_batch(count=args.count, event_type=args.event_type)
    output_file = write_events(events, args.output)
    print(f"Generated {len(events)} events to {output_file}")


if __name__ == "__main__":
    main()
