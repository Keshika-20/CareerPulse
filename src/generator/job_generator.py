from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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
