from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
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


# Relative hiring demand per title, plus a daily cycle (peak hour in UTC), so the
# simulated stream has learnable trends for the demand model.
TITLE_DEMAND = {
    "Software Engineer": (3.0, 6),
    "Data Engineer": (2.0, 9),
    "Product Manager": (1.0, 12),
    "Machine Learning Engineer": (2.5, 15),
    "DevOps Engineer": (1.2, 18),
    "Data Analyst": (1.5, 21),
    "Backend Engineer": (2.2, 3),
    "Frontend Engineer": (1.6, 8),
    "Cloud Engineer": (1.4, 14),
    "QA Engineer": (0.8, 20),
}
COMPANY_WEIGHTS = [3.0, 2.5, 3.0, 1.0, 2.0, 2.0, 1.8, 1.2, 1.0, 2.2]
SALARY_BANDS = {
    "0-2 years": (600000, 1400000),
    "2-4 years": (1000000, 2000000),
    "4-6 years": (1600000, 2600000),
    "6-8 years": (2000000, 3200000),
    "8+ years": (2600000, 4000000),
}
MALFORMED_SAMPLES = (
    lambda event: "{not valid json",
    lambda event: {**event, "company": ""},
    lambda event: {**event, "salary": "twelve lakh"},
    lambda event: {**event, "salary": -50000},
    lambda event: {**event, "event": "PROMOTED"},
    lambda event: {**event, "timestamp": "yesterday"},
    lambda event: {key: value for key, value in event.items() if key != "title"},
)


class JobEventSimulator:
    """Stateful job-event stream: postings are created, edited, re-sent and closed.

    Unlike ``generate_job_event`` (independent random records), every MODIFIED,
    DUPLICATE and CLOSED event refers to a posting the simulator opened earlier,
    so downstream change detection sees realistic lifecycles.
    """

    ACTION_WEIGHTS = {"NEW": 0.45, "MODIFIED": 0.22, "DUPLICATE": 0.10, "CLOSED": 0.23}

    def __init__(self, seed: Optional[int] = None, invalid_rate: float = 0.0, max_open: int = 400) -> None:
        if not 0 <= invalid_rate < 1:
            raise ValueError("invalid_rate must be in [0, 1).")
        self.rng = random.Random(seed)
        self.invalid_rate = invalid_rate
        self.max_open = max_open
        self.open_jobs: Dict[str, Dict[str, Any]] = {}
        self._next_id = 1000 + self.rng.randint(0, 8000) * 100

    def _demand_weights(self, moment: datetime) -> List[float]:
        hour = moment.hour + moment.minute / 60
        weights = []
        for title in TITLES:
            base, peak = TITLE_DEMAND[title]
            weights.append(base * (1.0 + 0.6 * math.cos(2 * math.pi * (hour - peak) / 24)))
        return weights

    def _salary(self, experience: str) -> int:
        low, high = SALARY_BANDS[experience]
        return int(round(self.rng.randint(low, high), -4))

    def _new_job(self, moment: datetime) -> Dict[str, Any]:
        self._next_id += 1
        experience = self.rng.choice(EXPERIENCE_LEVELS)
        return {
            "job_id": f"J{self._next_id}",
            "company": self.rng.choices(COMPANIES, weights=COMPANY_WEIGHTS)[0],
            "title": self.rng.choices(TITLES, weights=self._demand_weights(moment))[0],
            "location": self.rng.choice(LOCATIONS),
            "experience": experience,
            "salary": self._salary(experience),
        }

    def _modify(self, job: Dict[str, Any]) -> None:
        field = self.rng.choice(("salary", "salary", "location", "title", "experience"))
        if field == "salary":
            job["salary"] = int(round(job["salary"] * self.rng.uniform(1.03, 1.2), -4))
        elif field == "experience":
            job["experience"] = self.rng.choice([e for e in EXPERIENCE_LEVELS if e != job["experience"]])
        else:
            values = LOCATIONS if field == "location" else TITLES
            job[field] = self.rng.choice([value for value in values if value != job[field]])

    def next_event(self, moment: Optional[datetime] = None) -> Any:
        moment = moment or datetime.now(timezone.utc)
        if not self.open_jobs or len(self.open_jobs) < 5:
            action = "NEW"
        elif len(self.open_jobs) >= self.max_open:
            action = self.rng.choice(("MODIFIED", "CLOSED"))
        else:
            action = self.rng.choices(list(self.ACTION_WEIGHTS), weights=list(self.ACTION_WEIGHTS.values()))[0]

        if action == "NEW":
            job = self._new_job(moment)
            self.open_jobs[job["job_id"]] = job
        else:
            job = self.open_jobs[self.rng.choice(list(self.open_jobs))]
            if action == "MODIFIED":
                self._modify(job)
            elif action == "CLOSED":
                del self.open_jobs[job["job_id"]]

        event = {**job, "timestamp": moment.isoformat(), "event": action}
        if self.invalid_rate and self.rng.random() < self.invalid_rate:
            return self.rng.choice(MALFORMED_SAMPLES)(event)
        return event

    def generate(
        self,
        count: int,
        *,
        start: Optional[datetime] = None,
        span: Optional[timedelta] = None,
    ) -> List[Any]:
        """Generate ``count`` events, evenly spread over ``span`` from ``start``."""
        if count < 0:
            raise ValueError("count must be non-negative.")
        start = start or datetime.now(timezone.utc)
        step = (span / count) if span and count else timedelta(0)
        return [self.next_event(start + step * index) for index in range(count)]


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
    parser.add_argument("--event-type", choices=EVENT_TYPES, default=None, help="Optional event type for every record (independent mode only)")
    parser.add_argument("--output", default="./data/raw/job_events.jsonl", help="Path for generated JSONL output")
    parser.add_argument("--simulate", action="store_true", help="Use the stateful lifecycle simulator")
    parser.add_argument("--backfill-hours", type=float, default=0, help="Spread simulated events over the past N hours")
    parser.add_argument("--invalid-rate", type=float, default=0.0, help="Fraction of malformed records (simulator only)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    if args.simulate or args.backfill_hours or args.invalid_rate:
        simulator = JobEventSimulator(seed=args.seed, invalid_rate=args.invalid_rate)
        span = timedelta(hours=args.backfill_hours) if args.backfill_hours else None
        start = datetime.now(timezone.utc) - span if span else None
        events = simulator.generate(args.count, start=start, span=span)
    else:
        events = generate_batch(count=args.count, event_type=args.event_type)
    output_file = write_events(events, args.output)
    print(f"Generated {len(events)} events to {output_file}")


if __name__ == "__main__":
    main()
