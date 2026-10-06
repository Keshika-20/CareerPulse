from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List


def aggregate_company_counts(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts = Counter(str(record.get("company", "Unknown")).strip() for record in records)
    return [{"company": company, "count": value} for company, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def aggregate_location_counts(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts = Counter(str(record.get("location", "Unknown")).strip() for record in records)
    return [{"location": location, "count": value} for location, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def average_salary_by_company(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[float]] = defaultdict(list)
    for record in records:
        company = str(record.get("company", "Unknown")).strip()
        grouped[company].append(float(record.get("salary", 0)))

    result = []
    for company, salaries in sorted(grouped.items()):
        average = sum(salaries) / len(salaries) if salaries else 0.0
        result.append({"company": company, "average_salary": round(average, 2)})
    return result


def build_summary(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(records)
    if not rows:
        return {
            "total_jobs": 0,
            "new_jobs": 0,
            "modified_jobs": 0,
            "closed_jobs": 0,
            "top_companies": [],
            "average_salary_by_company": [],
        }

    event_counts = Counter(str(record.get("event", "")).upper() for record in rows)
    summary = {
        "total_jobs": len(rows),
        "new_jobs": event_counts.get("NEW", 0),
        "modified_jobs": event_counts.get("MODIFIED", 0),
        "closed_jobs": event_counts.get("CLOSED", 0),
        "top_companies": aggregate_company_counts(rows)[:5],
        "average_salary_by_company": average_salary_by_company(rows),
    }
    return summary


if __name__ == "__main__":
    sample = [
        {"company": "Google", "salary": 1400000, "event": "NEW"},
        {"company": "Google", "salary": 1600000, "event": "MODIFIED"},
        {"company": "Amazon", "salary": 1800000, "event": "NEW"},
    ]
    print(build_summary(sample))
