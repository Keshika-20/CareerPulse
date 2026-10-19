from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Optional


class HBaseJobStore:
    def __init__(self) -> None:
        self._state: Dict[str, Dict[str, Any]] = {}

    def put_job(self, job_event: Dict[str, Any]) -> Dict[str, Any]:
        job_id = str(job_event["job_id"])
        self._state[job_id] = dict(job_event)
        return self._state[job_id]

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._state.get(str(job_id))

    def list_jobs(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._state)


if __name__ == "__main__":
    store = HBaseJobStore()
    store.put_job({"job_id": "J101", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T10:00:00Z", "event": "NEW"})
    print(store.get_job("J101"))
