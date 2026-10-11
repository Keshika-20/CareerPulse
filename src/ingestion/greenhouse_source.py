from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import quote

from src.ingestion.base_source import JsonHttpClient, SourceFetchError, SourceScan


class GreenhouseSource:
    source_name = "greenhouse"

    def __init__(self, client: JsonHttpClient | None = None) -> None:
        self.client = client or JsonHttpClient()

    def fetch(self, board_token: str, company: str) -> SourceScan:
        if not board_token.strip() or not company.strip():
            raise ValueError("Greenhouse board_token and company must be non-empty.")

        url = (
            "https://boards-api.greenhouse.io/v1/boards/"
            f"{quote(board_token, safe='')}/jobs?content=true"
        )
        payload = self.client.get_json(url)
        if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
            raise SourceFetchError("Greenhouse response must contain a jobs array.")

        jobs: List[Dict[str, Any]] = payload["jobs"]
        for job in jobs:
            if (
                not isinstance(job, dict)
                or not _valid_provider_id(job.get("id"))
                or not isinstance(job.get("title"), str)
                or not job["title"].strip()
            ):
                raise SourceFetchError("Greenhouse response contains a malformed job.")
        if len({str(job["id"]) for job in jobs}) != len(jobs):
            raise SourceFetchError("Greenhouse response contains duplicate job IDs.")

        meta = payload.get("meta")
        total = meta.get("total") if isinstance(meta, dict) else None
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise SourceFetchError("Greenhouse response has no verifiable non-negative meta.total.")
        if total != len(jobs):
            raise SourceFetchError(
                f"Greenhouse returned {len(jobs)} jobs but meta.total reports {total}."
            )

        return SourceScan(
            source=self.source_name,
            account_id=board_token,
            company=company,
            records=jobs,
            raw_pages=[payload],
            complete=True,
            fetched_count=len(jobs),
        )


def _valid_provider_id(value: Any) -> bool:
    return (
        isinstance(value, (str, int))
        and not isinstance(value, bool)
        and (not isinstance(value, str) or bool(value.strip()))
    )
