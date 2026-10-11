from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import quote

from src.ingestion.base_source import JsonHttpClient, SourceFetchError, SourceScan


class LeverSource:
    source_name = "lever"
    PAGE_SIZE = 100
    MAX_PAGES = 1000

    def __init__(self, client: JsonHttpClient | None = None) -> None:
        self.client = client or JsonHttpClient()

    def fetch(self, site: str, company: str, region: str = "global") -> SourceScan:
        if not site.strip() or not company.strip():
            raise ValueError("Lever site and company must be non-empty.")
        if region not in {"global", "eu"}:
            raise ValueError("Lever region must be 'global' or 'eu'.")

        host = "api.eu.lever.co" if region == "eu" else "api.lever.co"
        base_url = f"https://{host}/v0/postings/{quote(site, safe='')}"
        first_records, first_pages = self._fetch_pass(base_url)
        verified_records, verified_pages = self._fetch_pass(base_url)
        if first_records != verified_records:
            raise SourceFetchError(
                "Lever postings changed between verification passes; scan is not complete."
            )
        raw_pages = verified_pages or first_pages
        records = verified_records

        return SourceScan(
            source=self.source_name,
            account_id=f"{region}:{site}",
            company=company,
            records=records,
            raw_pages=raw_pages,
            complete=True,
            fetched_count=len(records),
        )

    def _fetch_pass(self, base_url: str) -> tuple[List[Dict[str, Any]], list[Any]]:
        records: List[Dict[str, Any]] = []
        raw_pages = []

        for page_number in range(self.MAX_PAGES):
            skip = page_number * self.PAGE_SIZE
            payload = self.client.get_json(
                f"{base_url}?skip={skip}&limit={self.PAGE_SIZE}&mode=json"
            )
            if not isinstance(payload, list):
                raise SourceFetchError("Lever response page must be a JSON array.")
            raw_pages.append(payload)

            for posting in payload:
                if (
                    not isinstance(posting, dict)
                    or not _valid_provider_id(posting.get("id"))
                    or not isinstance(posting.get("text"), str)
                    or not posting["text"].strip()
                ):
                    raise SourceFetchError("Lever response contains a malformed posting.")
            records.extend(payload)

            if len(payload) < self.PAGE_SIZE:
                if len({str(posting["id"]) for posting in records}) != len(records):
                    raise SourceFetchError("Lever response contains duplicate posting IDs.")
                return records, raw_pages

        raise SourceFetchError(
            f"Lever scan reached the safety limit of {self.MAX_PAGES} pages without a final short page."
        )


def _valid_provider_id(value: Any) -> bool:
    return (
        isinstance(value, (str, int))
        and not isinstance(value, bool)
        and (not isinstance(value, str) or bool(value.strip()))
    )
