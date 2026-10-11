from __future__ import annotations

import json
from typing import Any

import pytest

from src.common.models import JobEvent, validate_record
from src.ingestion.base_source import JsonHttpClient, SourceFetchError
from src.ingestion.greenhouse_source import GreenhouseSource
from src.ingestion.lever_source import LeverSource
from src.ingestion.normalize import normalize_listing, normalize_scan


class MockHttpResponse:
    def __init__(self, payload: Any) -> None:
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "MockHttpResponse":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def read(self) -> bytes:
        return self.body


class MockHttpTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = iter(responses)
        self.urls: list[str] = []
        self.timeouts: list[float] = []

    def __call__(self, request: Any, timeout: float) -> MockHttpResponse:
        self.urls.append(request.full_url)
        self.timeouts.append(timeout)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return MockHttpResponse(response)


def make_http_client(responses: list[Any]) -> tuple[JsonHttpClient, MockHttpTransport]:
    transport = MockHttpTransport(responses)
    client = JsonHttpClient(opener=transport, sleep=lambda _: None)
    return client, transport


def test_greenhouse_fetches_and_verifies_complete_board_snapshot():
    client, transport = make_http_client(
        [
            {
                "jobs": [
                    {
                        "id": 12,
                        "title": "Data Engineer",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/12",
                    }
                ],
                "meta": {"total": 1},
            }
        ]
    )

    scan = GreenhouseSource(client).fetch("acme-board", "Acme")

    assert scan.complete is True
    assert scan.fetched_count == 1
    assert scan.records[0]["id"] == 12
    assert "acme-board/jobs?content=true" in transport.urls[0]
    assert transport.timeouts == [15]


@pytest.mark.parametrize(
    "payload",
    [
        {"jobs": []},
        {"jobs": [{"id": 1}], "meta": {"total": 1}},
        {"jobs": [{"id": 1, "title": "Engineer"}], "meta": {"total": 2}},
        {
            "jobs": [
                {"id": 1, "title": "Engineer"},
                {"id": 1, "title": "Engineer"},
            ],
            "meta": {"total": 2},
        },
    ],
)
def test_greenhouse_rejects_unverifiable_or_malformed_snapshots(payload):
    with pytest.raises(SourceFetchError):
        GreenhouseSource(make_http_client([payload])[0]).fetch("acme", "Acme")


def test_lever_fetches_offset_pages_until_a_short_page():
    first_page = [
        {"id": str(index), "text": "Engineer"} for index in range(LeverSource.PAGE_SIZE)
    ]
    last_page = [{"id": "last", "text": "Analyst"}]
    client, transport = make_http_client(
        [first_page, last_page, first_page, last_page]
    )

    scan = LeverSource(client).fetch("acme", "Acme", region="eu")

    assert scan.complete is True
    assert scan.fetched_count == LeverSource.PAGE_SIZE + 1
    assert len(scan.raw_pages) == 2
    assert "api.eu.lever.co" in transport.urls[0]
    assert "skip=0&limit=100" in transport.urls[0]
    assert "skip=100&limit=100" in transport.urls[1]
    assert "skip=0&limit=100" in transport.urls[2]
    assert "skip=100&limit=100" in transport.urls[3]


@pytest.mark.parametrize(
    "payload",
    [
        {"not": "a list"},
        [{"id": "broken"}],
    ],
)
def test_lever_rejects_malformed_pages(payload):
    with pytest.raises(SourceFetchError):
        LeverSource(make_http_client([payload])[0]).fetch("acme", "Acme")


def test_normalize_greenhouse_listing_preserves_source_data_and_optional_values():
    normalized = normalize_listing(
        "greenhouse",
        "acme",
        "Acme",
        {
            "id": 12,
            "title": "Data Engineer",
            "location": {"name": "Remote"},
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/12",
            "updated_at": "2026-01-01T12:00:00-05:00",
            "pay_input_ranges": [
                {
                    "min_cents": 10000000,
                    "max_cents": 15000000,
                    "currency_type": "USD",
                    "interval": "yearly",
                }
            ],
        },
        observed_at="2026-01-02T00:00:00Z",
    )

    assert normalized["job_id"] == "greenhouse:acme:12"
    assert normalized["source_job_id"] == "12"
    assert normalized["source_url"] == "https://boards.greenhouse.io/acme/jobs/12"
    assert normalized["source_updated_at"] == "2026-01-01T17:00:00+00:00"
    assert normalized["timestamp"] == "2026-01-02T00:00:00+00:00"
    assert normalized["salary"] is None
    assert normalized["experience"] is None
    assert normalized["salary_min"] == 100000
    assert normalized["salary_max"] == 150000
    assert normalized["salary_currency"] == "USD"
    assert normalized["salary_interval"] == "yearly"
    assert validate_record(normalized) == (True, None)


def test_greenhouse_normalization_retains_multiple_salary_ranges_without_midpoint():
    normalized = normalize_listing(
        "greenhouse",
        "acme",
        "Acme",
        {
            "id": 13,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/13",
            "pay_input_ranges": [
                {"min_cents": 10000000, "max_cents": 15000000, "currency_type": "USD"},
                {"min_cents": 8000000, "max_cents": 12000000, "currency_type": "CAD"},
            ],
        },
        observed_at="2026-01-02T00:00:00Z",
    )

    assert normalized["salary"] is None
    assert len(normalized["salary_ranges"]) == 2
    assert "salary_min" not in normalized
    assert "salary_max" not in normalized


def test_normalize_lever_listing_keeps_missing_salary_and_provider_timestamp_optional():
    response = [
        {
            "id": "posting-7",
            "text": "Platform Engineer",
            "categories": {"location": "Remote"},
            "hostedUrl": "https://jobs.lever.co/acme/posting-7",
        }
    ]
    scan = LeverSource(
        make_http_client(
            [response, response]
        )[0]
    ).fetch("acme", "Acme")

    normalized = normalize_scan(scan, observed_at="2026-01-02T00:00:00Z")[0]

    assert normalized["job_id"] == "lever:global:acme:posting-7"
    assert normalized["source_url"] == "https://jobs.lever.co/acme/posting-7"
    assert normalized["source_updated_at"] is None
    assert normalized["salary"] is None
    assert normalized["experience"] is None
    assert validate_record(normalized) == (True, None)


def test_lever_epoch_millisecond_update_timestamp_is_normalized_to_utc():
    normalized = normalize_listing(
        "lever",
        "global:acme",
        "Acme",
        {
            "id": "p2",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/acme/p2",
            "updatedAt": 1767225600000,
        },
        observed_at="2026-01-02T00:00:00Z",
    )

    assert normalized["source_updated_at"] == "2026-01-01T00:00:00+00:00"


def test_job_event_model_and_validator_accept_only_explicit_optional_nulls():
    record = {
        "job_id": "greenhouse:acme:12",
        "company": "Acme",
        "title": "Data Engineer",
        "location": "Remote",
        "experience": None,
        "salary": None,
        "timestamp": "2026-01-02T00:00:00Z",
        "event": "NEW",
    }

    event = JobEvent.from_dict(record)

    assert event.experience is None
    assert event.salary is None
    assert event.to_dict() == record
    assert validate_record(record) == (True, None)
    assert validate_record({**record, "experience": 2})[0] is False
    assert validate_record({**record, "salary": "unknown"})[0] is False
    assert validate_record({**record, "salary": True})[0] is False
    assert validate_record({**record, "salary": 10**10000})[0] is False


def test_normalize_listing_rejects_invalid_provider_timestamps():
    with pytest.raises(ValueError, match="source_updated_at"):
        normalize_listing(
            "lever",
            "global:acme",
            "Acme",
            {
                "id": "p1",
                "text": "Engineer",
                "hostedUrl": "https://jobs.lever.co/acme/p1",
                "updatedAt": "not-a-timestamp",
            },
            observed_at="2026-01-02T00:00:00Z",
        )


def test_lever_scan_rejects_changes_between_verification_passes():
    first_pass = [{"id": "p1", "text": "Engineer"}]
    changed_pass = [{"id": "p1", "text": "Senior Engineer"}]
    client, _ = make_http_client([first_pass, changed_pass])

    with pytest.raises(SourceFetchError, match="verification passes"):
        LeverSource(client).fetch("acme", "Acme")
