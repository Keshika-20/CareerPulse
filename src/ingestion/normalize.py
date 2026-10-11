from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit

from src.common.models import validate_record
from src.ingestion.base_source import SourceScan


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_valid_timestamp(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 timestamp.")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalize_provider_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = datetime.fromtimestamp(value / 1000, timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError("source_updated_at must be a valid epoch-millisecond value.") from exc
        return parsed.isoformat()
    return _normalize_valid_timestamp(value, "source_updated_at")


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Optional text fields must contain a string or None.")
    return value.strip() or None


def _valid_salary_bound(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _validated_salary_range(
    minimum: Any,
    maximum: Any,
    currency: Any,
    interval: Any,
) -> Dict[str, Any]:
    normalized_bounds = {}
    for name, value in (("salary_min", minimum), ("salary_max", maximum)):
        if value is not None and not _valid_salary_bound(value):
            raise ValueError(f"{name} must be a non-negative finite number or None.")
        normalized_bounds[name] = value
    if (
        normalized_bounds["salary_min"] is not None
        and normalized_bounds["salary_max"] is not None
        and normalized_bounds["salary_min"] > normalized_bounds["salary_max"]
    ):
        raise ValueError("salary_min cannot exceed salary_max.")
    return {
        **normalized_bounds,
        "salary_currency": _optional_text(currency),
        "salary_interval": _optional_text(interval),
    }


def _salary_range(source: str, record: Dict[str, Any]) -> Dict[str, Any]:
    if source == "lever":
        salary = record.get("salaryRange")
        if not isinstance(salary, dict):
            return {}
        normalized_range = _validated_salary_range(
            salary.get("min"),
            salary.get("max"),
            salary.get("currency"),
            salary.get("interval"),
        )
        return {"salary_ranges": [normalized_range], **normalized_range}

    ranges = record.get("pay_input_ranges")
    if not isinstance(ranges, list) or not ranges:
        return {}
    normalized_ranges = []
    for salary_range in ranges:
        if not isinstance(salary_range, dict):
            raise ValueError("Greenhouse pay_input_ranges entries must be objects.")
        minimum = salary_range.get("min_cents")
        maximum = salary_range.get("max_cents")
        for name, value in (("min_cents", minimum), ("max_cents", maximum)):
            if value is not None and not _valid_salary_bound(value):
                raise ValueError(f"{name} must be a non-negative finite number or None.")
        normalized_ranges.append(
            _validated_salary_range(
                minimum / 100 if minimum is not None else None,
                maximum / 100 if maximum is not None else None,
                salary_range.get("currency_type"),
                salary_range.get("interval"),
            )
        )
    result: Dict[str, Any] = {"salary_ranges": normalized_ranges}
    if len(normalized_ranges) == 1:
        result.update(normalized_ranges[0])
    return result


def normalize_listing(
    source: str,
    account_id: str,
    company: str,
    record: Dict[str, Any],
    observed_at: Optional[str] = None,
) -> Dict[str, Any]:
    observed_timestamp = _normalize_valid_timestamp(
        _utc_now() if observed_at is None else observed_at, "observed_at"
    )
    provider_id = str(record["id"])
    if source == "greenhouse":
        stable_id = f"greenhouse:{quote(account_id, safe='')}:{quote(provider_id, safe='')}"
        location_value = record.get("location")
        location = location_value.get("name") if isinstance(location_value, dict) else None
        url = record.get("absolute_url")
        provider_updated = record.get("updated_at")
        experience = _optional_text(record.get("experience"))
        range_fields = _salary_range(source, record)
    elif source == "lever":
        stable_id = f"lever:{quote(account_id, safe=':')}:{quote(provider_id, safe='')}"
        categories = record.get("categories")
        location = categories.get("location") if isinstance(categories, dict) else None
        url = record.get("hostedUrl")
        provider_updated = record.get("updatedAt")
        experience = _optional_text(record.get("experience"))
        range_fields = _salary_range(source, record)
    else:
        raise ValueError(f"Unsupported listing source: {source}")

    source_updated_at = _normalize_provider_timestamp(provider_updated)

    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"Listing {stable_id} is missing its original source URL.")
    parsed_url = urlsplit(url.strip())
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ValueError(f"Listing {stable_id} has an invalid original source URL.")
    normalized: Dict[str, Any] = {
        "job_id": stable_id,
        "source": source,
        "source_account_id": account_id,
        "source_job_id": provider_id,
        "source_url": url.strip(),
        "source_updated_at": source_updated_at,
        "company": company.strip(),
        "title": str(record["title"] if source == "greenhouse" else record["text"]).strip(),
        "location": _optional_text(location) or "Unspecified",
        "experience": experience,
        "salary": None,
        "timestamp": observed_timestamp,
        "event": "NEW",
        **range_fields,
    }
    valid, error = validate_record(normalized)
    if not valid:
        raise ValueError(f"Normalized listing {stable_id} failed validation: {error}")
    return normalized


def normalize_scan(scan: SourceScan, observed_at: Optional[str] = None) -> list[Dict[str, Any]]:
    if not scan.complete:
        raise ValueError(f"Cannot normalize incomplete scan for {scan.source}:{scan.account_id}.")
    scan_observed_at = _utc_now() if observed_at is None else observed_at
    normalized = []
    for record in scan.records:
        normalized.append(
            normalize_listing(
                scan.source,
                scan.account_id,
                scan.company,
                record,
                observed_at=scan_observed_at,
            )
        )
    return normalized
