from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class SourceFetchError(RuntimeError):
    """Raised when a public job source cannot be fetched completely."""


@dataclass
class SourceScan:
    source: str
    account_id: str
    company: str
    records: List[Dict[str, Any]]
    raw_pages: List[Any]
    complete: bool
    fetched_count: int
    error: Optional[str] = None


class JsonHttpClient:
    def __init__(
        self,
        timeout_seconds: float = 15,
        max_retries: int = 2,
        opener: Callable[..., Any] = urlopen,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive.")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative.")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.opener = opener
        self.sleep = sleep

    def get_json(self, url: str) -> Any:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "CareerPulse/1.0"})
        for attempt in range(self.max_retries + 1):
            try:
                with self.opener(request, timeout=self.timeout_seconds) as response:
                    body = response.read()
                try:
                    return json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise SourceFetchError(f"Source returned malformed JSON: {url}") from exc
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt == self.max_retries:
                    raise SourceFetchError(f"HTTP {exc.code} fetching {url}") from exc
            except (URLError, TimeoutError, socket.timeout) as exc:
                if attempt == self.max_retries:
                    raise SourceFetchError(f"Network error fetching {url}: {exc}") from exc
            self.sleep(min(0.25 * (2**attempt), 2.0))
        raise SourceFetchError(f"Exhausted retries fetching {url}")
