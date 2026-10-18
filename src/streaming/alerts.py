"""Monitoring and notification: hiring-surge and data-quality alerts.

Alerts are detected on the driver from small aggregates (window metrics and
micro-batch summaries), then delivered by ``AlertSink`` to:

* the Parquet lake (``alerts/``), read by the dashboard,
* a Kafka topic (``job-alerts``) for real-time consumers (see ``alert_consumer``),
* optionally an HTTP webhook (e.g. a Slack/Teams incoming webhook) to trigger
  workflows.

Every alert has a deterministic ``alert_id`` (type + subject + window), so a
re-evaluated window or a replayed micro-batch never fires the same alert twice.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.storage import parquet_store


@dataclass(frozen=True)
class AlertConfig:
    window_minutes: int = 5          # alerts use non-overlapping windows of this length
    history_windows: int = 12        # baseline = this many previous windows per category
    min_history: int = 3             # need at least this many to judge a surge
    surge_ratio: float = 2.0         # observed >= ratio * baseline mean ...
    surge_z: float = 3.0             # ... and >= mean + z * stdev (Poisson-floored) ...
    min_new_postings: int = 5        # ... and at least this many postings
    max_rejection_rate: float = 0.2  # data-quality alert above this share of rejected records
    min_batch_records: int = 20      # ignore tiny micro-batches for the rejection rate


def _utc(value: Any) -> Optional[datetime]:
    return parquet_store.to_utc_datetime(value)


def _aligned(moment: datetime, minutes: int) -> bool:
    return (moment.hour * 60 + moment.minute) % minutes == 0 and moment.second == 0


def category_windows(
    events: Iterable[Dict[str, Any]], *, window_minutes: int = 5, slide_minutes: int = 1
) -> List[Dict[str, Any]]:
    """Python form of the streaming per-category window metrics (local runner)."""
    buckets: Dict[tuple, Dict[str, Any]] = {}
    window = timedelta(minutes=window_minutes)
    for event in events:
        moment = _utc(event.get("event_time"))
        if moment is None:
            continue
        minute = moment.replace(second=0, microsecond=0)
        first = minute - timedelta(minutes=window_minutes - slide_minutes)
        start = first - timedelta(minutes=first.minute % slide_minutes)
        while start <= moment:
            if start + window > moment:
                key = (start, event.get("category") or "Other")
                bucket = buckets.setdefault(key, {
                    "window_start": start, "window_end": start + window, "category": key[1],
                    "new_postings": 0, "closed_postings": 0, "events": 0,
                    "_salaries": [], "_companies": set(),
                })
                claimed = str(event.get("claimed_event") or event.get("event") or "").upper()
                bucket["events"] += 1
                bucket["new_postings"] += claimed == "NEW"
                bucket["closed_postings"] += claimed == "CLOSED"
                if claimed in {"NEW", "MODIFIED"} and event.get("salary") is not None:
                    bucket["_salaries"].append(float(event["salary"]))
                if event.get("company"):
                    bucket["_companies"].add(event["company"])
            start += timedelta(minutes=slide_minutes)
    rows = []
    for bucket in buckets.values():
        salaries = bucket.pop("_salaries")
        bucket["avg_salary"] = round(statistics.fmean(salaries), 2) if salaries else None
        bucket["active_companies"] = len(bucket.pop("_companies"))
        rows.append(bucket)
    return sorted(rows, key=lambda row: (row["window_start"], row["category"]))


def detect_surges(
    current: Iterable[Dict[str, Any]],
    history: Iterable[Dict[str, Any]],
    config: AlertConfig = AlertConfig(),
) -> List[Dict[str, Any]]:
    """Flag categories whose NEW postings in a window jump far above their baseline.

    Only windows aligned to ``window_minutes`` are judged, so overlapping
    sliding windows do not raise the same surge several times. The baseline is
    the mean and standard deviation of the category's previous aligned windows,
    with the variance floored at the mean (Poisson) for low-count categories.
    """
    by_category: Dict[str, List[tuple]] = defaultdict(list)
    for row in history:
        start = _utc(row.get("window_start"))
        if start is not None and _aligned(start, config.window_minutes):
            by_category[row["category"]].append((start, int(row.get("new_postings") or 0)))

    alerts = []
    created_at = datetime.now(timezone.utc)
    for row in current:
        start = _utc(row.get("window_start"))
        if start is None or not _aligned(start, config.window_minutes):
            continue
        category = row["category"]
        observed = int(row.get("new_postings") or 0)
        previous = sorted((item for item in by_category[category] if item[0] < start), reverse=True)
        baseline = [count for _start, count in previous[: config.history_windows]]
        if len(baseline) < config.min_history:
            continue
        mean = statistics.fmean(baseline)
        # Counts are roughly Poisson, so variance is at least the mean; the floor
        # stops a quiet, flat baseline (stdev ~ 0) from flagging small blips.
        stdev = math.sqrt(max(statistics.pvariance(baseline), mean))
        threshold = max(config.min_new_postings, config.surge_ratio * mean, mean + config.surge_z * stdev)
        if observed < threshold:
            continue
        ratio = observed / mean if mean else math.inf
        alerts.append({
            "alert_id": f"surge:{category}:{start.isoformat()}",
            "alert_type": "HIRING_SURGE",
            "severity": "critical" if ratio >= 2 * config.surge_ratio else "warning",
            "subject": category,
            "message": (
                f"Hiring surge in {category}: {observed} new postings in "
                f"{config.window_minutes} min vs baseline {mean:.1f}"
            ),
            "observed": float(observed),
            "threshold": round(threshold, 2),
            "baseline": round(mean, 2),
            "window_start": start,
            "window_end": _utc(row.get("window_end")),
            "created_at": created_at,
        })
    return alerts


def data_quality_alert(summary: Dict[str, Any], config: AlertConfig = AlertConfig()) -> Optional[Dict[str, Any]]:
    """Alert when a micro-batch's validation rejection rate is too high."""
    total = int(summary.get("processed", 0)) + int(summary.get("rejected", 0))
    if total < config.min_batch_records:
        return None
    rate = int(summary.get("rejected", 0)) / total
    if rate <= config.max_rejection_rate:
        return None
    created_at = datetime.now(timezone.utc)
    return {
        "alert_id": f"quality:{summary.get('source')}:{summary.get('batch_id')}",
        "alert_type": "DATA_QUALITY",
        "severity": "critical" if rate >= 2 * config.max_rejection_rate else "warning",
        "subject": str(summary.get("source")),
        "message": f"{rate:.0%} of {total} records rejected by validation in batch {summary.get('batch_id')}",
        "observed": round(rate, 4),
        "threshold": config.max_rejection_rate,
        "baseline": None,
        "window_start": None,
        "window_end": None,
        "created_at": created_at,
    }


class AlertSink:
    """Delivers new alerts once: Parquet lake, Kafka topic and optional webhook."""

    def __init__(
        self,
        lake_root: str | Path,
        *,
        bootstrap_servers: Optional[str] = None,
        topic: str = "job-alerts",
        webhook_url: Optional[str] = None,
    ) -> None:
        self.lake_root = Path(lake_root)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.webhook_url = webhook_url
        self._producer = None
        self._seen: Optional[set] = None

    def _known_ids(self) -> set:
        if self._seen is None:
            frame = parquet_store.read_dataset(self.lake_root, parquet_store.ALERTS_DATASET)
            self._seen = set(frame["alert_id"]) if len(frame) else set()
        return self._seen

    def publish(self, alerts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        known = self._known_ids()
        fresh = []
        for alert in alerts:
            if alert and alert["alert_id"] not in known:
                known.add(alert["alert_id"])
                fresh.append(alert)
        if not fresh:
            return []
        digest = hashlib.sha1("|".join(alert["alert_id"] for alert in fresh).encode("utf-8")).hexdigest()[:12]
        label = f"alerts-{digest}"
        parquet_store.append_alerts(fresh, self.lake_root, label=label)
        payloads = [_payload(alert) for alert in fresh]
        if self.bootstrap_servers:
            self._publish_kafka(payloads)
        if self.webhook_url:
            for payload in payloads:
                _post_webhook(self.webhook_url, payload)
        return fresh

    def _publish_kafka(self, payloads: List[Dict[str, Any]]) -> None:
        try:
            if self._producer is None:
                from src.streaming.kafka_producer import KafkaJobProducer

                self._producer = KafkaJobProducer(bootstrap_servers=self.bootstrap_servers, topic=self.topic)
            for payload in payloads:
                # Keyed by alert_id so consumers can de-duplicate redeliveries.
                self._producer.send_async(payload, key=payload["alert_id"])
            self._producer.flush()
        except Exception as exc:  # Alerting must never stop the pipeline.
            print(f"Alert delivery to Kafka failed: {exc}", flush=True)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()


def _payload(alert: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: (value.isoformat() if isinstance(value, datetime) else value)
        for key, value in alert.items()
    }


def _post_webhook(url: str, payload: Dict[str, Any]) -> None:
    body = json.dumps({"text": payload["message"], "alert": payload}).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=3):
            pass
    except Exception as exc:  # Best effort: a slow or failing webhook must not block processing.
        print(f"Alert webhook failed: {exc}", flush=True)
