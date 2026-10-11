from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from src.ingestion.base_source import JsonHttpClient, SourceFetchError
from src.ingestion.greenhouse_source import GreenhouseSource
from src.ingestion.lever_source import LeverSource
from src.ingestion.normalize import normalize_scan
from src.ingestion.sqlite_store import IncompleteScanError, SQLiteSnapshotStore


def load_config(config_path: str | Path) -> Dict[str, Any]:
    path = Path(config_path)
    try:
        with path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to read ingestion config at {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError("Ingestion configuration must be a JSON object.")

    database_path = config.get("sqlite_path")
    if not isinstance(database_path, str) or not database_path.strip():
        raise ValueError("sqlite_path must be a non-empty string.")
    retention_days = config.get("raw_retention_days", 7)
    if (
        isinstance(retention_days, bool)
        or not isinstance(retention_days, int)
        or retention_days < 0
    ):
        raise ValueError("raw_retention_days must be a non-negative integer.")
    timeout_seconds = config.get("request_timeout_seconds", 15)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or timeout_seconds <= 0
    ):
        raise ValueError("request_timeout_seconds must be a positive number.")
    retries = config.get("max_retries", 2)
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError("max_retries must be a non-negative integer.")

    sources = config.get("sources", {})
    if not isinstance(sources, dict):
        raise ValueError("sources must be an object.")
    unknown_sources = set(sources) - {"greenhouse", "lever"}
    if unknown_sources:
        raise ValueError(f"Unsupported source types: {', '.join(sorted(unknown_sources))}.")
    for source_name in ("greenhouse", "lever"):
        accounts = sources.get(source_name, [])
        if not isinstance(accounts, list):
            raise ValueError(f"sources.{source_name} must be a list.")
        for index, account in enumerate(accounts):
            if not isinstance(account, dict):
                raise ValueError(f"sources.{source_name}[{index}] must be an object.")
            if "enabled" in account and not isinstance(account["enabled"], bool):
                raise ValueError(
                    f"sources.{source_name}[{index}].enabled must be a boolean."
                )
            if account.get("enabled", False) and (
                not isinstance(account.get("company"), str)
                or not account["company"].strip()
            ):
                raise ValueError(
                    f"Enabled sources.{source_name}[{index}] requires a company name."
                )
            if source_name == "greenhouse" and account.get("enabled", False):
                if not isinstance(account.get("board_token"), str) or not account["board_token"].strip():
                    raise ValueError(
                        f"Enabled sources.greenhouse[{index}] requires board_token."
                    )
            if source_name == "lever" and account.get("enabled", False):
                if not isinstance(account.get("site"), str) or not account["site"].strip():
                    raise ValueError(f"Enabled sources.lever[{index}] requires site.")
                region = account.get("region", "global")
                if not isinstance(region, str) or region not in {"global", "eu"}:
                    raise ValueError(
                        f"sources.lever[{index}].region must be 'global' or 'eu'."
                    )
    return config


class IngestionService:
    def __init__(
        self,
        config: Dict[str, Any],
        *,
        with_spark: bool = False,
        http_client: Optional[JsonHttpClient] = None,
    ) -> None:
        self.config = config
        self.with_spark = with_spark
        self.http_client = http_client or JsonHttpClient(
            timeout_seconds=config.get("request_timeout_seconds", 15),
            max_retries=config.get("max_retries", 2),
        )

    def run(self) -> Dict[str, Any]:
        enabled_sources = []
        for source_name, accounts in self.config.get("sources", {}).items():
            if source_name in {"greenhouse", "lever"}:
                enabled_sources.extend(
                    (source_name, account)
                    for account in accounts
                    if account.get("enabled", False)
                )
        if not enabled_sources:
            raise ValueError("No Greenhouse or Lever accounts are enabled in the config.")

        store = SQLiteSnapshotStore(
            self.config["sqlite_path"],
            raw_retention_days=self.config.get("raw_retention_days", 7),
        )
        processor = None
        results = []
        failures = []
        try:
            if self.with_spark:
                from src.streaming.spark_stream_processor import SparkJobStreamProcessor

                spark_config = self.config.get("spark", {})
                processor = SparkJobStreamProcessor(
                    app_name=spark_config.get("app_name", "CareerPulse Ingestion Batch"),
                    master=spark_config.get("master", "local[*]"),
                )

            greenhouse = GreenhouseSource(self.http_client)
            lever = LeverSource(self.http_client)
            for source_name, account in enabled_sources:
                account_id = (
                    account["board_token"]
                    if source_name == "greenhouse"
                    else f"{account.get('region', 'global')}:{account['site']}"
                )
                try:
                    if source_name == "greenhouse":
                        scan = greenhouse.fetch(account["board_token"], account["company"])
                    else:
                        scan = lever.fetch(
                            account["site"],
                            account["company"],
                            region=account.get("region", "global"),
                        )
                    observed_at = datetime.now(timezone.utc).isoformat()
                    listings = normalize_scan(scan, observed_at=observed_at)
                except (SourceFetchError, ValueError) as exc:
                    failures.append(
                        {
                            "source": source_name,
                            "account_id": account_id,
                            "error": str(exc),
                        }
                    )
                    continue

                try:
                    run_stats = store.apply_scan(
                        scan,
                        listings,
                        observed_at=observed_at,
                    )
                except IncompleteScanError as exc:
                    failures.append(
                        {
                            "source": source_name,
                            "account_id": account_id,
                            "error": str(exc),
                        }
                    )
                    continue

                if processor is not None:
                    run_events = store.list_events_for_run(run_stats["run_id"])
                    run_stats["spark_event_counts"] = processor.process_batch(run_events)
                results.append(run_stats)
        finally:
            store.close()
            if processor is not None:
                processor.close()

        return {"runs": results, "failures": failures}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch configured public Greenhouse and Lever job boards."
    )
    parser.add_argument(
        "--config",
        default="config/ingestion.example.json",
        help="Path to the JSON ingestion configuration.",
    )
    parser.add_argument(
        "--spark-batch",
        action="store_true",
        help="Run genuine Spark DataFrame aggregation on persisted lifecycle events.",
    )
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        result = IngestionService(config, with_spark=args.spark_batch).run()
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
