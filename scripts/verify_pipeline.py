"""Verify the pipeline end to end: nothing lost, nothing duplicated, state consistent.

    python scripts/verify_pipeline.py --lake data/lake                       # lake + local HBase emulator
    python scripts/verify_pipeline.py --kafka localhost:9092 --hbase-mode thrift --hbase-port 19090

Checks:
  raw archive unique        every archived message ID appears once
  processed unique          every processed event ID appears once
  quarantine unique         every rejected message ID appears once
  processed vs quarantine   no message is both processed and rejected
  raw accounted for         every archived message is processed or quarantined, and vice versa
  kafka coverage            (with --kafka) every offset in the topic is in the raw archive
  hbase consistency         every processed job has an HBase row whose status matches its latest event

Run against a lake created from a clean start: data written before the raw
archive existed has nothing to reconcile against.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.storage import parquet_store  # noqa: E402


def _ids(lake: Path, dataset: str) -> List[str]:
    frame = parquet_store.read_dataset(lake, dataset)
    if "event_id" not in frame.columns:
        return []
    return [value for value in frame["event_id"].tolist() if value]


def _check(name: str, ok: bool, detail: str) -> Dict[str, Any]:
    return {"check": name, "status": "PASS" if ok else "FAIL", "detail": detail}


def kafka_offsets(bootstrap_servers: str, topic: str) -> Dict[int, tuple]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",")])
    try:
        partitions = [TopicPartition(topic, partition) for partition in consumer.partitions_for_topic(topic) or []]
        begin = consumer.beginning_offsets(partitions)
        end = consumer.end_offsets(partitions)
        return {tp.partition: (begin[tp], end[tp]) for tp in partitions}
    finally:
        consumer.close()


def verify(
    lake: str | Path,
    *,
    offsets: Optional[Dict[int, tuple]] = None,
    topic: str = "job-events",
    store: Any = None,
) -> List[Dict[str, Any]]:
    lake = Path(lake)
    raw, processed, rejected = (
        _ids(lake, parquet_store.RAW_DATASET),
        _ids(lake, parquet_store.EVENTS_DATASET),
        _ids(lake, parquet_store.QUARANTINE_DATASET),
    )
    raw_set, processed_set, rejected_set = set(raw), set(processed), set(rejected)
    accounted = processed_set | rejected_set
    results = [
        _check("raw archive unique", len(raw) == len(raw_set), f"{len(raw):,} archived, {len(raw) - len(raw_set)} duplicates"),
        _check("processed unique", len(processed) == len(processed_set),
               f"{len(processed):,} processed, {len(processed) - len(processed_set)} duplicates"),
        _check("quarantine unique", len(rejected) == len(rejected_set),
               f"{len(rejected):,} rejected, {len(rejected) - len(rejected_set)} duplicates"),
        _check("processed vs quarantine", not (processed_set & rejected_set),
               f"{len(processed_set & rejected_set)} in both"),
        _check("raw accounted for", raw_set == accounted,
               f"{len(raw_set - accounted)} archived but unprocessed, {len(accounted - raw_set)} processed without archive"),
    ]

    if offsets is not None:
        expected = {
            f"{topic}-{partition}-{offset}"
            for partition, (begin, end) in offsets.items()
            for offset in range(begin, end)
        }
        archived = {value for value in raw_set if value.startswith(f"{topic}-")}
        results.append(_check(
            "kafka coverage", expected <= archived,
            f"{len(expected):,} offsets in topic, {len(expected - archived)} missing from archive",
        ))

    if store is not None:
        events = parquet_store.read_dataset(lake, parquet_store.EVENTS_DATASET)
        latest = (
            events.sort_values(["event_time", "processed_at"])
            .loc[lambda frame: frame["event"] != "DUPLICATE"]
            .groupby("job_id")["event"].last()
        )
        jobs = store.list_jobs()
        missing = [job for job in latest.index if job not in jobs]
        mismatched = [
            job for job, event in latest.items()
            if job in jobs and (jobs[job].get("status") == "CLOSED") != (event == "CLOSED")
        ]
        results.append(_check(
            "hbase consistency", not missing and not mismatched,
            f"{len(latest):,} jobs, {len(missing)} missing in HBase, {len(mismatched)} status mismatches",
        ))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lake", default="data/lake")
    parser.add_argument("--kafka", default=None, help="Bootstrap servers; enables the Kafka coverage check")
    parser.add_argument("--topic", default="job-events")
    parser.add_argument("--hbase-mode", choices=["local", "thrift", "none"], default="local")
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-path", default="data/hbase/careerpulse_hbase.sqlite3")
    parser.add_argument("--hbase-table-prefix", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    store = None
    if args.hbase_mode != "none":
        from src.storage.hbase_store import HBaseJobStore

        if args.hbase_mode == "local" and not Path(args.hbase_path).exists():
            print(f"No HBase emulator file at {args.hbase_path}; skipping the HBase check.", file=sys.stderr)
        else:
            store = HBaseJobStore.connect(
                args.hbase_mode, host=args.hbase_host, port=args.hbase_port,
                path=args.hbase_path, read_only=args.hbase_mode == "local",
                table_prefix=args.hbase_table_prefix,
            )
    try:
        offsets = kafka_offsets(args.kafka, args.topic) if args.kafka else None
        results = verify(args.lake, offsets=offsets, topic=args.topic, store=store)
    finally:
        if store is not None:
            store.close()

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for result in results:
            print(f"{result['status']:4}  {result['check']:24} {result['detail']}")
    passed = all(result["status"] == "PASS" for result in results)
    print("ALL CHECKS PASSED" if passed else "VERIFICATION FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
