"""Local end-to-end CareerPulse run without Kafka or a Spark cluster.

Generator -> raw JSONL -> micro-batches through the same ETL, change detection,
HBase and Parquet code the Structured Streaming job uses -> DuckDB summary.
Pass --spark to also run the Spark SQL / RDD / MLlib analytics.

    python scripts/run_pipeline.py --count 3000 --backfill-hours 24 --invalid-rate 0.03 --spark
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.duckdb_queries import LakeQueries
from src.generator.job_generator import JobEventSimulator, write_events
from src.storage.hbase_store import HBaseJobStore
from src.streaming.pipeline import StreamPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CareerPulse pipeline locally.")
    parser.add_argument("--count", type=int, default=600)
    parser.add_argument("--batch-size", type=int, default=100, help="Records per simulated micro-batch")
    parser.add_argument("--backfill-hours", type=float, default=6, help="Spread events over the past N hours")
    parser.add_argument("--invalid-rate", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--lake", default="data/lake")
    parser.add_argument("--hbase-mode", choices=["local", "thrift"], default="local")
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-path", default="data/hbase/careerpulse_hbase.sqlite3")
    parser.add_argument("--reset", action="store_true", help="Delete the lake and local HBase file first")
    parser.add_argument("--spark", action="store_true", help="Run Spark SQL/RDD/ML analytics afterwards")
    parser.add_argument("--bucket-minutes", type=int, default=30)
    args = parser.parse_args()

    lake = Path(args.lake)
    if args.reset:
        shutil.rmtree(lake, ignore_errors=True)
        if args.hbase_mode == "local":
            for suffix in ("", "-wal", "-shm"):
                Path(f"{args.hbase_path}{suffix}").unlink(missing_ok=True)

    simulator = JobEventSimulator(seed=args.seed, invalid_rate=args.invalid_rate)
    span = timedelta(hours=args.backfill_hours) if args.backfill_hours else None
    start = datetime.now(timezone.utc) - span if span else None
    events = simulator.generate(args.count, start=start, span=span)
    raw_path = write_events(events, "data/raw/job_events.jsonl")

    store = HBaseJobStore.connect(
        args.hbase_mode, host=args.hbase_host, port=args.hbase_port, path=args.hbase_path
    )
    pipeline = StreamPipeline(store, lake, source="local-runner")
    last_batch = pipeline.load_status().get("last_batch_by_source", {}).get(pipeline.source)
    first_batch = int(last_batch) + 1 if last_batch is not None else 0
    try:
        for offset in range(0, len(events), args.batch_size):
            batch_id = first_batch + offset // args.batch_size
            summary = pipeline.process_raw_records(events[offset:offset + args.batch_size], batch_id)
            print(f"Batch {batch_id}: processed={summary['processed']} rejected={summary['rejected']} detected={summary['detected']}")
        print(f"HBase jobs tracked: {len(store.list_jobs())}")
    finally:
        store.close()

    queries = LakeQueries(lake)
    print("Lake KPIs:", json.dumps(queries.kpis(), default=str))
    print("Top companies by NEW:", queries.top_companies(5))
    queries.close()
    print(f"Raw event file: {raw_path}")
    print(f"Parquet lake: {lake.resolve()}")

    if args.spark:
        from src.analytics.run_analytics import main as run_analytics

        run_analytics(["--lake", str(lake), "--bucket-minutes", str(args.bucket_minutes)])


if __name__ == "__main__":
    main()
