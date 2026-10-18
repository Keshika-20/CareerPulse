"""Reproducible CareerPulse benchmarks.

    python scripts/benchmark.py pipeline --events 20000
    python scripts/benchmark.py producer --events 20000          # needs Kafka on localhost:9092
    python scripts/benchmark.py spark --events 20000 --partitions 1 2 4
    python scripts/benchmark.py latency --lake data/lake --minutes 10

pipeline  ETL + change detection + HBase (embedded emulator) + Parquet, single process.
producer  Kafka publish throughput: one acknowledged send per event vs pipelined batches.
spark     The Structured Streaming foreachBatch handler on one micro-batch, by partition count.
latency   End-to-end latency (processed_at - event_time) of live events already in the lake.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator.job_generator import JobEventSimulator  # noqa: E402


def _events(count: int, seed: int = 1, invalid_rate: float = 0.0) -> List[Any]:
    return JobEventSimulator(seed=seed, invalid_rate=invalid_rate).generate(
        count, start=datetime(2026, 10, 1, tzinfo=timezone.utc), span=timedelta(hours=24)
    )


def bench_pipeline(args) -> Dict[str, Any]:
    from src.storage.hbase_store import HBaseJobStore
    from src.streaming.pipeline import StreamPipeline

    events = _events(args.events, invalid_rate=0.03)
    workdir = Path(tempfile.mkdtemp(prefix="cp-bench-"))
    try:
        store = HBaseJobStore.connect("local", path=workdir / "hbase.sqlite3")
        pipeline = StreamPipeline(store, workdir / "lake", source="bench")
        started = time.perf_counter()
        for offset in range(0, len(events), args.batch_size):
            pipeline.process_raw_records(events[offset:offset + args.batch_size], offset // args.batch_size)
        elapsed = time.perf_counter() - started
        store.close()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return {"events": len(events), "batch_size": args.batch_size, "seconds": round(elapsed, 2),
            "events_per_second": round(len(events) / elapsed)}


def bench_producer(args) -> Dict[str, Any]:
    from src.streaming.kafka_producer import KafkaJobProducer

    events = _events(args.events)
    producer = KafkaJobProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
    try:
        sample = events[: min(len(events), args.sync_sample)]
        started = time.perf_counter()
        for event in sample:
            producer.send_event(event)
        sync_rate = len(sample) / (time.perf_counter() - started)

        started = time.perf_counter()
        producer.send_batch(events)
        batch_rate = len(events) / (time.perf_counter() - started)
    finally:
        producer.close()
    return {"events": len(events), "sync_events_per_second": round(sync_rate),
            "pipelined_events_per_second": round(batch_rate), "speedup": round(batch_rate / sync_rate, 1)}


def bench_spark(args) -> Dict[str, Any]:
    from src.common.spark_session import build_spark_session
    from src.streaming.spark_kafka_consumer import apply_etl, make_batch_processor, parse_kafka_values

    raw = _events(args.events, invalid_rate=0.03)
    values = [value if isinstance(value, str) else json.dumps(value) for value in raw]
    spark = build_spark_session("CareerPulse benchmark", f"local[{max(args.partitions)}]")
    results = []
    try:
        rows = [(value.encode("utf-8"), "bench", index % 3, index) for index, value in enumerate(values)]
        frame = spark.createDataFrame(rows, "value binary, topic string, partition int, offset long")
        batch = apply_etl(parse_kafka_values(frame)).cache()
        batch.count()
        for partitions in args.partitions:
            workdir = Path(tempfile.mkdtemp(prefix=f"cp-spark-{partitions}-"))
            if args.hbase_mode == "thrift":
                hbase = {"mode": "thrift", "host": args.hbase_host, "port": args.hbase_port,
                         "table_prefix": f"bench_p{partitions}_"}
            else:
                hbase = {"mode": "local", "path": str(workdir / "hbase.sqlite3")}
            try:
                handler = make_batch_processor(
                    hbase=hbase,
                    lake_root=workdir / "lake",
                    num_partitions=partitions,
                    source="bench",
                )
                started = time.perf_counter()
                handler(batch, 0)
                elapsed = time.perf_counter() - started
            finally:
                shutil.rmtree(workdir, ignore_errors=True)
                if args.hbase_mode == "thrift":
                    _drop_bench_tables(hbase)
            results.append({"partitions": partitions, "seconds": round(elapsed, 2),
                            "events_per_second": round(len(values) / elapsed)})
    finally:
        spark.stop()
    return {"events": len(values), "hbase": args.hbase_mode, "runs": results}


def _drop_bench_tables(hbase: Dict[str, Any]) -> None:
    from src.storage.hbase_store import HBaseJobStore

    store = HBaseJobStore.connect("thrift", host=hbase["host"], port=hbase["port"], table_prefix=hbase["table_prefix"])
    for name in (store.job_table_name, store.stats_table_name):
        store.connection.delete_table(name, disable=True)
    store.close()


def _percentile(values: List[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def bench_latency(args) -> Dict[str, Any]:
    from src.storage import parquet_store

    events = parquet_store.read_dataset(args.lake)
    if events.empty:
        return {"error": f"no events under {args.lake}"}
    cutoff = events["processed_at"].max() - timedelta(minutes=args.minutes)
    live = events[events["event_time"] >= cutoff]
    latencies = ((live["processed_at"] - live["event_time"]).dt.total_seconds()).tolist()
    latencies = [value for value in latencies if value >= 0]
    if not latencies:
        return {"error": "no live events in the window"}
    return {
        "events": len(latencies),
        "p50_seconds": round(_percentile(latencies, 50), 2),
        "p95_seconds": round(_percentile(latencies, 95), 2),
        "p99_seconds": round(_percentile(latencies, 99), 2),
        "mean_seconds": round(statistics.fmean(latencies), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("pipeline")
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--batch-size", type=int, default=1000)
    p = sub.add_parser("producer")
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--sync-sample", type=int, default=2000)
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="job-events-bench")
    p = sub.add_parser("spark")
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--partitions", type=int, nargs="+", default=[1, 2, 4])
    p.add_argument("--hbase-mode", choices=["local", "thrift"], default="local")
    p.add_argument("--hbase-host", default="localhost")
    p.add_argument("--hbase-port", type=int, default=9090)
    p = sub.add_parser("latency")
    p.add_argument("--lake", default="data/lake")
    p.add_argument("--minutes", type=float, default=10)
    args = parser.parse_args()

    runner = {"pipeline": bench_pipeline, "producer": bench_producer,
              "spark": bench_spark, "latency": bench_latency}[args.command]
    result = {
        "benchmark": args.command,
        "machine": {"platform": platform.platform(), "python": platform.python_version(),
                    "cpus": __import__("os").cpu_count()},
        **runner(args),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
