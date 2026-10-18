# CareerPulse Benchmarks

All numbers come from [scripts/benchmark.py](../scripts/benchmark.py) and the Docker pipeline, measured on 2026-10-18.

**Machine:** Windows 11, 24 logical CPUs. The Docker Desktop VM has 24 CPUs and 8 GB.

**Software:** host Python 3.14 with PySpark 4.2. The Spark container runs Python 3.12, OpenJDK 17 and Spark `local[N]`. HBase is 2.1 over Thrift, and Kafka 4.1 runs as a single KRaft broker.

**Workload:** 20,000 simulated lifecycle events (NEW / MODIFIED / DUPLICATE / CLOSED) unless stated, with 3% malformed records where noted. These are single-machine numbers; they show relative effects, not cluster capacity.

## Before and after the performance work

| Path | Before | After | Change | What changed |
|---|---:|---:|---:|---|
| Local pipeline (ETL + change detection + HBase emulator + Parquet), events/s | 244 | 1,979 | **8.1×** | Bulk multi-get, one transaction per partition, cached schema, index-friendly version trim |
| Kafka producer, events/s | 440 (one ack per event) | 7,362 (pipelined) | **16.7×** | Async sends, 5 ms linger, 64 KB batches, gzip; one flush per batch |

The "before" producer figure was measured without linger. With `linger_ms=5`, an acknowledged-per-event send drops to 63/s. That's why the ingestion outbox now pipelines its sends too, while still marking each event published only after its own acknowledgement.

## Parallel scaling of the streaming `foreachBatch` handler

One 20,000-event micro-batch, repartitioned by `job_id`. The table shows end-to-end handler time, with the real HBase over Thrift (`--hbase-mode thrift`, run inside the Spark container).

| Partitions | Seconds | Events/s | Speed-up |
|---:|---:|---:|---:|
| 1 | 29.7 | 673 | 1.0× |
| 2 | 15.7 | 1,271 | 1.9× |
| 4 | 11.2 | 1,779 | 2.6× |
| 8 | 6.9 | 2,881 | **4.3×** |

Against the embedded SQLite emulator, the same handler peaks at 1,869 events/s with 4 partitions (1.46×) and falls back at 8. A single SQLite file has one write lock, so parallel partitions queue for it. That contrast is the reason for real HBase: every write goes to its own region-server row, with no global lock.

## End-to-end latency (Kafka produce → processed in the lake)

Live stream of 20 events/s for 90 s (1,800 events), measured as `processed_at − event_time` per event.

| Trigger interval | p50 | p95 | p99 | Mean | Micro-batch duration |
|---|---:|---:|---:|---:|---:|
| 10 s (default) | 6.0 s | 10.5 s | 11.2 s | 6.0 s | 1.0–1.3 s per ~200 events |
| 1 s | 2.3 s | 5.6 s | 8.3 s | 2.6 s | 1.2–2.0 s per 4–41 events |

With a 10 s trigger, latency is roughly half the trigger interval plus processing. At 1 s, the fixed per-batch cost of about 1–1.5 s dominates: Spark job scheduling, the Python worker hand-off and HBase round trips. Shorter triggers buy latency at the cost of more batches and more small files.

## Correctness under concurrency and failure

| Check | Result |
|---|---|
| Kafka records vs lake after a crash and replay (Docker run) | 7,880 records = 7,770 processed + 110 quarantined; **0 duplicate event IDs** |
| Replayed micro-batch (batch 8, re-run after the job crashed mid-batch) | Re-run with the same offsets and the same classifications; counters not double-counted |
| 4 OS processes × 150 increments on one counter, old read-then-write code | **300 / 600**: half the updates lost |
| Same, transactional increment | **600 / 600** (`test_counter_increments_are_atomic_across_processes`) |
| 1 vs 4 parallel partitions on 600 events | Identical events, HBase states and counters (`test_parallel_partitions_match_sequential_processing`) |

## Reproduce

```powershell
python scripts/benchmark.py pipeline --events 20000
docker compose up -d kafka; python scripts/benchmark.py producer --events 20000
docker compose --profile analytics run --rm --no-deps spark-analytics python scripts/benchmark.py spark --hbase-mode thrift --hbase-host hbase --partitions 1 2 4 8
python -m src.streaming.kafka_producer --stream --rate 20 --duration 90; python scripts/benchmark.py latency --minutes 1.6
```
